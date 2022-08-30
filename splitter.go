package splitter

// pod splitter - concurrently num app instances.

import (
    "context"
    "errors"
    "strconv"
    "sync/atomic"
    "time"
)

type storage interface {
    GetActivePodCount(ctx context.Context, group string, lastUpdatedAgo time.Duration) (int64, error) // Count active instances.
    GetFirstUnusedPod(ctx context.Context, group string, lastUpdatedAgo time.Duration) (*PodDTO, error)
    AddPod(ctx context.Context, num int64, group string) (*PodDTO, error)                     // Add new instance, on create already exist Num must fail.
    ActualizePod(ctx context.Context, newID, oldID int64, lastUpdatedAgo time.Duration) error // Actualize instance in storage.
}

type Config struct {
    Delay             time.Duration `envconfig:"default=15s"` // NOTE: must be less than MaxUnusedDuration
    MaxUnusedDuration time.Duration `envconfig:"default=30s"`
    FullScanCount     int64         `envconfig:"default=2"`
    GroupName         string        `envconfig:"default=splitter"`
}

type metricsSetter interface {
    CountProcessingDuration(handler string, seconds float64)
    IncrFail(handler string, reason string)
}

type logger interface {
    Errorf(ctx context.Context, format string, args ...interface{})
    Infof(ctx context.Context, format string, args ...interface{})
}

type callbackFunc func() error

type Splitter struct {
    num           *int64
    count         *int64
    fullScanCount *int64
    id            int64
    logger        logger
    config        *Config
    storage       storage
    metrics       metricsSetter
    callbackFunc  callbackFunc
}

func NewPodSplitter(config *Config, storage storage, metrics metricsSetter, logger logger, callbackFunc callbackFunc) *Splitter {
    return &Splitter{
        num:           PtrOfInt64(0),
        count:         PtrOfInt64(0),
        fullScanCount: PtrOfInt64(0),
        config:        config,
        storage:       storage,
        metrics:       metrics,
        callbackFunc:  callbackFunc,
        logger:        logger,
    }
}

func (s *Splitter) String() string {
    return s.config.GroupName + "_" + strconv.FormatInt(s.Num(), 10) + "_" + strconv.FormatInt(s.Count(), 10)
}

// Corner cases:
// Два и более пода на одном процессе - потеря производительности
// Некорректное количество процессов - невыполнение каких то задач или потеря производительности

// Sync - синхронизует номер процесса для пода приложения.
// Смотрим самую позднюю запись, если с последнего обновления прошло > s.maxUnusedDuration, захватываем номер процесса,
// если таких записей нет, то запускаем на всю область задач (tasks таблица).
// После нескольких прогонов (fullScanCount) на всю выборку tasks, создаем новую запись в бд.
// При подсчете num учитывать только актуальные записи =< maxUnusedDuration.
// Если у нас уже определен номер процесса, то остаемся на нем.
func (s *Splitter) Sync(ctx context.Context) {
    defer func(start time.Time) {
        s.metrics.CountProcessingDuration(handlerName, time.Since(start).Seconds())
    }(time.Now())

    activePodCount, err := s.storage.GetActivePodCount(ctx, s.config.GroupName, s.config.MaxUnusedDuration)
    if err != nil {
        s.reset()
        s.logger.Errorf(ctx, "on Splitter.Sync: GetActivePodCount: %s", err)
        s.metrics.IncrFail(handlerName, storageError)
        return
    }

    count := s.Count()
    num := s.Num()

    // если количество запущенных процессов не изменилось и текущий num не превышает общее количество процессов, выходим обновляя текущий.
    if count > 0 && activePodCount == count && num <= activePodCount {
        if err = s.storage.ActualizePod(ctx, s.id, 0, s.config.Delay/2); err != nil {
            s.reset()
            s.logger.Errorf(ctx, "on Splitter.Sync: main.ActualizePod: %s", err)
            if errors.Is(err, ErrNoEntries) {
                s.metrics.IncrFail(handlerName, actualizeConflict)
            } else {
                s.metrics.IncrFail(handlerName, storageError)
            }
            return
        }
        s.logger.Infof(ctx, "[pod_splitter] pod actualized: id: %d, num: %d, count: %d, group: %s", s.id, s.Num(), activePodCount, s.config.GroupName)

        return
    }

    // так как у нас уже есть какие-то запущенные процессы, надо сделать баланс в большую или меньшую сторону
    switch {
    case activePodCount < count, activePodCount < num:
        s.decreasePodCount(ctx, activePodCount)
    case activePodCount > count, activePodCount == 0:
        s.increasePodCount(ctx, activePodCount)
    }
}

// takeUnusedPod - перехватывает первую простаивающую запись с минимальным num.
func (s *Splitter) takeUnusedPod(ctx context.Context, activePodsCount int64) bool {
    firstUnusedPod, err := s.storage.GetFirstUnusedPod(ctx, s.config.GroupName, s.config.MaxUnusedDuration)
    if err != nil && !errors.Is(err, ErrNoEntries) {
        s.reset()
        s.logger.Errorf(ctx, "on Splitter.Sync: GetFirstUnusedPod: %s", err)
        s.metrics.IncrFail(handlerName, storageError)
        return false
    }

    // захватываем простаивающий процесс
    if firstUnusedPod != nil {
        if err = s.storage.ActualizePod(ctx, firstUnusedPod.ID, s.id, s.config.MaxUnusedDuration); err != nil {
            s.reset()
            s.logger.Errorf(ctx, "on Splitter.Sync: on takeUnusedPod.ActualizePod: %s", err)
            if errors.Is(err, ErrNoEntries) {
                s.metrics.IncrFail(handlerName, actualizeConflict)
            } else {
                s.metrics.IncrFail(handlerName, storageError)
            }

            return false
        }

        s.logger.Infof(ctx, "[pod_splitter] unused pod captured: id: %d, num: %d, group: %s, count: %d", firstUnusedPod.ID, firstUnusedPod.Num, s.config.GroupName, activePodsCount)

        s.id = firstUnusedPod.ID
        atomic.StoreInt64(s.count, activePodsCount)
        atomic.StoreInt64(s.num, firstUnusedPod.Num)
        atomic.StoreInt64(s.fullScanCount, 0)

        if s.callbackFunc != nil {
            if err = s.callbackFunc(); err != nil {
                s.logger.Errorf(ctx, "on afterUpdateCallback: %s", err)
                return false
            }
        }

        return true
    }

    return false
}

// decreasePodCount - у нас было много подов, но сейчас количество уменьшилось, возможны два случая:
// 1. Текущий номер выходит за пределы количества (count), тогда нужно перехватить простаивающий под.
// 2. Текущий номер НЕ выходит за пределы количества, тогда уменьшить локальное количество подов (count).
// Если не получилось обработать ни один из случаев, под будет работать над всеми задачами.
func (s *Splitter) decreasePodCount(ctx context.Context, activePodsCount int64) {
    if n := s.Num(); n != 0 && n <= activePodsCount {
        if err := s.storage.ActualizePod(ctx, s.id, 0, s.config.Delay/2); err != nil {
            s.reset()
            s.logger.Errorf(ctx, "on Splitter.Sync: on decreasePodCount.ActualizePod: %s", err)
            if errors.Is(err, ErrNoEntries) {
                s.metrics.IncrFail(handlerName, actualizeConflict)
            } else {
                s.metrics.IncrFail(handlerName, storageError)
            }
            return
        }

        atomic.StoreInt64(s.count, activePodsCount) // номер процесса тот же, но количество меньше
        s.logger.Infof(ctx, "[pod_splitter] local pods count decreased: id: %d, num: %d, count: %d, group: %s", s.id, n, activePodsCount, s.config.GroupName)
        return
    }

    // num пода выходит за пределы поэтому пытаемся перехватить запись ушедшего пода, иначе делаем выборку на всю таблицу.
    if !s.takeUnusedPod(ctx, activePodsCount) {
        s.reset()
    }
}

// increasePodCount - возможны два случая: мы еще не захватили запись или количество подов увеличилось.
func (s *Splitter) increasePodCount(ctx context.Context, activePodsCount int64) {
    if n := s.Num(); n != 0 && n <= activePodsCount {
        if err := s.storage.ActualizePod(ctx, s.id, 0, s.config.Delay/2); err != nil {
            s.reset()
            s.logger.Errorf(ctx, "on Splitter.Sync: on increasePodCount.ActualizePod: %s", err)
            if errors.Is(err, ErrNoEntries) {
                s.metrics.IncrFail(handlerName, actualizeConflict)
            } else {
                s.metrics.IncrFail(handlerName, storageError)
            }
            return
        }

        atomic.StoreInt64(s.count, activePodsCount) // номер процесса тот же, но количество больше
        s.logger.Infof(ctx, "[pod_splitter] local pods count increased: id: %d, num: %d, count: %d, group: %s", s.id, n, activePodsCount, s.config.GroupName)

        return
    }

    // мы еще не захватили запись пытаемся перехватить существующую или ее num выходит за пределы.
    if s.takeUnusedPod(ctx, activePodsCount+1) {
        return
    }

    // Чтобы уменьшить двойные создания, ждем некоторое время, прежде чем создать новую запись.
    if atomic.LoadInt64(s.fullScanCount) >= s.config.FullScanCount {
        if err := s.addNewPod(ctx, activePodsCount+1); err != nil {
            s.reset()
            s.logger.Errorf(ctx, "on Splitter.Sync: addNewPod: %s", err)
            s.metrics.IncrFail(handlerName, storageError)
        }
    } else {
        atomic.AddInt64(s.fullScanCount, 1)
    }
}

func (s *Splitter) addNewPod(ctx context.Context, activePodCount int64) error {
    dto, err := s.storage.AddPod(ctx, activePodCount, s.config.GroupName) // при попытке создать уже созданный процесс вызов упадет
    if err != nil {
        return err
    }

    s.logger.Infof(ctx, "[pod_splitter] new pod created: id: %d, num: %d, group: %s, count: %d", dto.ID, dto.Num, s.config.GroupName, activePodCount)

    s.id = dto.ID
    atomic.StoreInt64(s.count, activePodCount)
    atomic.StoreInt64(s.num, dto.Num)
    atomic.StoreInt64(s.fullScanCount, 0)

    if s.callbackFunc != nil {
        if err = s.callbackFunc(); err != nil {
            return err
        }
    }

    return nil
}

func (s *Splitter) reset() {
    s.id = 0
    atomic.StoreInt64(s.num, 0)
    atomic.StoreInt64(s.count, 0)
    atomic.AddInt64(s.fullScanCount, 1)
}

func (s *Splitter) Num() int64 {
    return atomic.LoadInt64(s.num)
}

func (s *Splitter) Count() int64 {
    return atomic.LoadInt64(s.count)
}

func PtrOfInt64(i int64) *int64 {
    return &i
}

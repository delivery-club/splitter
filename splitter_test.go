package splitter

import (
    "context"
    "math/rand"
    "sort"
    "sync"
    "sync/atomic"
    "testing"
    "time"
)

func init() {
    rand.Seed(time.Now().Unix())
}

type storageMock struct {
    unusedPodSplitters map[int]*Splitter
    pods               map[int]*Splitter
    activeCounter      *int64
    numCounter         *int64
    mu                 *sync.RWMutex
}

type metricsMock struct{}

func (metricsMock) CountProcessingDuration(_ string, _ float64) {}
func (metricsMock) IncrFail(_, _ string)                        {}

var supportTasksMetrics = metricsMock{}

type loggerMock struct{}

func (loggerMock) Errorf(_ context.Context, _ string, _ ...interface{}) {}
func (loggerMock) Infof(_ context.Context, _ string, _ ...interface{})  {}

// Запускаем с нуля все воркеры, все должны выбрать пустующие процессы или создать новые.
func TestFirstStart(t *testing.T) {
    t.Parallel()

    wg := &sync.WaitGroup{}
    s := &storageMock{
        unusedPodSplitters: make(map[int]*Splitter, 5),
        pods:               make(map[int]*Splitter, 5),
        activeCounter:      PtrOfInt64(0),
        numCounter:         PtrOfInt64(0),
        mu:                 &sync.RWMutex{},
    }

    for i := 0; i < 3; i++ {
        s.createNewPod()
    }

    // создает записи
    runPodsAsync(wg, s.pods)
    // балансирует на 3 процесса
    runPodsAsync(wg, s.pods)
    checks(t, s.pods)
}

// Запускаем еще один под. Под должен выбрать новый процесс, остальные должны увеличить количество процессов.
func TestScale(t *testing.T) {
    t.Parallel()

    wg := &sync.WaitGroup{}
    s := &storageMock{
        unusedPodSplitters: make(map[int]*Splitter, 5),
        pods:               make(map[int]*Splitter, 5),
        activeCounter:      PtrOfInt64(0),
        numCounter:         PtrOfInt64(0),
        mu:                 &sync.RWMutex{},
    }

    for i := 0; i < 3; i++ {
        s.createNewPod()
    }

    // создает записи
    runPodsAsync(wg, s.pods)
    // балансирует на 3 процесса
    runPodsAsync(wg, s.pods)
    checks(t, s.pods)

    // ничего не делают
    runPodsAsync(wg, s.pods)
    checks(t, s.pods)

    // добавляем один новый процесс
    s.createNewPod()

    // балансирует на 4 процесса
    runPodsAsync(wg, s.pods)
    checks(t, s.pods)

    for i := 0; i < 5; i++ {
        s.createNewPod()
        runPodsAsync(wg, s.pods)
        checks(t, s.pods)

        s.deleteOldPod()
    }
}

// Пересоздаем все поды, кроме одного. Поды должные выбрать пустующие процессы, оставшийся один должен сбалансироваться на новое количество процессов.
func TestNewDeploy(t *testing.T) {
    t.Parallel()
    wg := &sync.WaitGroup{}
    s := &storageMock{
        unusedPodSplitters: make(map[int]*Splitter, 5),
        pods:               make(map[int]*Splitter, 5),
        activeCounter:      PtrOfInt64(0),
        numCounter:         PtrOfInt64(0),
        mu:                 &sync.RWMutex{},
    }

    for i := 0; i < 3; i++ {
        s.createNewPod()
    }

    runPodsAsync(wg, s.pods) // создает записи процессов
    runPodsAsync(wg, s.pods) // балансирует на новое количество процессов
    checks(t, s.pods)

    count := len(s.pods)
    // пересоздаем все поды
    for i := 0; i < count; i++ {
        s.deleteOldPod()
        s.createNewPod()

        runPodsAsync(wg, s.pods) // балансирует на новое количество процессов
        checks(t, s.pods)
    }
}

// Удаляем часть подов, оставшиеся должны сбалансировать на новое количество,
// учесть что удаляются произвольные процессы, так что надо занимать процессы от младшего к большему.
func TestNodeOutOfOrder(t *testing.T) {
    t.Parallel()
    wg := &sync.WaitGroup{}
    s := &storageMock{
        unusedPodSplitters: make(map[int]*Splitter, 5),
        pods:               make(map[int]*Splitter, 5),
        activeCounter:      PtrOfInt64(0),
        numCounter:         PtrOfInt64(0),
        mu:                 &sync.RWMutex{},
    }

    for i := 0; i < 3; i++ {
        s.createNewPod()
    }

    runPodsAsync(wg, s.pods) // создает записи процессов
    runPodsAsync(wg, s.pods) // балансирует на новое количество процессов
    checks(t, s.pods)

    count := len(s.pods)
    // пересоздаем все поды
    for i := 0; i < count; i++ {
        s.deleteOldPod()
        s.createNewPod()

        runPodsAsync(wg, s.pods) // балансирует на новое количество процессов
        checks(t, s.pods)
    }

    s.deleteOldPod()
    s.deleteOldPod()

    // нет обновления не используемого
    runPodsAsync(wg, s.pods)
    checks(t, s.pods)

    s.createNewPod()
    s.createNewPod()

    runPodsAsync(wg, s.pods) // баланс на новое количество: 2
    checks(t, s.pods)
}

func runPodsAsync(wg *sync.WaitGroup, ps map[int]*Splitter) {
    wg.Add(len(ps))
    var pp []*Splitter
    for _, p := range ps {
        pp = append(pp, p)
    }

    for _, p := range pp {
        go run(wg, p.Sync)
    }

    wg.Wait()
}

func run(wg *sync.WaitGroup, f func(ctx context.Context)) {
    defer wg.Done()
    f(context.Background())
}

func checks(t *testing.T, pods map[int]*Splitter) {
    uniqNums := make(map[int]int, 3)
    for i, pod := range pods {
        uniqNums[int(*pod.num)]++
        if num := atomic.LoadInt64(pod.num); num == 0 || num > int64(len(pods)) {
            t.Fatalf("unexpected num: pod num: %d, actual: %d, expected: <= %d", i, num, len(pods))
        }

        if count := atomic.LoadInt64(pod.count); count == 0 || count != int64(len(pods)) {
            t.Fatalf("unexpected count: pod num: %d, actual: %d, expected: %d", i, count, len(pods))
        }
    }

    for _, v := range uniqNums {
        if v > 1 {
            t.Fatalf("not uniq num found")
        }
    }

    nums := make([]int, 0, 10)
    for i := range uniqNums {
        nums = append(nums, i)
    }

    sort.Ints(nums)
    prev := nums[0]

    for i, next := range nums {
        if i == 0 {
            prev = next
            continue
        }
        if prev+1 != next {
            t.Fatalf("nums not monotonic: prev: %d, next: %d, nums: %v", prev, next, nums)
        }
        prev = next
    }
}

func (s *storageMock) GetActivePodCount(_ context.Context, _ string, _ time.Duration) (int64, error) {
    return atomic.LoadInt64(s.activeCounter), nil
}

func (s *storageMock) GetFirstUnusedPod(_ context.Context, _ string, _ time.Duration) (*PodDTO, error) {
    s.mu.RLock()
    defer s.mu.RUnlock()
    if len(s.unusedPodSplitters) == 0 {
        return nil, ErrNoEntries
    }

    var first int64
    for _, newSw := range s.unusedPodSplitters {
        oldSw, ok := s.unusedPodSplitters[int(first)]
        if ok && *newSw.num < *oldSw.num {
            first = newSw.id
        } else if !ok {
            first = newSw.id
        }
    }
    pod := s.unusedPodSplitters[int(first)]

    if pod == nil {
        return nil, ErrNoEntries
    }

    return &PodDTO{
        ID:  pod.id,
        Num: *pod.num,
    }, nil
}

func (s *storageMock) AddPod(_ context.Context, _ int64, _ string) (*PodDTO, error) {
    atomic.AddInt64(s.activeCounter, 1)

    return &PodDTO{
        ID:        rand.Int63(),
        Num:       atomic.AddInt64(s.numCounter, 1),
        CreatedAt: time.Now(),
        UpdatedAt: time.Now(),
    }, nil
}

// Выбрасывает ошибку, если обновляемый pod уже обновлен
func (s *storageMock) ActualizePod(_ context.Context, newID, oldID int64, _ time.Duration) error {
    s.mu.Lock()
    defer s.mu.Unlock()

    _, ok := s.pods[int(newID)]
    if ok {
        return nil
    }

    newSw, ok := s.unusedPodSplitters[int(newID)]

    if ok {
        newI := int(newID)
        oldI := int(oldID)

        if oldSw, ok := s.pods[oldI]; ok {
            // так как мы не можем воздействовать на вызывающий указатель, то просто меняем значения, актуально только для тестов
            oldSw.num, newSw.num = newSw.num, oldSw.num
            oldSw.count, newSw.count = newSw.count, oldSw.count
            return nil
        }

        s.pods[newI] = s.unusedPodSplitters[newI]
        delete(s.unusedPodSplitters, newI)

        atomic.AddInt64(s.activeCounter, 1)

        return nil
    }

    return ErrNoEntries
}

func (s *storageMock) deleteOldPod() {
    s.mu.Lock()
    // удаляет случайный процесс
    for i, pod := range s.pods {
        s.unusedPodSplitters[i] = pod
        delete(s.pods, i)
        break
    }
    s.mu.Unlock()
    atomic.AddInt64(s.activeCounter, -1)
}

func (s *storageMock) createNewPod() {
    pod := NewPodSplitter(&Config{}, s, supportTasksMetrics, loggerMock{}, nil)
    pod.Sync(context.Background())

    s.mu.Lock()
    s.pods[int(pod.id)] = pod
    s.mu.Unlock()
}

func (s *storageMock) Close(_ context.Context, _ PodDTO) error { return nil }

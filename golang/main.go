package main

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
)

// ЗАДАНИЕ:
// * сделать из плохого кода хороший;
// * важно сохранить логику появления ошибочных тасков;
// * сделать правильную мультипоточность обработки заданий.
// Обновленный код отправить через merge-request.

// приложение эмулирует получение и обработку тасков, пытается и получать и обрабатывать в многопоточном режиме
// В конце должно выводить успешные таски и ошибки выполнены остальных тасков

// A Ttype represents a meaninglessness of our life
type Ttype struct {
	id          int
	resultsWork string    // Какой-то результат выполнения
	createdT    time.Time // время создания // Изменения поля для удобного чтения
	finishedT   time.Time // время выполнения // Изменения поля для удобного чтения
	err         error     // Для передачи ошибки, если она произошла
}

func main() {
	// Канал отмены выполнения
	cancel := make(chan struct{})
	defer close(cancel)

	// Количество тасок
	taskNumber := 100

	// Создаем канал c количество тасков
	taskChan := createTask(taskNumber, cancel)

	// Выполнение какой-то работы #1
	fn := func(t Ttype) Ttype {
		if t.err != nil {
			t.err = fmt.Errorf("failed work #1: %v", t.err)
		} else {
			if time.Now().UnixMilli()%2 > 0 { // вот такое условие появления ошибочных тасков
				t.err = fmt.Errorf("work #1 id - %d: something error", t.id)
			} else {
				t.resultsWork = "task has been successed"
			}
		}

		return t
	}
	taskWork1Chan := workerTask(cancel, taskChan, fn)

	// Выполнение какой-то работы #2
	fn = func(t Ttype) Ttype {
		if t.err != nil {
			t.err = fmt.Errorf("failed work #2: %v", t.err)
		} else {
			if t.createdT.After(time.Now().Add(-20 * time.Second)) {
				t.resultsWork = "task has been successed"
			} else {
				t.err = errors.New("something went wrong")
			}
		}
		t.finishedT = time.Now()
		return t
	}
	taskWork2Chan := workerTask(cancel, taskChan, fn)

	// Объединение каналов
	taskMerge := mergeTask(cancel, taskWork1Chan, taskWork2Chan)

	// Сортировка тасков
	doneTasks, undoneTasks := sortedTask(cancel, taskMerge)

	// Получение успешно выполненных и ошибок
	result, err := distriTask(cancel, doneTasks, undoneTasks)

	// Печатаем информацию
	fmt.Println("Errors:")
	for _, r := range err {
		println(r.Error())
	}

	fmt.Println("Done tasks:")
	for _, r := range result {
		println(r.id, r.resultsWork)
	}
}

// Создаем нужное нам количество тасков
func createTask(n int, cancel <-chan struct{}) <-chan Ttype {
	taskChan := make(chan Ttype)
	go func() {
		defer close(taskChan)

		for i := 0; i < n; i++ {
			uuid := uuid.New()
			task := Ttype{
				id:       int(uuid.ID()),
				createdT: time.Now(),
			}
			select {
			case taskChan <- task:
			case <-cancel:
				return
			}
		}
	}()

	return taskChan
}

// Выполнение какой-то работы в тасках
func workerTask(cancel <-chan struct{}, tC <-chan Ttype, fn func(Ttype) Ttype) <-chan Ttype {
	outTask := make(chan Ttype)

	go func() {
		defer close(outTask)
		for t := range tC {
			select {
			case outTask <- fn(t):
			case <-cancel:
				return
			}
		}
	}()

	return outTask
}

// Объединение каналов
func mergeTask(cancel <-chan struct{}, tC ...<-chan Ttype) <-chan Ttype {
	out := make(chan Ttype)
	go func() {
		defer close(out)

		wg := sync.WaitGroup{}
		wg.Add(len(tC))

		send := func(in <-chan Ttype) {
			defer wg.Done()
			for task := range in {
				select {
				case out <- task:
				case <-cancel:
					return
				}
			}
		}
		for _, t := range tC {
			go send(t)
		}
		wg.Wait()
	}()
	return out
}

// Сортировка тасков
func sortedTask(cancel <-chan struct{}, tC <-chan Ttype) (<-chan Ttype, <-chan Ttype) {
	doneTask := make(chan Ttype)
	errTask := make(chan Ttype)
	go func() {
		defer func() {
			close(doneTask)
			close(errTask)
		}()

		for {
			select {
			case <-cancel:
				return
			case t, ok := <-tC:
				if !ok {
					return
				}
				if t.err != nil {
					errTask <- t
				} else {
					doneTask <- t
				}
			}
		}

	}()
	return doneTask, errTask
}

// Распределние тасков
func distriTask(cancel <-chan struct{}, doneTask, errTask <-chan Ttype) (map[int]Ttype, []error) {
	wg := sync.WaitGroup{}
	wg.Add(2)
	mu := sync.Mutex{}
	result := make(map[int]Ttype)
	err := []error{}

	processChannel := func(in <-chan Ttype, handleErr bool) {
		defer wg.Done()
		for {
			select {
			case task, ok := <-in:
				if !ok {
					return
				}

				mu.Lock()
				if handleErr {
					err = append(err, task.err)
				} else {
					result[task.id] = task
				}
				mu.Unlock()

			case <-cancel:
				return
			}
		}
	}

	go processChannel(doneTask, false)
	go processChannel(errTask, true)

	wg.Wait()
	return result, err
}

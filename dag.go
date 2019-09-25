package dag

import (
	"context"
	"errors"
	"sort"
	"sync"

	"golang.org/x/xerrors"

	"golang.org/x/sync/errgroup"
)

var (
	errFieldNotFound = errors.New("field not found")
	errWrongType     = errors.New("wrong type")
)

// IsFieldNotFoundError if the error is because a field is not found
func IsFieldNotFoundError(err error) bool {
	return xerrors.Is(err, errFieldNotFound)
}

// Record to be modified
type Record struct {
	content map[string]interface{}
	mutex   sync.Mutex
}

func (r *Record) get(key string) (interface{}, error) {
	v, ok := r.content[key]
	if !ok {
		return nil, errFieldNotFound
	}
	return v, nil
}

func (r *Record) Copy() map[string]interface{} {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	dupe := map[string]interface{}{}
	for k, v := range r.content {
		dupe[k] = v
	}
	return dupe
}

func (r *Record) Delete(fields ...string) {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	for _, field := range fields {
		delete(r.content, field)
	}
}

// Get a raw value
func (r *Record) Get(field string) (interface{}, error) {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	return r.get(field)
}

// Float64 value
func (r *Record) Float64(field string) (float64, error) {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	raw, err := r.get(field)
	if err != nil {
		return 0, err
	}

	v, ok := raw.(float64)
	if !ok {
		return 0, errWrongType
	}

	return v, nil
}

// Int value
func (r *Record) Int(field string) (int, error) {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	raw, err := r.get(field)
	if err != nil {
		return 0, err
	}

	v, ok := raw.(int)
	if !ok {
		return 0, errWrongType
	}

	return v, nil
}

// Int64 value
func (r *Record) Int64(field string) (int64, error) {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	raw, err := r.get(field)
	if err != nil {
		return 0, err
	}

	v, ok := raw.(int64)
	if !ok {
		return 0, errWrongType
	}

	return v, nil
}

func (r *Record) Fields() (fields []string) {
	r.mutex.Lock()
	for k := range r.content {
		fields = append(fields, k)
	}
	r.mutex.Unlock()

	sort.Strings(fields)
	return fields
}

func (r *Record) Merge(that map[string]interface{}) {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	if r.content == nil {
		r.content = map[string]interface{}{}
	}

	for k, v := range that {
		r.content[k] = v
	}
}

// String value
func (r *Record) String(field string) (string, error) {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	raw, err := r.get(field)
	if err != nil {
		return "", err
	}

	v, ok := raw.(string)
	if !ok {
		return "", errWrongType
	}

	return v, nil
}

// Set key and value
func (r *Record) Set(field string, value interface{}) bool {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	if r.content == nil {
		r.content = map[string]interface{}{}
	}

	_, ok := r.content[field]
	r.content[field] = value
	return ok
}

// Task is the atomic unit of work
type Task interface {
	// Apply a task
	Apply(ctx context.Context, record *Record) error
}

// TaskFunc provides a functional interface for Task
type TaskFunc func(ctx context.Context, record *Record) error

// Apply implements Task
func (fn TaskFunc) Apply(ctx context.Context, record *Record) error {
	return fn(ctx, record)
}

type parallel struct {
	tasks []Task
}

func (p parallel) Apply(ctx context.Context, record *Record) error {
	group, ctx := errgroup.WithContext(ctx)
	for _, t := range p.tasks {
		task := t
		group.Go(func() error {
			return task.Apply(ctx, record)
		})
	}
	return group.Wait()
}

// Parallel executes the requested tasks in parallel
func Parallel(tasks ...Task) Task {
	return parallel{tasks: tasks}
}

type serial struct {
	tasks []Task
}

func (s serial) Apply(ctx context.Context, record *Record) error {
	for _, task := range s.tasks {
		if err := task.Apply(ctx, record); err != nil {
			return err
		}
	}
	return nil
}

// Serial applies the tasks in serial
func Serial(tasks ...Task) Task {
	return serial{tasks: tasks}
}

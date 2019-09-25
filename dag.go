package dag

import (
	"context"
	"errors"
	"sort"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"
	"golang.org/x/xerrors"
)

var (
	errFieldNotFound = errors.New("field not found")
	errWrongType     = errors.New("wrong type")
)

// IsFieldNotFoundError if the error is because a field is not found
func IsFieldNotFoundError(err error) bool {
	return xerrors.Is(err, errFieldNotFound)
}

// IsWrongTypeError if the requested type was incorrect
func IsWrongTypeError(err error) bool {
	return xerrors.Is(err, errWrongType)
}

// Meta provides READ ONLY metadata for the record
type Meta struct {
	ID         string
	StartedAt  time.Time
	Properties map[string]string
}

// Record to be modified
type Record struct {
	meta    Meta
	content map[string]interface{}
	mutex   sync.Mutex
}

// NewRecord constructs a new record
func NewRecord(meta Meta) *Record {
	return &Record{
		meta: meta,
	}
}

// Meta data for record
func (r *Record) Meta() Meta {
	return r.meta
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

// Runner provides task with some metadata
type Runner interface {
	Task

	// Use middleware with EACH Task
	Use(middleware ...func(Task) Task)
}

// TaskFunc provides a functional interface for Task
type TaskFunc func(ctx context.Context, record *Record) error

// Apply implements Task
func (fn TaskFunc) Apply(ctx context.Context, record *Record) error {
	return fn(ctx, record)
}

type parallel struct {
	middleware []func(Task) Task
	raw        []Task
	tasks      []Task
}

func (p *parallel) Apply(ctx context.Context, record *Record) error {
	group, ctx := errgroup.WithContext(ctx)
	for _, t := range p.tasks {
		task := t
		group.Go(func() error {
			return task.Apply(ctx, record)
		})
	}
	return group.Wait()
}

func (p *parallel) Use(middleware ...func(Task) Task) {
	p.middleware = append(p.middleware, middleware...)
	p.tasks = wrap(p.raw, p.middleware...)
}

// Parallel executes the requested tasks in parallel
func Parallel(tasks ...Task) Runner {
	return &parallel{
		raw:   tasks,
		tasks: tasks,
	}
}

type serial struct {
	middleware []func(Task) Task
	raw        []Task
	tasks      []Task
}

func (s *serial) Apply(ctx context.Context, record *Record) error {
	for _, task := range s.tasks {
		if err := task.Apply(ctx, record); err != nil {
			return err
		}
	}
	return nil
}

func (s *serial) Use(middleware ...func(Task) Task) {
	s.middleware = append(s.middleware, middleware...)
	s.tasks = wrap(s.raw, s.middleware...)
}

// Serial applies the tasks in serial
func Serial(tasks ...Task) Runner {
	return &serial{
		raw:   tasks,
		tasks: tasks,
	}
}

func wrap(tasks []Task, middleware ...func(Task) Task) []Task {
	var wrapped []Task
	for _, t := range tasks {
		task := t
		for _, m := range middleware {
			task = m(task)
		}
		wrapped = append(wrapped, task)
	}
	return wrapped
}

package task

type FieldMapperFunc func(string) (string, error)

type ValueMapperFunc func(interface{}) (interface{}, error)

type options struct {
	fields   []string
	mapField FieldMapperFunc
}

type Option func(*options)

func WithFields(fields ...string) Option {
	return func(o *options) {
		o.fields = fields
	}
}

func WithFieldMapper(fn FieldMapperFunc) Option {
	return func(o *options) {
		o.mapField = fn
	}
}

func WithPrefix(prefix string) Option {
	return func(o *options) {
		o.mapField = func(field string) (string, error) {
			return prefix + field, nil
		}
	}
}

func makeOptions(opts ...Option) options {
	o := options{
		mapField: defaultFieldMapper,
	}
	for _, opt := range opts {
		opt(&o)
	}
	return o
}

func defaultFieldMapper(field string) (string, error) {
	return field, nil
}

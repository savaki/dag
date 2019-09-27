package builtin

// FieldMapperFunc renames a field into another field. FieldMapperFunc should return
// the original field name if no mapping is to be performed.
type FieldMapperFunc func(string) (string, error)

// ValueMapperFunc performs transformation on a field value
type ValueMapperFunc func(interface{}) (interface{}, error)

type options struct {
	fields   []string
	mapField FieldMapperFunc
}

// Option provides functional options
type Option func(*options)

// WithFields limits an enrichment to the specified fields
func WithFields(fields ...string) Option {
	return func(o *options) {
		o.fields = fields
	}
}

// WithFieldMapper performs transformation on the field name; useful for canonicalization
// WithFieldMapper cannot be combined with WithPrefix
func WithFieldMapper(fn FieldMapperFunc) Option {
	return func(o *options) {
		o.mapField = fn
	}
}

// WithPrefix applies a prefix to each enrichment prior to the enrichment
// WithPrefix cannot be combined with WithFieldMapper
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

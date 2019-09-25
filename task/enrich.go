package task

import (
	"context"

	"github.com/savaki/dag"
)

// Enrich a record from the specified data source
func Enrich(ds DataSource, keyFunc KeyFunc, opts ...Option) dag.TaskFunc {
	options := makeOptions(opts...)

	return func(ctx context.Context, record *dag.Record) error {
		key, err := keyFunc(record)
		if err != nil {
			return err
		}

		that, err := ds.Get(ctx, key)
		if err != nil {
			return err
		}

		if len(options.fields) == 0 {
			options.fields = that.Fields()
		}

		for _, field := range options.fields {
			if v, err := that.Get(field); err == nil {
				if mapped, err := options.mapField(field); err == nil {
					record.Set(mapped, v)
				}
			}
		}

		return nil
	}
}

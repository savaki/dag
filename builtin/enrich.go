package builtin

import (
	"context"

	"github.com/savaki/dag"
)

// Enrich a record from the specified data source
func Enrich(label string, ds DataSource, keyFunc KeyFunc, opts ...Option) dag.Task {
	options := makeOptions(opts...)

	return withName(label, func(ctx context.Context, record *dag.Record) error {
		key, err := keyFunc(record)
		if err != nil {
			return err
		}

		that, err := ds.Get(ctx, key)
		if err != nil {
			return err
		}

		if len(options.fields) == 0 {
			for k := range that {
				options.fields = append(options.fields, k)
			}
		}

		for _, field := range options.fields {
			if v, ok := that[field]; ok {
				if mapped, err := options.mapField(field); err == nil {
					record.Set(mapped, v)
				}
			}
		}

		return nil
	})
}

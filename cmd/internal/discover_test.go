package internal

import (
	"context"
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
)

func TestDiscover_CanFailIfCredentialsInvalid(t *testing.T) {
	tma := getTestMysqlAccess()
	settings := DiscoverSettings{}
	tma.PingContextFn = func(ctx context.Context, source PlanetScaleSource) error {
		return errors.New("Access Denied")
	}
	_, err := Discover(context.Background(), PlanetScaleSource{}, tma, settings)
	assert.NotNil(t, err)
	assert.ErrorContains(t, err, "unable to access PlanetScale Database: Access Denied")
	assert.True(t, tma.PingContextFnInvoked)
	assert.False(t, tma.GetVitessTabletsFnInvoked)
}

func TestDiscover_CanFailIfCannotQuery(t *testing.T) {
	tma := getTestMysqlAccess()
	tma.GetTableNamesFn = func(ctx context.Context, source PlanetScaleSource) ([]string, error) {
		return []string{}, errors.New("read prohibited")
	}

	_, err := Discover(context.Background(), PlanetScaleSource{}, tma, DiscoverSettings{})
	assert.NotNil(t, err)
	assert.ErrorContains(t, err, "unable to retrieve table names: read prohibited")
	assert.True(t, tma.PingContextFnInvoked)
	assert.False(t, tma.GetVitessTabletsFnInvoked)
}

func TestDiscover_SchemaHasPrimaryKeys(t *testing.T) {
	tma := getTestMysqlAccess()
	tma.GetTableNamesFn = func(ctx context.Context, source PlanetScaleSource) ([]string, error) {
		return []string{
			"employees",
		}, nil
	}

	tma.GetTableSchemaFn = func(ctx context.Context, source PlanetScaleSource, s string) (map[string]StreamProperty, error) {
		return map[string]StreamProperty{
			"emp_no":     {Types: []string{"null", "string"}},
			"first_name": {Types: []string{"null", "string"}},
			"last_name":  {Types: []string{"null", "string"}},
		}, nil
	}

	tma.GetTablePrimaryKeysFn = func(ctx context.Context, source PlanetScaleSource, s string) ([]string, error) {
		return []string{
			"emp_no",
		}, nil
	}

	c, err := Discover(context.Background(), PlanetScaleSource{}, tma, DiscoverSettings{})
	assert.Nil(t, err)
	assert.True(t, tma.PingContextFnInvoked)
	assert.Len(t, c.Streams, 1)
	emp := c.Streams[0]
	assert.Equal(t, []string{"emp_no"}, emp.KeyProperties)
}

func TestDiscover_SchemaHasCursorProperties(t *testing.T) {
	tma := getTestMysqlAccess()
	tma.GetTableNamesFn = func(ctx context.Context, source PlanetScaleSource) ([]string, error) {
		return []string{
			"employees",
		}, nil
	}

	tma.GetTableSchemaFn = func(ctx context.Context, source PlanetScaleSource, s string) (map[string]StreamProperty, error) {
		return map[string]StreamProperty{
			"emp_no":     {Types: []string{"null", "string"}},
			"first_name": {Types: []string{"null", "string"}},
			"last_name":  {Types: []string{"null", "string"}},
		}, nil
	}

	tma.GetTablePrimaryKeysFn = func(ctx context.Context, source PlanetScaleSource, s string) ([]string, error) {
		return []string{
			"emp_no",
		}, nil
	}

	c, err := Discover(context.Background(), PlanetScaleSource{}, tma, DiscoverSettings{})
	assert.Nil(t, err)
	assert.True(t, tma.PingContextFnInvoked)
	assert.Len(t, c.Streams, 1)
	emp := c.Streams[0]
	assert.Equal(t, []string{"emp_no"}, emp.CursorProperties)
}

func TestDiscover_CanSelectAllTables(t *testing.T) {
	tma := getTestMysqlAccess()
	tma.GetTableNamesFn = func(ctx context.Context, source PlanetScaleSource) ([]string, error) {
		return []string{
			"employees",
		}, nil
	}

	tma.GetTableSchemaFn = func(ctx context.Context, source PlanetScaleSource, s string) (map[string]StreamProperty, error) {
		return map[string]StreamProperty{
			"emp_no":     {Types: []string{"null", "string"}},
			"first_name": {Types: []string{"null", "string"}},
			"last_name":  {Types: []string{"null", "string"}},
		}, nil
	}

	tma.GetTablePrimaryKeysFn = func(ctx context.Context, source PlanetScaleSource, s string) ([]string, error) {
		return []string{
			"emp_no",
		}, nil
	}

	c, err := Discover(context.Background(), PlanetScaleSource{}, tma, DiscoverSettings{
		AutoSelectTables: true,
	})
	assert.Nil(t, err)
	assert.True(t, tma.PingContextFnInvoked)
	assert.Len(t, c.Streams, 1)
	emp := c.Streams[0]
	mm := emp.Metadata.GetPropertyMap()
	assert.Equal(t, NodeMetadata{
		Inclusion:  "automatic",
		Selected:   true,
		BreadCrumb: []string{"properties", "emp_no"},
	}, mm["emp_no"].Metadata, "key properties should be auto-included")

	assert.Equal(t, NodeMetadata{
		Inclusion:  "available",
		Selected:   true,
		BreadCrumb: []string{"properties", "first_name"},
	}, mm["first_name"].Metadata, "non-key properties should be auto-selected")

	assert.Equal(t, NodeMetadata{
		Inclusion:  "available",
		Selected:   true,
		BreadCrumb: []string{"properties", "last_name"},
	}, mm["last_name"].Metadata, "non-key properties should be auto-selected")
}

func TestDiscover_CanExcludeTables(t *testing.T) {
	tma := getTestMysqlAccess()
	tma.GetTableNamesFn = func(ctx context.Context, source PlanetScaleSource) ([]string, error) {
		return []string{
			"employees",
			"customers",
		}, nil
	}

	tma.GetTableSchemaFn = func(ctx context.Context, source PlanetScaleSource, s string) (map[string]StreamProperty, error) {
		return map[string]StreamProperty{
			"emp_no":     {Types: []string{"null", "string"}},
			"first_name": {Types: []string{"null", "string"}},
			"last_name":  {Types: []string{"null", "string"}},
		}, nil
	}

	tma.GetTablePrimaryKeysFn = func(ctx context.Context, source PlanetScaleSource, s string) ([]string, error) {
		return []string{
			"emp_no",
		}, nil
	}

	c, err := Discover(context.Background(), PlanetScaleSource{}, tma, DiscoverSettings{
		AutoSelectTables: true,
		ExcludedTables: []string{
			"employees",
		},
	})
	assert.Nil(t, err)
	assert.True(t, tma.PingContextFnInvoked)
	assert.Len(t, c.Streams, 1)
	emp := c.Streams[0]
	assert.Equal(t, emp.Name, "customers")
	mm := emp.Metadata.GetPropertyMap()
	assert.Equal(t, NodeMetadata{
		Inclusion:  "automatic",
		Selected:   true,
		BreadCrumb: []string{"properties", "emp_no"},
	}, mm["emp_no"].Metadata, "key properties should be auto-included")

	assert.Equal(t, NodeMetadata{
		Inclusion:  "available",
		Selected:   true,
		BreadCrumb: []string{"properties", "first_name"},
	}, mm["first_name"].Metadata, "non-key properties should be auto-selected")

	assert.Equal(t, NodeMetadata{
		Inclusion:  "available",
		Selected:   true,
		BreadCrumb: []string{"properties", "last_name"},
	}, mm["last_name"].Metadata, "non-key properties should be auto-selected")
}

func TestDiscover_SchemaHasValidMetadata(t *testing.T) {
	tma := getTestMysqlAccess()
	tma.GetTableNamesFn = func(ctx context.Context, source PlanetScaleSource) ([]string, error) {
		return []string{
			"employees",
		}, nil
	}

	tma.GetTableSchemaFn = func(ctx context.Context, source PlanetScaleSource, s string) (map[string]StreamProperty, error) {
		return map[string]StreamProperty{
			"emp_no":     {Types: []string{"null", "string"}},
			"first_name": {Types: []string{"null", "string"}},
			"last_name":  {Types: []string{"null", "string"}},
		}, nil
	}

	tma.GetTablePrimaryKeysFn = func(ctx context.Context, source PlanetScaleSource, s string) ([]string, error) {
		return []string{
			"emp_no",
		}, nil
	}

	c, err := Discover(context.Background(), PlanetScaleSource{}, tma, DiscoverSettings{})
	assert.Nil(t, err)
	assert.True(t, tma.PingContextFnInvoked)
	assert.Len(t, c.Streams, 1)
	emp := c.Streams[0]
	mm := emp.Metadata.GetPropertyMap()
	assert.Equal(t, NodeMetadata{
		Inclusion:  "automatic",
		BreadCrumb: []string{"properties", "emp_no"},
	}, mm["emp_no"].Metadata, "key properties should be auto-included")

	assert.Equal(t, NodeMetadata{
		Inclusion:  "available",
		BreadCrumb: []string{"properties", "first_name"},
	}, mm["first_name"].Metadata, "non-key properties should be selectable")

	assert.Equal(t, NodeMetadata{
		Inclusion:  "available",
		BreadCrumb: []string{"properties", "last_name"},
	}, mm["last_name"].Metadata, "non-key properties should be selectable")
}

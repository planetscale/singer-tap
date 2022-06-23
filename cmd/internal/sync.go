package internal

import (
	"context"
	"github.com/pkg/errors"
)

func Sync(ctx context.Context, logger *Logger, source PlanetScaleSource, catalog Catalog, state State) error {
	_, err := filterSchema(catalog)
	if err != nil {
		return errors.Wrap(err, "unable to filter schema")
	}

	return nil
}

// filterSchema returns only the selected streams from a given catalog
func filterSchema(catalog Catalog) (Catalog, error) {
	filteredCatalog := Catalog{
		Type: "CATALOG",
	}
	for _, stream := range catalog.Streams {
		tableMetadata, err := stream.GetTableMetadata()
		if err != nil {
			return filteredCatalog, err
		}

		// if stream is selected.
		if tableMetadata.Metadata.Selected {
			fstream := stream
			// empty out the properties of this stream
			// add back only the properties that are selected.
			fstream.Schema.Properties = make(map[string]StreamProperty)
			propertyMetadataMap := stream.Metadata.GetPropertyMap()
			for name, prop := range stream.Schema.Properties {
				// if field was selected
				if propertyMetadataMap[name].Metadata.Selected {
					fstream.Schema.Properties[name] = prop
				}
			}
			filteredCatalog.Streams = append(filteredCatalog.Streams, fstream)
		}

	}
	return filteredCatalog, nil
}

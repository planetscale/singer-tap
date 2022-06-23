## HTTP Tap

HTTP Tap is a binary executable that can consume messages produced by other Singer Taps and
sends them to the [Batch Import API](https://www.stitchdata.com/docs/developers/import-api/api#batch-data) for Stitch. 

The primary purpose of this binary is to allow PlanetScale to push data from a PlanetScale database to Stitch
while we run through the integration checklist for Stitch.

``` bash
go run cmd/singer-tap/main.go --config sources/demo/source.json  --discover >2 /dev/null |  go run cmd/http-tap/main.go --api-token "something" 
```
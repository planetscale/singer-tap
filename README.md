# singer-tap
Singer.io tap for extracting PlanetScale data

Singer specification for building a tap is available [here](https://github.com/singer-io/getting-started/blob/master/docs/SPEC.md#singer-specification)

Since this is a tap, it can run [Discover mode](https://github.com/singer-io/getting-started/blob/master/docs/DISCOVERY_MODE.md#discovery-mode) or [Sync mode](https://github.com/singer-io/getting-started/blob/master/docs/SYNC_MODE.md#sync-mode)

## Local Development
**1. Generate config file to connect to your PlanetScale database.** 
1. Opt-in to Connect for your organization.
2. Generate a password to your PlanetScale database.
3. Make a copy of the `sample/config.json` available in this repo and edit values, 
lets call this `sample/employee_database.json`

### Running in Discover Mode

Singer taps are a binary executable with a single entry point. 
We signal that we want to discover the source by passing in the `--discover` flag.

**Example invocation**
``` bash
go run cmd/singer-tap/main.go --config sources/demo/employee_database.json --discover
```

**Example output**
``` bash
go run cmd/singer-tap/main.go --config sources/demo/employee_database.json --discover  | jq .
PlanetScale Tap : INFO : Discovering Schema for PlanetScale database : employees
{
    "streams":
    [
        {
            "stream": "departments",
            "tap_stream_id": "employees:departments",
            "schema":
            {
                "type":
                [
                    "null",
                    "object"
                ],
                "additionalProperties": false,
                "properties":
                {
                    "dept_name":
                    {
                        "type":
                        [
                            "null",
                            "string"
                        ]
                    },
                    "dept_no":
                    {
                        "type":
                        [
                            "null",
                            "string"
                        ]
                    }
                }
            },
            "table-name": "departments",
            "metadata":
            [
                {
                    "metadata":
                    {
                        "inclusion": "available",
                        "valid-replication-keys":
                        [
                            "dept_no"
                        ],
                        "table-key-properties":
                        [
                            "dept_no"
                        ],
                        "breadcrumb":
                        []
                    }
                },
                {
                    "metadata":
                    {
                        "inclusion": "available",
                        "breadcrumb":
                        [
                            "properties",
                            "dept_no"
                        ]
                    }
                },
                {
                    "metadata":
                    {
                        "inclusion": "available",
                        "breadcrumb":
                        [
                            "properties",
                            "dept_name"
                        ]
                    }
                }
            ]
        },
        {
            "stream": "dept_emp",
            "tap_stream_id": "employees:dept_emp",
            "schema":
            {
                "type":
                [
                    "null",
                    "object"
                ],
                "additionalProperties": false,
                "properties":
                {
                    "dept_no":
                    {
                        "type":
                        [
                            "null",
                            "string"
                        ]
                    },
                    "emp_no":
                    {
                        "type":
                        [
                            "null",
                            "integer"
                        ]
                    },
                    "from_date":
                    {
                        "type":
                        [
                            "null",
                            "date"
                        ]
                    },
                    "to_date":
                    {
                        "type":
                        [
                            "null",
                            "date"
                        ]
                    }
                }
            },
            "table-name": "dept_emp",
            "metadata":
            [
                {
                    "metadata":
                    {
                        "inclusion": "available",
                        "valid-replication-keys":
                        [
                            "emp_no",
                            "dept_no"
                        ],
                        "table-key-properties":
                        [
                            "emp_no",
                            "dept_no"
                        ],
                        "breadcrumb":
                        []
                    }
                },
                {
                    "metadata":
                    {
                        "inclusion": "available",
                        "breadcrumb":
                        [
                            "properties",
                            "to_date"
                        ]
                    }
                },
                {
                    "metadata":
                    {
                        "inclusion": "available",
                        "breadcrumb":
                        [
                            "properties",
                            "emp_no"
                        ]
                    }
                },
                {
                    "metadata":
                    {
                        "inclusion": "available",
                        "breadcrumb":
                        [
                            "properties",
                            "dept_no"
                        ]
                    }
                },
                {
                    "metadata":
                    {
                        "inclusion": "available",
                        "breadcrumb":
                        [
                            "properties",
                            "from_date"
                        ]
                    }
                }
            ]
        }
    ]
}
```

### Running in Sync Mode

To run the tap in Sync mode, run the CLI without the `--discover` flag

example: 

``` bash
$ go run cmd/singer-tap/main.go --config sources/demo/source.json --catalog sources/demo/departments.json
```

#### Output
``` bash
PlanetScale Tap : INFO : PlanetScale Singer Tap : version [""], commit [""], published on [""]
PlanetScale Tap : INFO : Syncing records for PlanetScale database : import-on-scaler
{"type":"SCHEMA","stream":"departments","tap_stream_id":"import-on-scaler:departments","schema":{"type":["null","object"],"additionalProperties":false,"properties":{"dept_name":{"type":["null","string"]},"dept_no":{"type":["null","string"]}}},"table-name":"departments","metadata":[{"metadata":{"selected":true,"inclusion":"available","breadcrumb":["properties","dept_name"]}},{"metadata":{"inclusion":"available","breadcrumb":["properties","dept_no"]}},{"metadata":{"selected":true,"replication-method":"INCREMENTAL","inclusion":"available","valid-replication-keys":["dept_no"],"table-key-properties":["dept_no"],"breadcrumb":[]}}],"key_properties":["dept_no"],"bookmark_properties":["dept_no"]}
PlanetScale Tap : INFO : Stream "departments" will be synced incrementally
PlanetScale Tap : INFO : syncing rows from stream "departments" from shard "-"
PlanetScale Tap : INFO : [departments shard : -] peeking to see if there's any new rows
PlanetScale Tap : INFO : new rows found, syncing rows for 1m0s
PlanetScale Tap : INFO : [departments shard : -] syncing rows with cursor [shard:"-" keyspace:"import-on-scaler"]
PlanetScale Tap : INFO : Syncing with cursor position : [], using last known PK : false, stop cursor is : [MySQL56/e42292e8-e28f-11ec-9c5b-d680f5d655b3:1-717,e4e20f06-e28f-11ec-8d20-8e7ac09cb64c:1-44,eba743a8-e28f-11ec-9227-62aa711d33c6:1-32]
c{"type":"RECORD","stream":"departments","time_extracted":"2022-07-26T15:17:04.574252-05:00","record":{"dept_name":"Marketing","dept_no":"d001"}}
{"type":"RECORD","stream":"departments","time_extracted":"2022-07-26T15:17:04.574486-05:00","record":{"dept_name":"Finance","dept_no":"d002"}}
{"type":"RECORD","stream":"departments","time_extracted":"2022-07-26T15:17:04.574489-05:00","record":{"dept_name":"Human Resources","dept_no":"d003"}}
{"type":"RECORD","stream":"departments","time_extracted":"2022-07-26T15:17:04.574491-05:00","record":{"dept_name":"Production","dept_no":"d004"}}
{"type":"RECORD","stream":"departments","time_extracted":"2022-07-26T15:17:04.574493-05:00","record":{"dept_name":"Development","dept_no":"d005"}}
{"type":"RECORD","stream":"departments","time_extracted":"2022-07-26T15:17:04.574499-05:00","record":{"dept_name":"Quality Management","dept_no":"d006"}}
{"type":"RECORD","stream":"departments","time_extracted":"2022-07-26T15:17:04.574501-05:00","record":{"dept_name":"Sales","dept_no":"d007"}}
{"type":"RECORD","stream":"departments","time_extracted":"2022-07-26T15:17:04.574503-05:00","record":{"dept_name":"Research","dept_no":"d008"}}
{"type":"RECORD","stream":"departments","time_extracted":"2022-07-26T15:17:04.574505-05:00","record":{"dept_name":"Customer Service","dept_no":"d009"}}
{"type":"RECORD","stream":"departments","time_extracted":"2022-07-26T15:17:04.574507-05:00","record":{"dept_name":"insert test","dept_no":"d010"}}
{"type":"RECORD","stream":"departments","time_extracted":"2022-07-26T15:17:04.574509-05:00","record":{"dept_name":"Primary mode","dept_no":"d011"}}
{"type":"RECORD","stream":"departments","time_extracted":"2022-07-26T15:17:04.574511-05:00","record":{"dept_name":"Replica mode","dept_no":"d012"}}
{"type":"RECORD","stream":"departments","time_extracted":"2022-07-26T15:17:04.574517-05:00","record":{"dept_name":"insert test 2","dept_no":"d01c"}}
{"type":"RECORD","stream":"departments","time_extracted":"2022-07-26T15:17:04.574519-05:00","record":{"dept_name":"Incremental Sync 8","dept_no":"d092"}}
{"type":"RECORD","stream":"departments","time_extracted":"2022-07-26T15:17:04.574522-05:00","record":{"dept_name":"Incremental Sync 7","dept_no":"d093"}}
{"type":"RECORD","stream":"departments","time_extracted":"2022-07-26T15:17:04.574524-05:00","record":{"dept_name":"Incremental Sync 6","dept_no":"d094"}}
{"type":"RECORD","stream":"departments","time_extracted":"2022-07-26T15:17:04.574526-05:00","record":{"dept_name":"Incremental Sync 5","dept_no":"d095"}}
{"type":"RECORD","stream":"departments","time_extracted":"2022-07-26T15:17:04.574528-05:00","record":{"dept_name":"Incremental Sync 4","dept_no":"d096"}}
{"type":"RECORD","stream":"departments","time_extracted":"2022-07-26T15:17:04.574533-05:00","record":{"dept_name":"Incremental Sync 3","dept_no":"d097"}}
{"type":"RECORD","stream":"departments","time_extracted":"2022-07-26T15:17:04.574535-05:00","record":{"dept_name":"Incremental Sync 2","dept_no":"d098"}}
{"type":"RECORD","stream":"departments","time_extracted":"2022-07-26T15:17:04.574537-05:00","record":{"dept_name":"Incremental Sync","dept_no":"d099"}}
PlanetScale Tap : INFO : [departments shard : -] Continuing with cursor after server timeout
PlanetScale Tap : INFO : [departments shard : -] peeking to see if there's any new rows
PlanetScale Tap : INFO : [departments shard : -] no new rows found, exiting
{"type":"STATE","value":{"bookmarks":{"departments":{"shards":{"-":{"cursor":"CgEtEhBpbXBvcnQtb24tc2NhbGVyGoYBTXlTUUw1Ni9lNDIyOTJlOC1lMjhmLTExZWMtOWM1Yi1kNjgwZjVkNjU1YjM6MS03MTcsZTRlMjBmMDYtZTI4Zi0xMWVjLThkMjAtOGU3YWMwOWNiNjRjOjEtNDQsZWJhNzQzYTgtZTI4Zi0xMWVjLTkyMjctNjJhYTcxMWQzM2M2OjEtMzI="}}}}}}
{"type":"STATE","value":{"bookmarks":{"departments":{"shards":{"-":{"cursor":"CgEtEhBpbXBvcnQtb24tc2NhbGVyGoYBTXlTUUw1Ni9lNDIyOTJlOC1lMjhmLTExZWMtOWM1Yi1kNjgwZjVkNjU1YjM6MS03MTcsZTRlMjBmMDYtZTI4Zi0xMWVjLThkMjAtOGU3YWMwOWNiNjRjOjEtNDQsZWJhNzQzYTgtZTI4Zi0xMWVjLTkyMjctNjJhYTcxMWQzM2M2OjEtMzI="}}}}}}
```

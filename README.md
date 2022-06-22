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

**Coming soon**
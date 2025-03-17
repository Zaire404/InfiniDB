# InfiniDB

[![Go Reference](https://pkg.go.dev/badge/github.com/Zaire404/InfiniDB.svg)](https://pkg.go.dev/github.com/Zaire404/InfiniDB)
[![Go Report Card](https://goreportcard.com/badge/github.com/Zaire404/InfiniDB)](https://goreportcard.com/report/github.com/Zaire404/InfiniDB)

InfiniDB is a fully native and persistent key/value store in Go.

## Table of Contents
- [InfiniDB](#infinidb)
  - [Table of Contents](#table-of-contents)
  - [Getting Started](#getting-started)
    - [Installing](#installing)
    - [Opening a database](#opening-a-database)
    - [Getting a value](#getting-a-value)
    - [Setting a value](#setting-a-value)
    - [Deleting a value](#deleting-a-value)
  - [Comparison with other key/value stores](#comparison-with-other-keyvalue-stores)
## Getting Started

### Installing
To start using InfiniDB, install Go and run `go get`:
```sh
$ go get github.com/Zaire404/InfiniDB
```

### Opening a database
To open your database, simply use the `infinidb.Open()` function:
```go
package main

import (
	"github.com/Zaire404/InfiniDB"
)

func main() {
	opt := infinidb.DefaultOptions()
	db := infinidb.Open(&opt)
	defer db.Close()

    ...
}
```

### Getting a value
To get a value from the database, use the `Get()` function:
```go
entry, err := db.Get([]byte("key"))
...
```

### Setting a value
To set a value in the database, use the `Set()` function:
```go
err := db.Set(&util.Entry{Key: []byte("key"), ValueStruct: util.ValueStruct{Value: []byte("value")}})
...
```

### Deleting a value
To delete a value from the database, use the `Del()` function:
```go
err := db.Del([]byte("key"))
..
```

## Comparison with other key/value stores

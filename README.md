# Tarantool

[Tarantool 1.6+](http://tarantool.org/) client on Go.

## Usage

```go
package main

import (
  "github.com/tarantool/go-tarantool"
  "time"
  "fmt"
)

func main() {
    spaceNo := uint32(512)
    indexNo := uint32(0)
    tuple1 := []interface{}{12, "Hello World", "Olga"}

    server := "127.0.0.1:3013"
    opts := tarantool.Opts{
        Timeout: 500 * time.Millisecond,
        Reconnect: 3 * time.Second,
        MaxReconnects: 10,
        User: "test",
        Pass: "pass",
    }
    client, err := tarantool.Connect(server, opts)
    if err != nil {
        log.Fatalf("Failed to connect: %s", err.Error())
    }

    resp, err = client.Ping()
    fmt.Println(resp.Code)
    fmt.Println(resp.Data)
    fmt.Println(err)

    resp, err = client.Insert(spaceNo, tuple1)
    fmt.Println("Insert")
    fmt.Println("ERROR", err)
    fmt.Println("Code", resp.Code)
    fmt.Println("Data", resp.Data)
    fmt.Println("----")

    // TODO: complete doc
}

```

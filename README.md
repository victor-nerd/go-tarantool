# Tarantool

[Tarantool 1.6+](http://tarantool.org/) client on Go.

## Usage

```go
package main

import (
	"github.com/tarantool/go-tarantool"
	"log"
	"time"
)

func main() {
	spaceNo := uint32(512)
	indexNo := uint32(0)
	tuple1 := []interface{}{12, "Hello World", "Olga"}
	tuple2 := []interface{}{13, "99 bugs in code", "Anna"}

	server := "127.0.0.1:3013"
	opts := tarantool.Opts{
		Timeout:       500 * time.Millisecond,
		Reconnect:     1 * time.Second,
		MaxReconnects: 3,
		User:          "test",
		Pass:          "test",
	}
	client, err := tarantool.Connect(server, opts)
	if err != nil {
		log.Fatalf("Failed to connect: %s", err.Error())
	}

	resp, err := client.Ping()
	log.Println(resp.Code)
	log.Println(resp.Data)
	log.Println(err)

	resp, err = client.Insert(spaceNo, tuple1)
	log.Println("Insert")
	log.Println("Error", err)
	log.Println("Code", resp.Code)
	log.Println("Data", resp.Data)

	resp, err = client.Replace(spaceNo, tuple2)
	log.Println("Replace")
	log.Println("Error", err)
	log.Println("Code", resp.Code)
	log.Println("Data", resp.Data)

	resp, err = client.Delete(spaceNo, indexNo, tuple1[0:1])
	log.Println("Delete")
	log.Println("Error", err)
	log.Println("Code", resp.Code)
	log.Println("Data", resp.Data)

	resp, err = client.Select(spaceNo, indexNo, 0, 1, tarantool.IterEq, tuple2[0:1])
	log.Println("Select")
	log.Println("Error", err)
	log.Println("Code", resp.Code)
	log.Println("Data", resp.Data)

	resp, err = client.Call("func_name", []interface{}{1, 2, 3})
	log.Println("Call")
	log.Println("Error", err)
	log.Println("Code", resp.Code)
	log.Println("Data", resp.Data)
}

```

# Tarantool

[Tarantool 1.6+](http://tarantool.org/) client on Go.

## Usage

```go
package main

import (
  "github.com/tarantool/go-tarantool"
  "fmt"
)

func main() {
  server    := "127.0.0.1:3013"
  spaceNo   := uint32(514)
  indexNo   := uint32(0)
  limit     := uint32(10)
  offset    := uint32(0)
  iterator  := tarantool.IterAll
  key       := []interface{}{ 12 }
  tuple1    := []interface{}{ 12, "Hello World", "Olga" }
  tuple2    := []interface{}{ 12, "Hello Mars", "Anna" }
  upd_tuple := []interface{}{ []interface{}{ "=", 1, "Hello Moon" }, []interface{}{ "#", 2, 1 } }

  functionName  := "box.cfg()"
  functionTuple := []interface{}{ "box.schema.SPACE_ID" }


  client, err := tarantool.Connect(server)

  var resp *tarantool.Response

  resp, err = client.Ping()
  fmt.Println("Ping")
  fmt.Println("ERROR", err)
  fmt.Println("Code", resp.Code)
  fmt.Println("Data", resp.Data)
  fmt.Println("----")

  resp, err = client.Insert(spaceNo, tuple1)
  fmt.Println("Insert")
  fmt.Println("ERROR", err)
  fmt.Println("Code", resp.Code)
  fmt.Println("Data", resp.Data)
  fmt.Println("----")

  resp, err = client.Select(spaceNo, indexNo, offset, limit, iterator, key)
  fmt.Println("Select")
  fmt.Println("ERROR", err)
  fmt.Println("Code", resp.Code)
  fmt.Println("Data", resp.Data)
  fmt.Println("----")

  resp, err = client.Replace(spaceNo, tuple2)
  fmt.Println("Replace")
  fmt.Println("ERROR", err)
  fmt.Println("Code", resp.Code)
  fmt.Println("Data", resp.Data)
  fmt.Println("----")

  resp, err = client.Select(spaceNo, indexNo, offset, limit, iterator, key)
  fmt.Println("Select")
  fmt.Println("ERROR", err)
  fmt.Println("Code", resp.Code)
  fmt.Println("Data", resp.Data)
  fmt.Println("----")

  resp, err = client.Update(spaceNo, indexNo, key, upd_tuple)
  fmt.Println("Update")
  fmt.Println("ERROR", err)
  fmt.Println("Code", resp.Code)
  fmt.Println("Data", resp.Data)
  fmt.Println("----")

  resp, err = client.Select(spaceNo, indexNo, offset, limit, iterator, key)
  fmt.Println("Select")
  fmt.Println("ERROR", err)
  fmt.Println("Code", resp.Code)
  fmt.Println("Data", resp.Data)
  fmt.Println("----")

  resp, err = client.Delete(spaceNo, indexNo, key)
  fmt.Println("Delete")
  fmt.Println("ERROR", err)
  fmt.Println("Code", resp.Code)
  fmt.Println("Data", resp.Data)
  fmt.Println("----")

  resp, err = client.Call(functionName, functionTuple)
  fmt.Println("Call")
  fmt.Println("ERROR", err)
  fmt.Println("Code", resp.Code)
  fmt.Println("Data", resp.Data)
  fmt.Println("----")
}

// #=> Connecting to 127.0.0.1:3013 ...
// #=> Connected ...
// #=> Greeting ... Success
// #=> Version: Tarantool 1.6.2-34-ga53cf4a
// #=> 
// #=> Insert
// #=> ERROR <nil>
// #=> Code 0
// #=> Data [[12 Hello World Olga]]
// #=> ----
// #=> Select
// #=> ERROR <nil>
// #=> Code 0
// #=> Data [[12 Hello World Olga]]
// #=> ----
// #=> Replace
// #=> ERROR <nil>
// #=> Code 0
// #=> Data [[12 Hello Mars Anna]]
// #=> ----
// #=> Select
// #=> ERROR <nil>
// #=> Code 0
// #=> Data [[12 Hello Mars Anna]]
// #=> ----
// #=> Update
// #=> ERROR <nil>
// #=> Code 0
// #=> Data [[12 Hello Moon]]
// #=> ----
// #=> Select
// #=> ERROR <nil>
// #=> Code 0
// #=> Data [[12 Hello Moon]]
// #=> ----
// #=> Delete
// #=> ERROR <nil>
// #=> Code 0
// #=> Data [[12 Hello Moon]]
// #=> ----
// #=> Call
// #=> ERROR Execute access denied for user 'guest' to function 'box.cfg()'
// #=> Code 13570
// #=> Data []
// #=> ----
```

package main

import (
	"fmt"
	"log"

	"github.com/victor-nerd/go-tarantool"
)

// tarantool
// box.cfg{listen=3301}
// box.schema.user.grant('guest','read,write,execute,create,drop','universe')

func main() {
	opts := tarantool.Opts{User: "guest"}
	conn, err := tarantool.Connect("127.0.0.1:3301", opts)
	if err != nil {
		panic(err)
	}

	resp, err := conn.Execute("CREATE TABLE modules (name STRING, size INTEGER, purpose STRING, PRIMARY KEY (name));")
	if err != nil {
		fmt.Println(err)
	}
	if resp != nil {
		log.Println(resp.Error)
		log.Println(resp.Data)
		log.Println(resp.Code)
	}

	resp, err = conn.Execute("INSERT INTO modules (name, size, purpose) values('test', 100, 'purpose');")
	if err != nil {
		fmt.Println(err)
	}
	if resp != nil {
		log.Println(resp.Error)
		log.Println(resp.Data)
		log.Println(resp.Code)
	}

	resp, err = conn.Execute("SELECT * FROM modules")
	if err != nil {
		fmt.Println(err)
	}
	if resp != nil {
		log.Println(resp.Error)
		log.Println(resp.Data)
		log.Println(resp.Code)
	}

}

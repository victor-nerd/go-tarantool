package tarantool

import (
	"fmt"
	"gopkg.in/vmihailenco/msgpack.v2"
	"reflect"
	"testing"
	"time"
)

type tuple struct {
	Id   uint
	Msg  string
	Name string
}

func init() {
	msgpack.Register(reflect.TypeOf(new(tuple)).Elem(), encodeTuple, decodeTuple)
}

var server = "127.0.0.1:3013"
var spaceNo = uint32(512)
var indexNo = uint32(0)
var opts = Opts{Timeout: 500 * time.Millisecond}

const N = 500

func BenchmarkClientSerial(b *testing.B) {
	var err error

	conn, err := Connect(server, opts)
	if err != nil {
		b.Errorf("No connection available")
	}

	_, err = conn.Replace(spaceNo, []interface{}{uint(1), "hello", "world"})
	if err != nil {
		b.Errorf("No connection available")
	}

	for i := 0; i < b.N; i++ {
		_, err = conn.Select(spaceNo, indexNo, 0, 1, IterAll, []interface{}{uint(1)})
		if err != nil {
			b.Errorf("No connection available")
		}

	}
}

func BenchmarkClientFuture(b *testing.B) {
	var err error

	conn, err := Connect(server, opts)
	if err != nil {
		b.Error(err)
	}

	_, err = conn.Replace(spaceNo, []interface{}{uint(1), "hello", "world"})
	if err != nil {
		b.Error(err)
	}

	for i := 0; i < b.N; i += N {
		var fs [N]*Future
		for j := 0; j < N; j++ {
			fs[j] = conn.SelectAsync(spaceNo, indexNo, 0, 1, IterAll, []interface{}{uint(1)})
		}
		for j := 0; j < N; j++ {
			_, err = fs[j].Get()
			if err != nil {
				b.Error(err)
			}
		}

	}
}

func encodeTuple(e *msgpack.Encoder, v reflect.Value) error {
	t := v.Interface().(tuple)
	if err := e.EncodeSliceLen(3); err != nil {
		return err
	}
	if err := e.EncodeUint(t.Id); err != nil {
		return err
	}
	if err := e.EncodeString(t.Msg); err != nil {
		return err
	}
	if err := e.EncodeString(t.Name); err != nil {
		return err
	}
	return nil
}

func decodeTuple(d *msgpack.Decoder, v reflect.Value) error {
	var err error
	var l int
	t := v.Addr().Interface().(*tuple)
	if l, err = d.DecodeSliceLen(); err != nil {
		return err
	}
	if l != 3 {
		return fmt.Errorf("array len doesn't match: %d", l)
	}
	if t.Id, err = d.DecodeUint(); err != nil {
		return err
	}
	if t.Msg, err = d.DecodeString(); err != nil {
		return err
	}
	if t.Name, err = d.DecodeString(); err != nil {
		return err
	}
	return nil
}

func BenchmarkClientFutureTyped(b *testing.B) {
	var err error

	conn, err := Connect(server, opts)
	if err != nil {
		b.Errorf("No connection available")
	}

	_, err = conn.Replace(spaceNo, []interface{}{uint(1), "hello", "world"})
	if err != nil {
		b.Errorf("No connection available")
	}

	for i := 0; i < b.N; i += N {
		var fs [N]*Future
		for j := 0; j < N; j++ {
			fs[j] = conn.SelectAsync(spaceNo, indexNo, 0, 1, IterAll, []interface{}{uint(1)})
		}
		for j := 0; j < N; j++ {
			var r []tuple
			err = fs[j].GetTyped(&r)
			if err != nil {
				b.Error(err)
			}
			if len(r) != 1 || r[0].Id != 12 {
				b.Errorf("Doesn't match %v", r)
			}
		}

	}
}

func BenchmarkClientFutureParallel(b *testing.B) {
	var err error

	conn, err := Connect(server, opts)
	if err != nil {
		b.Errorf("No connection available")
	}

	_, err = conn.Replace(spaceNo, []interface{}{uint(1), "hello", "world"})
	if err != nil {
		b.Errorf("No connection available")
	}

	b.RunParallel(func(pb *testing.PB) {
		exit := false
		for !exit {
			var fs [N]*Future
			var j int
			for j = 0; j < N && pb.Next(); j++ {
				fs[j] = conn.SelectAsync(spaceNo, indexNo, 0, 1, IterAll, []interface{}{uint(1)})
			}
			exit = j < N
			for j > 0 {
				j--
				_, err = fs[j].Get()
				if err != nil {
					b.Error(err)
				}
			}
		}
	})
}

func BenchmarkClientFutureParallelTyped(b *testing.B) {
	var err error

	conn, err := Connect(server, opts)
	if err != nil {
		b.Errorf("No connection available")
	}

	_, err = conn.Replace(spaceNo, []interface{}{uint(1), "hello", "world"})
	if err != nil {
		b.Errorf("No connection available")
	}

	b.RunParallel(func(pb *testing.PB) {
		exit := false
		for !exit {
			var fs [N]*Future
			var j int
			for j = 0; j < N && pb.Next(); j++ {
				fs[j] = conn.SelectAsync(spaceNo, indexNo, 0, 1, IterAll, []interface{}{uint(1)})
			}
			exit = j < N
			for j > 0 {
				var r []tuple
				j--
				err = fs[j].GetTyped(&r)
				if err != nil {
					b.Error(err)
				}
				if len(r) != 1 || r[0].Id != 12 {
					b.Errorf("Doesn't match %v", r)
				}
			}
		}
	})
}

func BenchmarkClientParrallel(b *testing.B) {
	conn, err := Connect(server, opts)
	if err != nil {
		b.Errorf("No connection available")
	}

	_, err = conn.Replace(spaceNo, []interface{}{uint(1), "hello", "world"})
	if err != nil {
		b.Errorf("No connection available")
	}

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, err = conn.Select(spaceNo, indexNo, 0, 1, IterAll, []interface{}{uint(1)})
			if err != nil {
				b.Errorf("No connection available")
			}
		}
	})
}

///////////////////

func TestClient(t *testing.T) {
	var resp *Response
	var err error
	var conn *Connection

	conn, err = Connect(server, Opts{User: "test", Pass: "test"})
	if err != nil {
		t.Errorf("Failed to connect: %s", err.Error())
	}
	if conn == nil {
		t.Errorf("conn is nil after Connect")
	}

	// Ping
	resp, err = conn.Ping()
	if err != nil {
		t.Errorf("Failed to Ping: %s", err.Error())
	}
	if resp == nil {
		t.Errorf("Response is nil after Ping")
	}

	// Insert
	resp, err = conn.Insert(spaceNo, []interface{}{uint(1), "hello", "world"})
	if err != nil {
		t.Errorf("Failed to Insert: %s", err.Error())
	}
	if resp == nil {
		t.Errorf("Response is nil after Insert")
	}
	if len(resp.Data) != 1 {
		t.Errorf("Response Body len != 1")
	}
	if tpl, ok := resp.Data[0].([]interface{}); !ok {
		t.Errorf("Unexpected body of Insert")
	} else {
		if len(tpl) != 3 {
			t.Errorf("Unexpected body of Insert (tuple len)")
		}
		if id, ok := tpl[0].(uint64); !ok || id != 1 {
			t.Errorf("Unexpected body of Insert (0)")
		}
		if h, ok := tpl[1].(string); !ok || h != "hello" {
			t.Errorf("Unexpected body of Insert (1)")
		}
	}
	resp, err = conn.Insert(spaceNo, []interface{}{uint(1), "hello", "world"})
	if tntErr, ok := err.(Error); !ok || tntErr.Code != ErrTupleFound {
		t.Errorf("Expected ErrTupleFound but got: %v", err)
	}
	if len(resp.Data) != 0 {
		t.Errorf("Response Body len != 1")
	}

	// Delete
	resp, err = conn.Delete(spaceNo, indexNo, []interface{}{uint(1)})
	if err != nil {
		t.Errorf("Failed to Delete: %s", err.Error())
	}
	if resp == nil {
		t.Errorf("Response is nil after Delete")
	}
	if len(resp.Data) != 1 {
		t.Errorf("Response Body len != 1")
	}
	if tpl, ok := resp.Data[0].([]interface{}); !ok {
		t.Errorf("Unexpected body of Delete")
	} else {
		if len(tpl) != 3 {
			t.Errorf("Unexpected body of Delete (tuple len)")
		}
		if id, ok := tpl[0].(uint64); !ok || id != 1 {
			t.Errorf("Unexpected body of Delete (0)")
		}
		if h, ok := tpl[1].(string); !ok || h != "hello" {
			t.Errorf("Unexpected body of Delete (1)")
		}
	}
	resp, err = conn.Delete(spaceNo, indexNo, []interface{}{uint(101)})
	if err != nil {
		t.Errorf("Failed to Replace: %s", err.Error())
	}
	if resp == nil {
		t.Errorf("Response is nil after Delete")
	}
	if len(resp.Data) != 0 {
		t.Errorf("Response Data len != 0")
	}

	// Replace
	resp, err = conn.Replace(spaceNo, []interface{}{uint(2), "hello", "world"})
	if err != nil {
		t.Errorf("Failed to Replace: %s", err.Error())
	}
	if resp == nil {
		t.Errorf("Response is nil after Replace")
	}
	resp, err = conn.Replace(spaceNo, []interface{}{uint(2), "hi", "planet"})
	if err != nil {
		t.Errorf("Failed to Replace (duplicate): %s", err.Error())
	}
	if resp == nil {
		t.Errorf("Response is nil after Replace (duplicate)")
	}
	if len(resp.Data) != 1 {
		t.Errorf("Response Data len != 1")
	}
	if tpl, ok := resp.Data[0].([]interface{}); !ok {
		t.Errorf("Unexpected body of Replace")
	} else {
		if len(tpl) != 3 {
			t.Errorf("Unexpected body of Replace (tuple len)")
		}
		if id, ok := tpl[0].(uint64); !ok || id != 2 {
			t.Errorf("Unexpected body of Replace (0)")
		}
		if h, ok := tpl[1].(string); !ok || h != "hi" {
			t.Errorf("Unexpected body of Replace (1)")
		}
	}

	// Update
	resp, err = conn.Update(spaceNo, indexNo, []interface{}{uint(2)}, []interface{}{[]interface{}{"=", 1, "bye"}, []interface{}{"#", 2, 1}})
	if err != nil {
		t.Errorf("Failed to Update: %s", err.Error())
	}
	if resp == nil {
		t.Errorf("Response is nil after Update")
	}
	if len(resp.Data) != 1 {
		t.Errorf("Response Data len != 1")
	}
	if tpl, ok := resp.Data[0].([]interface{}); !ok {
		t.Errorf("Unexpected body of Update")
	} else {
		if len(tpl) != 2 {
			t.Errorf("Unexpected body of Update (tuple len)")
		}
		if id, ok := tpl[0].(uint64); !ok || id != 2 {
			t.Errorf("Unexpected body of Update (0)")
		}
		if h, ok := tpl[1].(string); !ok || h != "bye" {
			t.Errorf("Unexpected body of Update (1)")
		}
	}

	// Upsert
	resp, err = conn.Upsert(spaceNo, []interface{}{uint(3), 1}, []interface{}{[]interface{}{"+", 1, 1}})
	if err != nil {
		t.Errorf("Failed to Upsert (insert): %s", err.Error())
	}
	if resp == nil {
		t.Errorf("Response is nil after Upsert (insert)")
	}
	resp, err = conn.Upsert(spaceNo, []interface{}{uint(3), 1}, []interface{}{[]interface{}{"+", 1, 1}})
	if err != nil {
		t.Errorf("Failed to Upsert (update): %s", err.Error())
	}
	if resp == nil {
		t.Errorf("Response is nil after Upsert (update)")
	}

	// Select
	for i := 10; i < 20; i++ {
		resp, err = conn.Replace(spaceNo, []interface{}{uint(i), fmt.Sprintf("val %d", i), "bla"})
		if err != nil {
			t.Errorf("Failed to Replace: %s", err.Error())
		}
	}
	resp, err = conn.Select(spaceNo, indexNo, 0, 1, IterEq, []interface{}{uint(10)})
	if err != nil {
		t.Errorf("Failed to Select: %s", err.Error())
	}
	if resp == nil {
		t.Errorf("Response is nil after Select")
	}
	if len(resp.Data) != 1 {
		t.Errorf("Response Data len != 1")
	}
	if tpl, ok := resp.Data[0].([]interface{}); !ok {
		t.Errorf("Unexpected body of Select")
	} else {
		if id, ok := tpl[0].(uint64); !ok || id != 10 {
			t.Errorf("Unexpected body of Select (0)")
		}
		if h, ok := tpl[1].(string); !ok || h != "val 10" {
			t.Errorf("Unexpected body of Select (1)")
		}
	}

	// Select empty
	resp, err = conn.Select(spaceNo, indexNo, 0, 1, IterEq, []interface{}{uint(30)})
	if err != nil {
		t.Errorf("Failed to Select: %s", err.Error())
	}
	if resp == nil {
		t.Errorf("Response is nil after Select")
	}
	if len(resp.Data) != 0 {
		t.Errorf("Response Data len != 0")
	}

	// Select Typed
	var tpl []tuple
	err = conn.SelectTyped(spaceNo, indexNo, 0, 1, IterEq, []interface{}{uint(10)}, &tpl)
	if err != nil {
		t.Errorf("Failed to SelectTyped: %s", err.Error())
	}
	if len(tpl) != 1 {
		t.Errorf("Result len of SelectTyped != 1")
	} else {
		if tpl[0].Id != 10 {
			t.Errorf("Bad value loaded from SelectTyped")
		}
	}

	// Select Typed Empty
	var tpl2 []tuple
	err = conn.SelectTyped(spaceNo, indexNo, 0, 1, IterEq, []interface{}{uint(30)}, &tpl2)
	if err != nil {
		t.Errorf("Failed to SelectTyped: %s", err.Error())
	}
	if len(tpl2) != 0 {
		t.Errorf("Result len of SelectTyped != 1")
	}

	// Call
	resp, err = conn.Call("box.info", []interface{}{"box.schema.SPACE_ID"})
	if err != nil {
		t.Errorf("Failed to Call: %s", err.Error())
	}
	if resp == nil {
		t.Errorf("Response is nil after Call")
	}
	if len(resp.Data) < 1 {
		t.Errorf("Response.Data is empty after Eval")
	}

	// Eval
	resp, err = conn.Eval("return 5 + 6", []interface{}{})
	if err != nil {
		t.Errorf("Failed to Eval: %s", err.Error())
	}
	if resp == nil {
		t.Errorf("Response is nil after Eval")
	}
	if len(resp.Data) < 1 {
		t.Errorf("Response.Data is empty after Eval")
	}
	val := resp.Data[0].(uint64)
	if val != 11 {
		t.Errorf("5 + 6 == 11, but got %v", val)
	}

	// Schema
	schema := conn.Schema
	if schema.Spaces == nil {
		t.Errorf("schema.Spaces is nil")
	}
	if schema.SpacesN == nil {
		t.Errorf("schema.SpacesN is nil")
	}
	var space, space2 *Space
	var ok bool
	if space, ok = schema.Spaces[9991]; !ok {
		t.Errorf("space with id = 9991 was not found in schema.Spaces")
	}
	if space2, ok = schema.SpacesN["schematest"]; !ok {
		t.Errorf("space with name 'schematest' was not found in schema.Spaces")
	}
	if space != space2 {
		t.Errorf("space with id = 9991 and space with name schematest are different")
	}
	if space.Id != 9991 {
		t.Errorf("space 9991 has incorrect Id")
	}
	if space.Name != "schematest" {
		t.Errorf("space 9991 has incorrect Name")
	}
	if !space.Temporary {
		t.Errorf("space 9991 should be temporary")
	}
	if space.Engine != "memtx" {
		t.Errorf("space 9991 engine should be memtx")
	}
	if space.FieldsCount != 7 {
		t.Errorf("space 9991 has incorrect fields count")
	}

	if space.Fields == nil {
		t.Errorf("space.Fields is nill")
	}
	if space.FieldsN == nil {
		t.Errorf("space.FieldsN is nill")
	}
	if len(space.Fields) != 3 {
		t.Errorf("space.Fields len is incorrect")
	}
	if len(space.FieldsN) != 2 {
		t.Errorf("space.FieldsN len is incorrect")
	}

	var field1, field2, field5, field1_, field5_ *Field
	if field1, ok = space.Fields[1]; !ok {
		t.Errorf("field id = 1 was not found")
	}
	if field2, ok = space.Fields[2]; !ok {
		t.Errorf("field id = 2 was not found")
	}
	if field5, ok = space.Fields[5]; !ok {
		t.Errorf("field id = 5 was not found")
	}

	if field1_, ok = space.FieldsN["name1"]; !ok {
		t.Errorf("field name = name1 was not found")
	}
	if field5_, ok = space.FieldsN["name5"]; !ok {
		t.Errorf("field name = name5 was not found")
	}
	if field1 != field1_ || field5 != field5_ {
		t.Errorf("field with id = 1 and field with name 'name1' are different")
	}
	if field1.Name != "name1" {
		t.Errorf("field 1 has incorrect Name")
	}
	if field1.Type != "" {
		t.Errorf("field 1 has incorrect Type")
	}
	if field2.Name != "" {
		t.Errorf("field 2 has incorrect Name")
	}
	if field2.Type != "type2" {
		t.Errorf("field 2 has incorrect Type")
	}

	if space.Indexes == nil {
		t.Errorf("space.Indexes is nill")
	}
	if space.IndexesN == nil {
		t.Errorf("space.IndexesN is nill")
	}
	if len(space.Indexes) != 2 {
		t.Errorf("space.Indexes len is incorrect")
	}
	if len(space.IndexesN) != 2 {
		t.Errorf("space.IndexesN len is incorrect")
	}

	var index0, index3, index0_, index3_ *Index
	if index0, ok = space.Indexes[0]; !ok {
		t.Errorf("index id = 0 was not found")
	}
	if index3, ok = space.Indexes[3]; !ok {
		t.Errorf("index id = 3 was not found")
	}
	if index0_, ok = space.IndexesN["primary"]; !ok {
		t.Errorf("index name = primary was not found")
	}
	if index3_, ok = space.IndexesN["secondary"]; !ok {
		t.Errorf("index name = secondary was not found")
	}
	if index0 != index0_ || index3 != index3_ {
		t.Errorf("index with id = 3 and index with name 'secondary' are different")
	}
	if index3.Id != 3 {
		t.Errorf("index has incorrect Id")
	}
	if index0.Name != "primary" {
		t.Errorf("index has incorrect Name")
	}
	if index0.Type != "hash" || index3.Type != "tree" {
		t.Errorf("index has incorrect Type")
	}
	if !index0.Unique || index3.Unique {
		t.Errorf("index has incorrect Unique")
	}
	if index3.Fields == nil {
		t.Errorf("index.Fields is nil")
	}
	if len(index3.Fields) != 2 {
		t.Errorf("index.Fields len is incorrect")
	}

	ifield1 := index3.Fields[0]
	ifield2 := index3.Fields[1]
	if ifield1 == nil || ifield2 == nil {
		t.Errorf("index field is nil")
	}
	if ifield1.Id != 1 || ifield2.Id != 2 {
		t.Errorf("index field has incorrect Id")
	}
	if ifield1.Type != "num" || ifield2.Type != "STR" {
		t.Errorf("index field has incorrect Type[")
	}
}

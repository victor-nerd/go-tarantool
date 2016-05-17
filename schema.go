package tarantool

import (
	_ "fmt"
)

type Schema struct {
	Version uint
	Spaces  map[uint32]*Space
	SpacesN map[string]*Space
}

type Space struct {
	Id          uint32
	Name        string
	Engine      string
	Temporary   bool
	FieldsCount uint32
	Fields      map[uint32]*Field
	FieldsN     map[string]*Field
	Indexes     map[uint32]*Index
	IndexesN    map[string]*Index
}

type Field struct {
	Id   uint32
	Name string
	Type string
}

type Index struct {
	Id     uint32
	Name   string
	Type   string
	Unique bool
	Fields []*IndexField
}

type IndexField struct {
	Id   uint32
	Type string
}

const (
	maxSchemas = 10000
	spaceSpId  = 280
	vspaceSpId = 281
	indexSpId  = 288
	vindexSpId = 289
)

func (conn *Connection) loadSchema() (err error) {
	var req *Request
	var resp *Response

	schema := new(Schema)
	schema.Spaces = make(map[uint32]*Space)
	schema.SpacesN = make(map[string]*Space)

	// reload spaces
	req = conn.NewRequest(SelectRequest)
	req.fillSearch(spaceSpId, 0, []interface{}{})
	req.fillIterator(0, maxSchemas, IterAll)
	resp, err = req.perform()
	if err != nil {
		return err
	}
	for _, row := range resp.Data {
		row := row.([]interface{})
		space := new(Space)
		space.Id = uint32(row[0].(uint64))
		space.Name = row[2].(string)
		space.Engine = row[3].(string)
		space.FieldsCount = uint32(row[4].(uint64))
		space.Temporary = bool(row[5].(string) == "temporary")
		space.Fields = make(map[uint32]*Field)
		space.FieldsN = make(map[string]*Field)
		space.Indexes = make(map[uint32]*Index)
		space.IndexesN = make(map[string]*Index)
		for i, f := range row[6].([]interface{}) {
			if f == nil {
				continue
			}
			f := f.(map[interface{}]interface{})
			field := new(Field)
			field.Id = uint32(i)
			if name, ok := f["name"]; ok && name != nil {
				field.Name = name.(string)
			}
			if type_, ok := f["type"]; ok && type_ != nil {
				field.Type = type_.(string)
			}
			space.Fields[field.Id] = field
			if field.Name != "" {
				space.FieldsN[field.Name] = field
			}
		}

		schema.Spaces[space.Id] = space
		schema.SpacesN[space.Name] = space
	}

	// reload indexes
	req = conn.NewRequest(SelectRequest)
	req.fillSearch(indexSpId, 0, []interface{}{})
	req.fillIterator(0, maxSchemas, IterAll)
	resp, err = req.perform()
	if err != nil {
		return err
	}
	for _, row := range resp.Data {
		row := row.([]interface{})
		index := new(Index)
		index.Id = uint32(row[1].(uint64))
		index.Name = row[2].(string)
		index.Type = row[3].(string)
		opts := row[4].(map[interface{}]interface{})
		index.Unique = opts["unique"].(bool)
		for _, f := range row[5].([]interface{}) {
			f := f.([]interface{})
			field := new(IndexField)
			field.Id = uint32(f[0].(uint64))
			field.Type = f[1].(string)
			index.Fields = append(index.Fields, field)
		}
		spaceId := uint32(row[0].(uint64))
		schema.Spaces[spaceId].Indexes[index.Id] = index
		schema.Spaces[spaceId].IndexesN[index.Name] = index
	}
	conn.Schema = schema
	return nil
}

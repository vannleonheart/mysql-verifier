package main

import "mysql-verifier/src/lib"

type Config struct {
	Database lib.DatabaseConfig `json:"database"`
	In       string             `json:"in"`
	Out      string             `json:"out"`
	Schema   string             `json:"schema"`
}

type TableInfo struct {
	Name        string         `json:"name"`
	SchemaRows  int64          `json:"schema_rows"`
	SizeInMB    float64        `json:"size_mb"`
	ColumnSize  *int           `json:"column_size"`
	HasIDColumn bool           `json:"has_id_column"`
	CountRows   int64          `json:"count_rows"`
	MaxId       *string        `json:"max_id"`
	LastRow     *[]interface{} `json:"last_row"`
	Hash        string         `json:"hash"`
	Duration    string         `json:"duration"`
}

type Result struct {
	In       string               `json:"in"`
	Out      string               `json:"out"`
	Schema   string               `json:"schema"`
	Start    int64                `json:"start"`
	End      int64                `json:"end"`
	Duration string               `json:"duration"`
	Tables   map[string]TableInfo `json:"tables"`
	Status   string               `json:"status"`
}

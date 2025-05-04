package main

import "mysql-verifier/src/lib"

type Config struct {
	Database lib.DatabaseConfig `json:"database"`
	In       string             `json:"in"`
	Out      string             `json:"out"`
	Schema   string             `json:"schema"`
}

type TableInfo struct {
	Name       string       `json:"name"`
	SchemaRows int64        `json:"schema_rows"`
	CountRows  int64        `json:"count_rows"`
	SizeInMB   float64      `json:"size_mb"`
	MaxId      *string      `json:"max_id"`
	LastRow    *interface{} `json:"last_row"`
	Hash       string       `json:"hash"`
}

type Result struct {
	In       string               `json:"in"`
	Out      string               `json:"out"`
	Schema   string               `json:"schema"`
	Start    string               `json:"start"`
	End      string               `json:"end"`
	Duration string               `json:"duration"`
	Tables   map[string]TableInfo `json:"tables"`
	Status   string               `json:"status"`
}

package main

import (
	"mysql-verifier/src/lib"
	"time"
)

var (
	initStart time.Time

	configFilename *string
	inputFileName  *string
	outputFileName *string
	schemaFileName *string
	dbHost         *string
	dbPort         *string
	dbUser         *string
	dbName         *string

	config         Config
	dbCon          *lib.DatabaseClient
	tables         []TableInfo
	previousResult *Result
	currentResult  Result
)

func main() {
	initStart = time.Now()
	readFlags()
	readArgs()
	initConfig()
	validateConfig()
	connectToDatabase()
	readDatabaseSchema()
	readInputFile()
	verifyDatabase()
	compareDatabase()
	writeOutputFile()
	shutdown()
}

package main

import "mysql-verifier/src/lib"

var (
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
	currentResult  *Result
)

func main() {
	readArgs()
	readFlags()
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

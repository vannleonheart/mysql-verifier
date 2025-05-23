package main

import (
	"crypto/sha256"
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"mysql-verifier/src/lib"
	"os"
	"reflect"
	"strings"
	"time"
)

func readArgs() {
	args := os.Args[1:]

	if len(args) == 0 {
		return
	}

	switch args[0] {
	case "config-export", "ce":
		var outputDir *string
		if len(args) > 1 {
			outputDir = &args[1]
		}
		exportConfig(outputDir)
	case "schema-export", "se":
		if len(args) > 1 {
			configFilename = &args[1]
		}
		initConfig()
		validateConfig()
		connectToDatabase()
		readDatabaseSchemaFromDatabase()
		var outputDir *string
		if len(args) > 2 {
			outputDir = &args[2]
		}
		exportTableSchema(outputDir)

	}
}

func readFlags() {
	configFilename = flag.String("config", "", "Config file")
	inputFileName = flag.String("in", "", "Input file")
	outputFileName = flag.String("out", "", "Output file")
	schemaFileName = flag.String("schema", "", "Schema file")
	dbHost = flag.String("host", "", "Database host")
	dbPort = flag.String("port", "", "Database port")
	dbUser = flag.String("user", "", "Database user")
	dbName = flag.String("database", "", "Database name")

	flag.Parse()
}

func initConfig() {
	if configFilename != nil && len(*configFilename) > 0 {
		readConfigFile(*configFilename)
	}

	if inputFileName != nil && len(*inputFileName) > 0 {
		config.In = *inputFileName
	}

	if outputFileName != nil && len(*outputFileName) > 0 {
		config.Out = *outputFileName
	}

	if schemaFileName != nil && len(*schemaFileName) > 0 {
		config.Schema = *schemaFileName
	}

	if dbHost != nil && len(*dbHost) > 0 {
		config.Database.Host = *dbHost
	}

	if dbPort != nil && len(*dbPort) > 0 {
		config.Database.Port = *dbPort
	}

	if dbUser != nil && len(*dbUser) > 0 {
		config.Database.User = *dbUser
	}

	if dbName != nil && len(*dbName) > 0 {
		config.Database.Database = *dbName
	}
}

func validateConfig() {
	if len(config.Database.Host) == 0 {
		fmt.Print("Enter database host: ")
		_, err := fmt.Scanln(&config.Database.Host)
		if err != nil {
			fmt.Printf("error reading database host: %s\n", err.Error())
			validateConfig()
			return
		}
	} else {
		fmt.Printf("database host: %s\n", config.Database.Host)
	}

	if len(config.Database.Port) == 0 {
		fmt.Print("Enter database port: ")
		_, err := fmt.Scanln(&config.Database.Port)
		if err != nil {
			fmt.Printf("error reading database port: %s\n", err.Error())
			validateConfig()
			return
		}
	} else {
		fmt.Printf("database port: %s\n", config.Database.Port)
	}

	if len(config.Database.User) == 0 {
		fmt.Print("Enter database user: ")
		_, err := fmt.Scanln(&config.Database.User)
		if err != nil {
			fmt.Printf("error reading database user: %s\n", err.Error())
			validateConfig()
			return
		}
	} else {
		fmt.Printf("database user: %s\n", config.Database.User)
	}

	if len(config.Database.Password) == 0 {
		fmt.Print("Enter database password: ")
		_, err := fmt.Scanln(&config.Database.Password)
		if err != nil {
			fmt.Printf("error reading database password: %s\n", err.Error())
			validateConfig()
			return
		}
	} else {
		fmt.Printf("database password: %s\n", "***")
	}

	if len(config.Database.Database) == 0 {
		fmt.Print("Enter database name: ")
		_, err := fmt.Scanln(&config.Database.Database)
		if err != nil {
			fmt.Printf("error reading database name: %s\n", err.Error())
			validateConfig()
		}
	} else {
		fmt.Printf("database name: %s\n", config.Database.Database)
	}
}

func readConfigFile(configFilename string) {
	fmt.Printf("reading config file: %s\n", configFilename)

	fileInfo, err := os.Stat(configFilename)
	if err != nil {
		if os.IsNotExist(err) {
			fmt.Printf("file does not exist: %s\n", configFilename)
		} else {
			fmt.Printf("error accessing file: %s\n", err.Error())
		}
		return
	}

	if fileInfo.IsDir() {
		fmt.Printf("path is a directory, not a file: %s\n", configFilename)
		return
	}

	fileContent, err := os.ReadFile(configFilename)
	if err != nil {
		fmt.Printf("file is not readable: %s\n", err.Error())
		return
	}

	if err = json.Unmarshal(fileContent, &config); err != nil {
		fmt.Printf("error parsing config file: %s\n", err.Error())
		return
	}
}

func connectToDatabase() {
	if dbCon == nil {
		dbCon = lib.NewDatabaseClient(config.Database)
		if err := dbCon.Connect(); err != nil {
			fmt.Printf("error connecting to database: %s\n", err.Error())
			os.Exit(1)
		}
		fmt.Println("connected to database")
	}
}

func readDatabaseSchema() {
	if len(config.Schema) > 0 {
		fmt.Printf("reading schema file: %s\n", config.Schema)
		readDatabaseSchemaFromCSVFile()
	} else {
		fmt.Printf("reading database schema\n")
		readDatabaseSchemaFromDatabase()
	}
}

func readDatabaseSchemaFromDatabase() {
	query := "SELECT TABLE_NAME, TABLE_ROWS, ROUND((DATA_LENGTH + INDEX_LENGTH) / 1024 / 1024, 2) AS size_mb FROM information_schema.TABLES WHERE TABLE_SCHEMA = ? ORDER BY TABLE_ROWS ASC"

	rows, err := dbCon.Connection.Query(query, config.Database.Database)
	if err != nil {
		fmt.Printf("error querying database: %s\n", err.Error())
		return
	}
	defer func() {
		if err = rows.Close(); err != nil {
			fmt.Printf("error closing rows: %s\n", err.Error())
		}
	}()

	for rows.Next() {
		var t TableInfo
		if err = rows.Scan(
			&t.Name,
			&t.SchemaRows,
			&t.SizeInMB,
		); err != nil {
			fmt.Printf("error scanning row: %s\n", err.Error())
			return
		}

		readTableSchema(&t)

		tables = append(tables, t)
	}

	if err = rows.Err(); err != nil {
		fmt.Printf("error iterating rows: %s\n", err.Error())
		return
	}

	return
}

func readTableSchema(t *TableInfo) {
	rows, err := dbCon.Connection.Query("SELECT COLUMN_NAME FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?", dbCon.Config.Database, t.Name)
	if err != nil {
		fmt.Printf("error querying database: %s\n", err.Error())
		return
	}
	defer func() {
		if err = rows.Close(); err != nil {
			fmt.Printf("error closing rows: %s\n", err.Error())
		}
	}()
	hasIdColumn := false
	columnSize := 0
	for rows.Next() {
		var columnName string
		if err = rows.Scan(&columnName); err != nil {
			log.Fatal(err)
		}
		if columnName == "id" {
			hasIdColumn = true
		}
		columnSize++
	}
	t.ColumnSize = &columnSize
	t.HasIDColumn = hasIdColumn
}

func readDatabaseSchemaFromCSVFile() {
	file, err := os.Open(config.Schema)
	if err != nil {
		fmt.Printf("error opening file: %s\n", err.Error())
		return
	}
	defer func() {
		if err = file.Close(); err != nil {
			fmt.Printf("error closing file: %s\n", err.Error())
		}
	}()

	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	if err != nil {
		fmt.Printf("error reading file: %s\n", err.Error())
		return
	}
	for _, record := range records {
		tables = append(tables, TableInfo{
			Name:         record[0],
			SchemaRows:   0,
			SizeInMB:     0,
			ColumnSize:   nil,
			HasIDColumn:  false,
			CountRows:    0,
			MaxId:        nil,
			LastRow:      nil,
			StringToHash: "",
			Hash:         "",
			Duration:     "",
		})
	}

	return
}

func readInputFile() {
	if len(config.In) > 0 {
		fileInfo, err := os.Stat(config.In)
		if err != nil {
			if os.IsNotExist(err) {
				fmt.Printf("file does not exist: %s\n", configFilename)
			} else {
				fmt.Printf("error accessing file: %s\n", err.Error())
			}
			return
		}

		if fileInfo.IsDir() {
			fmt.Printf("path is a directory, not a file: %s\n", configFilename)
			return
		}

		fileContent, err := os.ReadFile(config.In)
		if err != nil {
			fmt.Printf("file is not readable: %s\n", err.Error())
			return
		}

		if err = json.Unmarshal(fileContent, &previousResult); err != nil {
			fmt.Printf("error parsing file: %s\n", err.Error())
			return
		}
	}
}

func verifyDatabase() {
	if len(tables) == 0 {
		return
	}

	result := Result{
		In:       config.In,
		Out:      config.Out,
		Schema:   config.Schema,
		Start:    0,
		End:      0,
		Duration: "",
		Tables:   map[string]TableInfo{},
		Status:   "",
	}

	for _, table := range tables {
		fmt.Println("-----------------------------------------")
		start := time.Now()
		fmt.Printf("verifying table: %s\n", table.Name)
		newTable := verifyTable(table)
		end := time.Now()
		duration := end.Sub(start)
		newTable.Duration = fmt.Sprintf("%s", duration)
		result.Tables[newTable.Name] = newTable
		fmt.Printf("duration: %s\n", end.Sub(start))
	}

	currentResult = result
}

func verifyTable(table TableInfo) TableInfo {
	query := fmt.Sprintf("SELECT COUNT(1) AS count FROM %s", table.Name)
	row := dbCon.Connection.QueryRow(query)
	var count int64
	if err := row.Scan(&count); err != nil {
		fmt.Printf("error scanning row: %s\n", err.Error())
		return table
	}
	fmt.Printf("rows: %d\n", count)
	table.CountRows = count

	if table.ColumnSize != nil && *table.ColumnSize > 0 && table.CountRows > 0 {
		if table.HasIDColumn {
			query = fmt.Sprintf("SELECT MAX(id) FROM %s", table.Name)
			row = dbCon.Connection.QueryRow(query)
			var maxId string
			if err := row.Scan(&maxId); err != nil {
				fmt.Printf("error scanning row: %s\n", err.Error())
			} else {
				fmt.Printf("max id: %s\n", maxId)
				table.MaxId = &maxId
			}
		}

		query = fmt.Sprintf("SELECT * FROM %s LIMIT 1", table.Name)
		if table.MaxId != nil {
			query = fmt.Sprintf("SELECT * FROM %s WHERE id = ? LIMIT 1", table.Name)
		}
		row = dbCon.Connection.QueryRow(query, table.MaxId)
		lastRow := make([]interface{}, *table.ColumnSize)
		for i := range lastRow {
			lastRow[i] = new(interface{})
		}
		if err := row.Scan(lastRow...); err != nil {
			fmt.Printf("error scanning row: %s\n", err.Error())
		} else {
			strRow := make([]interface{}, *table.ColumnSize)
			for i := range lastRow {
				lastRowCol := lastRow[i]
				if lastRowCol != nil {
					lastRowColValue := *(lastRowCol.(*interface{}))
					if lastRowColValue == nil {
						strRow[i] = ""
						continue
					}
					lastRowColValueType := reflect.TypeOf(lastRowColValue).Kind()
					if lastRowColValueType == reflect.Slice {
						lastRowColValue = strings.TrimSpace(string(lastRowColValue.([]uint8)))
					}
					strRow[i] = lastRowColValue
				} else {
					strRow[i] = ""
				}
			}
			strToSign := ""
			for i := range strRow {
				strToSign += fmt.Sprintf("%v", strRow[i])
			}
			fmt.Printf("last row: %v\n", strRow)
			hasher := sha256.New()
			hasher.Write([]byte(strToSign))
			hash := fmt.Sprintf("%x", hasher.Sum(nil))
			table.StringToHash = strToSign
			table.LastRow = &strRow
			table.Hash = hash
		}
	}

	tableMaxId := ""
	if table.MaxId != nil {
		tableMaxId = *table.MaxId
	}
	strToHash := fmt.Sprintf("%s%d%s%s", table.Name, table.CountRows, tableMaxId, table.Hash)
	hasher := sha256.New()
	hasher.Write([]byte(strToHash))
	table.Hash = fmt.Sprintf("%x", hasher.Sum(nil))

	return table
}

func compareDatabase() {
	if previousResult == nil {
		return
	}

	match := true

	for currentTable, currentTableInfo := range currentResult.Tables {
		previousTableInfo, ok := previousResult.Tables[currentTable]
		if !ok {
			continue
		}
		if currentTableInfo.Hash != previousTableInfo.Hash {
			match = false
		}
	}

	if match {
		currentResult.Status = "MATCH"
	} else {
		currentResult.Status = "NOT MATCH"
	}
}

func writeOutputFile() {
	now := time.Now()
	currentResult.Start = initStart.Unix()
	currentResult.End = now.Unix()
	duration := now.Sub(initStart)
	currentResult.Duration = fmt.Sprintf("%s", duration)

	if len(config.Out) > 0 {
		content, err := json.MarshalIndent(currentResult, "", "  ")
		if err != nil {
			fmt.Printf("error marshalling result: %s\n", err.Error())
			return
		}

		if err = os.WriteFile(config.Out, content, 0644); err != nil {
			fmt.Printf("error writing file: %s\n", err.Error())
			return
		}

		fmt.Printf("result written to file: %s\n", config.Out)
	}
}

func shutdown() {
	if dbCon != nil {
		if err := dbCon.Disconnect(); err != nil {
			fmt.Printf("error disconnecting from database: %s\n", err.Error())
		} else {
			dbCon = nil
			fmt.Println("disconnected from database")
		}
	}

	if previousResult != nil {
		previousResult = nil
	}

	if tables != nil {
		tables = nil
	}

	fmt.Println("shutdown complete")

	os.Exit(0)
}

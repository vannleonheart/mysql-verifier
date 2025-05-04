package main

import (
	"encoding/json"
	"fmt"
	"os"
)

func exportConfig(outputDir *string) {
	if outputDir == nil || len(*outputDir) == 0 {
		curDir, err := os.Getwd()
		if err != nil {
			fmt.Printf("error getting current working directory: %s\n", err.Error())
		}
		outputDir = &curDir
	}

	if outputDir != nil && len(*outputDir) > 0 {
		outputFile := fmt.Sprintf("%s/config.json", *outputDir)
		fileContent, err := json.MarshalIndent(config, "", "  ")

		if err != nil {
			fmt.Printf("error marshalling config: %s\n", err.Error())
		} else {
			if err = os.WriteFile(outputFile, fileContent, 0644); err != nil {
				fmt.Printf("error writing config file: %s\n", err.Error())
			}
		}
	}

	shutdown()
}

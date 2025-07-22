package main

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"text/template"
)

const tplPath = "../template/step.go.tpl"
const stepsDir = "../"

type TemplateData struct {
	ID         string
	StructName string
	Package    string
}

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Usage: generator <step_name>")
		os.Exit(1)
	}

	stepName := os.Args[1]
	id := strings.ToLower(stepName)
	structName := strings.Title(stepName)
	fileName := id + ".go"
	packageName := "steps"

	tplContent, err := os.ReadFile(tplPath)
	if err != nil {
		panic(err)
	}

	tpl := template.Must(template.New("step").Parse(string(tplContent)))
	var buf bytes.Buffer
	err = tpl.Execute(&buf, TemplateData{
		ID:         id,
		StructName: structName,
		Package:    packageName,
	})
	if err != nil {
		panic(err)
	}

	outPath := filepath.Join(stepsDir, fileName)
	if _, err := os.Stat(outPath); err == nil {
		fmt.Printf("File %s already exists\n", outPath)
		os.Exit(1)
	}

	if err := os.WriteFile(outPath, buf.Bytes(), 0644); err != nil {
		panic(err)
	}

	fmt.Printf("Step %s created at %s\n", structName, outPath)
}

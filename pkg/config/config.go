package config

import (
	"bytes"
	"encoding/json"
	"io"
	"os"

	"gopkg.in/yaml.v3"
)

// Config représente une configuration de transformation (règles, options, etc.).
type Config struct {
	Tables []TableConfig `json:"tables" yaml:"tables"`
}

type TableConfig struct {
	Name         string              `json:"name" yaml:"name"`
	Transformers []TransformerConfig `json:"transformers" yaml:"transformers"`
}

type TransformerConfig struct {
	Name string `json:"name" yaml:"name"`
	Type string `json:"type" yaml:"type"`
	// Options spécifiques à chaque type de transformation
	Options map[string]any `json:"options,omitempty" yaml:"options,omitempty"`
}

type TransformerGeneratorConfig struct {
	ColumnName  string   `json:"column_name" yaml:"column_name"`
	DataType    DataType `json:"data_type" yaml:"data_type"`
	FormatRegex string   `json:"format,omitempty" yaml:"format,omitempty"`
}

type TransformerShufflerConfig struct {
	ColumnNames []string `json:"column_names" yaml:"column_names"`
}

type TransformerMaskerConfig struct {
	ColumnName string `json:"column_name" yaml:"column_name"`
	MaskChar   string `json:"mask_char,omitempty" yaml:"mask_char,omitempty"`
}

type DataType string

const (
	String  DataType = "string"
	Integer DataType = "integer"
	Float   DataType = "float"
	Boolean DataType = "boolean"
)

// Load charge une configuration depuis un flux en JSON ou YAML.
func Load(r io.Reader) (*Config, error) {
	raw, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}

	var config Config
	jsondecoder := json.NewDecoder(bytes.NewReader(raw))
	jsondecoder.UseNumber()
	if err := jsondecoder.Decode(&config); err == nil {
		return &config, nil
	}

	yamldecoder := yaml.NewDecoder(bytes.NewReader(raw))
	if err := yamldecoder.Decode(&config); err != nil {
		return nil, err
	}

	return &config, nil
}

// LoadFromFile charge une configuration depuis un fichier.
func LoadFromFile(path string) (*Config, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	return Load(file)
}

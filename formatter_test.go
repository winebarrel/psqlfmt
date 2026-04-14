package psqlfmt_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/winebarrel/psqlfmt"
	"gopkg.in/yaml.v3"
)

type testCase struct {
	Name     string `yaml:"name"`
	Input    string `yaml:"input"`
	Expected string `yaml:"expected"`
}

func TestFormat(t *testing.T) {
	files, err := filepath.Glob("testdata/*.yml")
	require.NoError(t, err)
	require.NotEmpty(t, files)

	for _, file := range files {
		data, err := os.ReadFile(file)
		require.NoError(t, err)

		var tests []testCase
		require.NoError(t, yaml.Unmarshal(data, &tests))

		base := filepath.Base(file)

		for _, tt := range tests {
			t.Run(base+"/"+tt.Name, func(t *testing.T) {
				result, err := psqlfmt.Format(tt.Input)
				require.NoError(t, err)
				assert.Equal(t, tt.Expected, result)
			})
		}
	}
}

package lsm

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestOpenTableWithoutBuilder(t *testing.T) {
	// openTable(nil, "tablePath", nil)
	lm := newLevelManager(opt)
	tablePath := "../work_test/1.sst"
	table, err := openTable(lm, tablePath, nil)
	require.NoError(t, err)
	require.NotNil(t, table)
}

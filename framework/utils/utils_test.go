package utils_test

import (
	"encoder-service/framework/utils"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIsJson(t *testing.T) {
	json := `{
		"id": "555dhv2wyg873ge",
		"name": "JK"
	}`
	err := utils.IsJson(json)

	require.Nil(t,err)

	json = "naojson"
	err = utils.IsJson(json)

	require.NotNil(t,err)
}
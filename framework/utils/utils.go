package utils

import "encoding/json"

//verificar se é um json valido
func IsJson(input string) error {
	var js struct{}

	if err := json.Unmarshal([]byte(input), &js); err != nil {
		return err
	}

	return nil
}
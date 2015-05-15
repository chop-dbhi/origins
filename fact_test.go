package origins

import "testing"

func TestParseIdent(t *testing.T) {
	tests := map[string]bool{
		"person/1":      true,
		"person/one":    true,
		"person/one_":   true,
		"person/ONE":    true,
		"db.person/1":   true,
		"person/one-":   false,
		"person":        false,
		"db:one/person": false,
	}

	var err error

	for test, expected := range tests {
		if _, err = ParseIdent(test); err != nil && expected {
			t.Errorf("ident: expectation failed for `%s`", test)
		}
	}
}

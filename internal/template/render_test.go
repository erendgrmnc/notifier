package template

import (
	"strings"
	"testing"
)

func TestRenderSubstitutesVariables(t *testing.T) {
	rendered, err := Render("Hi {{.name}}, your code is {{.code}}.", map[string]string{
		"name": "Eren", "code": "421337",
	})
	if err != nil {
		t.Fatalf("Render: %v", err)
	}
	if rendered != "Hi Eren, your code is 421337." {
		t.Errorf("rendered = %q", rendered)
	}
}

func TestRenderMissingVariableFails(t *testing.T) {
	_, err := Render("Hi {{.name}}!", map[string]string{})
	if err == nil {
		t.Fatal("Render with missing variable succeeded; must fail at enqueue")
	}
	if !strings.Contains(err.Error(), "name") {
		t.Errorf("error does not name the missing variable: %v", err)
	}
}

func TestValidateRejectsBrokenBody(t *testing.T) {
	if err := Validate("Hello {{.name"); err == nil {
		t.Error("Validate accepted an unparseable body")
	}
	if err := Validate("Hello {{.name}}"); err != nil {
		t.Errorf("Validate rejected a good body: %v", err)
	}
}

// Package template renders notification templates with strict variable
// checking: unknown placeholders fail at enqueue time, not delivery time.
package template

import (
	"fmt"
	"strings"
	texttemplate "text/template"
)

// Validate reports whether a template body parses.
func Validate(body string) error {
	if _, err := parse(body); err != nil {
		return fmt.Errorf("template does not parse: %w", err)
	}
	return nil
}

// Render substitutes vars into the body. Referencing a variable the
// caller did not supply is an error (missingkey=error), so a typo can
// never silently deliver "<no value>" to a recipient.
func Render(body string, vars map[string]string) (string, error) {
	parsed, err := parse(body)
	if err != nil {
		return "", fmt.Errorf("parse template: %w", err)
	}

	var rendered strings.Builder
	if err := parsed.Execute(&rendered, vars); err != nil {
		return "", fmt.Errorf("render template: %w", err)
	}
	return rendered.String(), nil
}

func parse(body string) (*texttemplate.Template, error) {
	return texttemplate.New("notification").Option("missingkey=error").Parse(body)
}

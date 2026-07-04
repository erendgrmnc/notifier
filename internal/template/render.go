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

// Parsed is a compiled template body, reusable across renders — batch
// creation parses each referenced template once, not once per item.
type Parsed struct {
	tmpl *texttemplate.Template
}

// Parse compiles a template body.
func Parse(body string) (Parsed, error) {
	tmpl, err := parse(body)
	if err != nil {
		return Parsed{}, fmt.Errorf("parse template: %w", err)
	}
	return Parsed{tmpl: tmpl}, nil
}

// Render substitutes vars into the compiled body. Referencing a variable
// the caller did not supply is an error (missingkey=error), so a typo
// can never silently deliver "<no value>" to a recipient.
func (parsed Parsed) Render(vars map[string]string) (string, error) {
	var rendered strings.Builder
	if err := parsed.tmpl.Execute(&rendered, vars); err != nil {
		return "", fmt.Errorf("render template: %w", err)
	}
	return rendered.String(), nil
}

// Render is the one-shot convenience: parse then render.
func Render(body string, vars map[string]string) (string, error) {
	parsed, err := Parse(body)
	if err != nil {
		return "", err
	}
	return parsed.Render(vars)
}

func parse(body string) (*texttemplate.Template, error) {
	return texttemplate.New("notification").Option("missingkey=error").Parse(body)
}

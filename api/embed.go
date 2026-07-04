// Package apispec holds the OpenAPI contract. The YAML file is the
// single source of truth, embedded here so the running service can
// serve its own documentation.
package apispec

import _ "embed"

//go:embed openapi.yaml
var OpenAPIYAML []byte

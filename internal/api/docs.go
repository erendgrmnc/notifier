package api

import (
	"net/http"

	apispec "notifier/api"
)

// swaggerUIPage renders the embedded OpenAPI spec with Swagger UI. The
// assets load from a CDN, so /docs needs internet access; the raw spec
// at /api/v1/openapi.yaml works offline.
const swaggerUIPage = `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <title>Notification Service API — Docs</title>
  <link rel="stylesheet" href="https://unpkg.com/swagger-ui-dist@5/swagger-ui.css"/>
</head>
<body>
  <div id="swagger-ui"></div>
  <script src="https://unpkg.com/swagger-ui-dist@5/swagger-ui-bundle.js"></script>
  <script>
    SwaggerUIBundle({
      url: "/api/v1/openapi.yaml",
      dom_id: "#swagger-ui",
      tryItOutEnabled: true
    });
  </script>
</body>
</html>`

func handleOpenAPISpec(writer http.ResponseWriter, _ *http.Request) {
	writer.Header().Set("Content-Type", "application/yaml")
	writer.WriteHeader(http.StatusOK)
	_, _ = writer.Write(apispec.OpenAPIYAML)
}

func handleSwaggerUI(writer http.ResponseWriter, _ *http.Request) {
	writer.Header().Set("Content-Type", "text/html; charset=utf-8")
	writer.WriteHeader(http.StatusOK)
	_, _ = writer.Write([]byte(swaggerUIPage))
}

package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"notifier/internal/domain"
)

// TemplateRepository persists notification templates.
type TemplateRepository struct {
	pool *pgxpool.Pool
}

func NewTemplateRepository(pool *pgxpool.Pool) *TemplateRepository {
	return &TemplateRepository{pool: pool}
}

const templateColumns = `id, name, channel, body, created_at, updated_at`

// CreateTemplate inserts a template; a taken name surfaces as
// domain.ErrDuplicateTemplateName.
func (repo *TemplateRepository) CreateTemplate(ctx context.Context, template domain.Template) error {
	const insertTemplate = `
		INSERT INTO notification_templates (id, name, channel, body, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6)`

	_, err := repo.pool.Exec(ctx, insertTemplate,
		template.ID, template.Name, template.Channel, template.Body,
		template.CreatedAt, template.UpdatedAt,
	)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == uniqueViolationCode {
			return domain.ErrDuplicateTemplateName
		}
		return fmt.Errorf("insert template: %w", err)
	}
	return nil
}

// GetTemplateByName loads one template or domain.ErrNotFound.
func (repo *TemplateRepository) GetTemplateByName(ctx context.Context, name string) (domain.Template, error) {
	query := `SELECT ` + templateColumns + ` FROM notification_templates WHERE name = $1`

	var template domain.Template
	err := repo.pool.QueryRow(ctx, query, name).Scan(
		&template.ID, &template.Name, &template.Channel, &template.Body,
		&template.CreatedAt, &template.UpdatedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return domain.Template{}, domain.ErrNotFound
		}
		return domain.Template{}, fmt.Errorf("select template %s: %w", name, err)
	}
	return template, nil
}

// ListTemplates returns every template, newest first.
func (repo *TemplateRepository) ListTemplates(ctx context.Context) ([]domain.Template, error) {
	query := `SELECT ` + templateColumns + ` FROM notification_templates ORDER BY created_at DESC`

	rows, err := repo.pool.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("list templates: %w", err)
	}
	defer rows.Close()

	var templates []domain.Template
	for rows.Next() {
		var template domain.Template
		if err := rows.Scan(
			&template.ID, &template.Name, &template.Channel, &template.Body,
			&template.CreatedAt, &template.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan template: %w", err)
		}
		templates = append(templates, template)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list templates: %w", err)
	}
	return templates, nil
}

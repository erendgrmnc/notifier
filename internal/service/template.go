package service

import (
	"context"
	"fmt"

	"github.com/google/uuid"

	"notifier/internal/domain"
	"notifier/internal/template"
)

// TemplateRepository is what the template service needs from persistence.
type TemplateRepository interface {
	CreateTemplate(ctx context.Context, template domain.Template) error
	GetTemplateByName(ctx context.Context, name string) (domain.Template, error)
	ListTemplates(ctx context.Context) ([]domain.Template, error)
}

// TemplateService implements template management.
type TemplateService struct {
	repo  TemplateRepository
	clock Clock
}

func NewTemplateService(repo TemplateRepository, clock Clock) *TemplateService {
	return &TemplateService{repo: repo, clock: clock}
}

// CreateTemplateInput is the validated-shape create request.
type CreateTemplateInput struct {
	Name    string
	Channel domain.Channel
	Body    string
}

// Create validates shape and parseability, then persists.
func (svc *TemplateService) Create(ctx context.Context, input CreateTemplateInput) (domain.Template, error) {
	now := svc.clock.Now()
	newTemplate := domain.Template{
		ID:        uuid.New(),
		Name:      input.Name,
		Channel:   input.Channel,
		Body:      input.Body,
		CreatedAt: now,
		UpdatedAt: now,
	}

	if err := domain.ValidateNewTemplate(newTemplate); err != nil {
		return domain.Template{}, err
	}
	if err := template.Validate(newTemplate.Body); err != nil {
		return domain.Template{}, domain.ValidationErrors{{Field: "body", Message: err.Error()}}
	}

	if err := svc.repo.CreateTemplate(ctx, newTemplate); err != nil {
		return domain.Template{}, err
	}
	return newTemplate, nil
}

// GetByName returns one template.
func (svc *TemplateService) GetByName(ctx context.Context, name string) (domain.Template, error) {
	found, err := svc.repo.GetTemplateByName(ctx, name)
	if err != nil {
		return domain.Template{}, fmt.Errorf("get template: %w", err)
	}
	return found, nil
}

// List returns all templates.
func (svc *TemplateService) List(ctx context.Context) ([]domain.Template, error) {
	templates, err := svc.repo.ListTemplates(ctx)
	if err != nil {
		return nil, fmt.Errorf("list templates: %w", err)
	}
	return templates, nil
}

// templateResolver renders template references, caching the fetched
// template and its compiled form so a 1000-item batch referencing one
// template costs one SELECT and one parse, not a thousand.
type templateResolver struct {
	repo      TemplateRepository
	templates map[string]domain.Template
	compiled  map[string]template.Parsed
}

func newTemplateResolver(repo TemplateRepository) *templateResolver {
	return &templateResolver{
		repo:      repo,
		templates: map[string]domain.Template{},
		compiled:  map[string]template.Parsed{},
	}
}

// render resolves a template reference into final content. Every failure
// is a client error: unknown template, channel mismatch, or missing
// variables.
func (resolver *templateResolver) render(ctx context.Context, ref TemplateRef, channel domain.Channel) (string, error) {
	stored, cached := resolver.templates[ref.Name]
	if !cached {
		loaded, err := resolver.repo.GetTemplateByName(ctx, ref.Name)
		if err != nil {
			return "", domain.ValidationErrors{{Field: "template.name", Message: fmt.Sprintf("template %q not found", ref.Name)}}
		}
		resolver.templates[ref.Name] = loaded
		stored = loaded
	}
	if stored.Channel != channel {
		return "", domain.ValidationErrors{{
			Field:   "template.name",
			Message: fmt.Sprintf("template %q is for channel %s, not %s", ref.Name, stored.Channel, channel),
		}}
	}

	compiled, cached := resolver.compiled[ref.Name]
	if !cached {
		parsed, err := template.Parse(stored.Body)
		if err != nil {
			return "", domain.ValidationErrors{{Field: "template.name", Message: err.Error()}}
		}
		resolver.compiled[ref.Name] = parsed
		compiled = parsed
	}

	rendered, err := compiled.Render(ref.Vars)
	if err != nil {
		return "", domain.ValidationErrors{{Field: "template.vars", Message: err.Error()}}
	}
	return rendered, nil
}

// TemplateRef points a notification at a stored template.
type TemplateRef struct {
	Name string
	Vars map[string]string
}

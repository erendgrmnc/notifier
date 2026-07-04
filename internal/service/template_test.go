package service

import (
	"context"
	"errors"
	"testing"

	"notifier/internal/domain"
)

type fakeTemplateRepository struct {
	stored map[string]domain.Template
}

func (repo *fakeTemplateRepository) CreateTemplate(_ context.Context, template domain.Template) error {
	if repo.stored == nil {
		repo.stored = map[string]domain.Template{}
	}
	if _, exists := repo.stored[template.Name]; exists {
		return domain.ErrDuplicateTemplateName
	}
	repo.stored[template.Name] = template
	return nil
}

func (repo *fakeTemplateRepository) GetTemplateByName(_ context.Context, name string) (domain.Template, error) {
	template, ok := repo.stored[name]
	if !ok {
		return domain.Template{}, domain.ErrNotFound
	}
	return template, nil
}

func (repo *fakeTemplateRepository) ListTemplates(_ context.Context) ([]domain.Template, error) {
	var templates []domain.Template
	for _, template := range repo.stored {
		templates = append(templates, template)
	}
	return templates, nil
}

func newTestTemplateService(repo *fakeTemplateRepository) *TemplateService {
	return NewTemplateService(repo, fixedClock{now: testNow})
}

func TestTemplateCreateValidatesAndPersists(t *testing.T) {
	repo := &fakeTemplateRepository{}
	svc := newTestTemplateService(repo)

	created, err := svc.Create(context.Background(), CreateTemplateInput{
		Name: "otp-code", Channel: domain.ChannelSMS, Body: "Your code: {{.code}}",
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if created.Name != "otp-code" || repo.stored["otp-code"].Body == "" {
		t.Error("template not persisted")
	}
}

func TestTemplateCreateRejectsBadInput(t *testing.T) {
	svc := newTestTemplateService(&fakeTemplateRepository{})

	testCases := []struct {
		name  string
		input CreateTemplateInput
		field string
	}{
		{name: "bad name", input: CreateTemplateInput{Name: "Bad Name!", Channel: domain.ChannelSMS, Body: "x"}, field: "name"},
		{name: "bad channel", input: CreateTemplateInput{Name: "ok-name", Channel: "fax", Body: "x"}, field: "channel"},
		{name: "empty body", input: CreateTemplateInput{Name: "ok-name", Channel: domain.ChannelSMS}, field: "body"},
		{name: "unparseable body", input: CreateTemplateInput{Name: "ok-name", Channel: domain.ChannelSMS, Body: "{{.oops"}, field: "body"},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := svc.Create(context.Background(), tc.input)
			var validationErrs domain.ValidationErrors
			if !errors.As(err, &validationErrs) {
				t.Fatalf("error = %v, want ValidationErrors", err)
			}
			if validationErrs[0].Field != tc.field {
				t.Errorf("failing field = %s, want %s", validationErrs[0].Field, tc.field)
			}
		})
	}
}

func TestCreateNotificationFromTemplate(t *testing.T) {
	repo := newFakeRepository()
	templateRepo := &fakeTemplateRepository{stored: map[string]domain.Template{
		"welcome": {Name: "welcome", Channel: domain.ChannelSMS, Body: "Welcome {{.name}}!"},
	}}
	publisher := &fakePublisher{}
	svc := NewNotificationService(repo, &fakeBatchRepository{fakeRepository: repo}, templateRepo,
		publisher, fixedClock{now: testNow}, testLogger(), nil)

	result, err := svc.Create(context.Background(), CreateInput{
		Recipient: "+905551234567",
		Channel:   domain.ChannelSMS,
		Template:  &TemplateRef{Name: "welcome", Vars: map[string]string{"name": "Eren"}},
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if result.Notification.Content != "Welcome Eren!" {
		t.Errorf("content = %q, want rendered template", result.Notification.Content)
	}
}

func TestCreateNotificationTemplateErrors(t *testing.T) {
	repo := newFakeRepository()
	templateRepo := &fakeTemplateRepository{stored: map[string]domain.Template{
		"welcome": {Name: "welcome", Channel: domain.ChannelEmail, Body: "Welcome {{.name}}!"},
	}}
	svc := NewNotificationService(repo, &fakeBatchRepository{fakeRepository: repo}, templateRepo,
		&fakePublisher{}, fixedClock{now: testNow}, testLogger(), nil)

	testCases := []struct {
		name  string
		input CreateInput
	}{
		{name: "unknown template", input: CreateInput{
			Recipient: "+905551234567", Channel: domain.ChannelSMS,
			Template: &TemplateRef{Name: "nope"},
		}},
		{name: "channel mismatch", input: CreateInput{
			Recipient: "+905551234567", Channel: domain.ChannelSMS,
			Template: &TemplateRef{Name: "welcome", Vars: map[string]string{"name": "x"}},
		}},
		{name: "missing variable", input: CreateInput{
			Recipient: "user@example.com", Channel: domain.ChannelEmail,
			Template: &TemplateRef{Name: "welcome"},
		}},
		{name: "both content and template", input: CreateInput{
			Recipient: "+905551234567", Channel: domain.ChannelSMS, Content: "hi",
			Template: &TemplateRef{Name: "welcome"},
		}},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := svc.Create(context.Background(), tc.input)
			var validationErrs domain.ValidationErrors
			if !errors.As(err, &validationErrs) {
				t.Errorf("error = %v, want ValidationErrors", err)
			}
			if len(repo.created) != 0 {
				t.Error("invalid template input persisted a notification")
			}
		})
	}
}

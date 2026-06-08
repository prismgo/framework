package make

// Artifact identifies a supported make:* generator target.
type Artifact string

const (
	// CommandArtifact generates app/cmd console commands.
	CommandArtifact Artifact = "command"
	// ControllerArtifact generates app/http/controllers HTTP controllers.
	ControllerArtifact Artifact = "controller"
	// EventArtifact generates app/events event payloads.
	EventArtifact Artifact = "event"
	// JobArtifact generates app/jobs queued jobs.
	JobArtifact Artifact = "job"
	// ListenerArtifact generates app/listeners event listeners.
	ListenerArtifact Artifact = "listener"
	// MiddlewareArtifact generates app/http/middleware Gin middleware.
	MiddlewareArtifact Artifact = "middleware"
	// MigrationArtifact generates database/migrations schema migrations.
	MigrationArtifact Artifact = "migration"
	// ModelArtifact generates app/models GORM models.
	ModelArtifact Artifact = "model"
	// ProviderArtifact generates app/providers service providers.
	ProviderArtifact Artifact = "provider"
	// ResourceArtifact generates app/http/resources API transformers.
	ResourceArtifact Artifact = "resource"
	// SeederArtifact generates database/seeders seeders.
	SeederArtifact Artifact = "seeder"
)

type artifactSpec struct {
	Artifact    Artifact
	CommandName string
	Description string
	Directory   string
	Suffix      string
	Stub        string
	Package     string
	Hint        string
}

var artifactSpecs = map[Artifact]artifactSpec{
	CommandArtifact: {
		Artifact:    CommandArtifact,
		CommandName: "make:command",
		Description: "Create a new console command",
		Directory:   "app/cmd",
		Suffix:      "Command",
		Stub:        "command.stub",
		Package:     "cmd",
	},
	ControllerArtifact: {
		Artifact:    ControllerArtifact,
		CommandName: "make:controller",
		Description: "Create a new HTTP controller",
		Directory:   "app/http/controllers",
		Suffix:      "Controller",
		Stub:        "controller.stub",
		Package:     "controllers",
	},
	EventArtifact: {
		Artifact:    EventArtifact,
		CommandName: "make:event",
		Description: "Create a new event",
		Directory:   "app/events",
		Stub:        "event.stub",
		Package:     "events",
	},
	JobArtifact: {
		Artifact:    JobArtifact,
		CommandName: "make:job",
		Description: "Create a new queued job",
		Directory:   "app/jobs",
		Stub:        "job.stub",
		Package:     "jobs",
	},
	ListenerArtifact: {
		Artifact:    ListenerArtifact,
		CommandName: "make:listener",
		Description: "Create a new event listener",
		Directory:   "app/listeners",
		Stub:        "listener.stub",
		Package:     "listeners",
	},
	MiddlewareArtifact: {
		Artifact:    MiddlewareArtifact,
		CommandName: "make:middleware",
		Description: "Create a new HTTP middleware",
		Directory:   "app/http/middleware",
		Stub:        "middleware.stub",
		Package:     "middleware",
		Hint:        "Register this middleware in routes or HTTP middleware setup when needed.",
	},
	MigrationArtifact: {
		Artifact:    MigrationArtifact,
		CommandName: "make:migration",
		Description: "Create a new database migration",
		Directory:   "database/migrations",
		Stub:        "migration.stub",
		Package:     "migrations",
	},
	ModelArtifact: {
		Artifact:    ModelArtifact,
		CommandName: "make:model",
		Description: "Create a new GORM model",
		Directory:   "app/models",
		Stub:        "model.stub",
		Package:     "models",
	},
	ProviderArtifact: {
		Artifact:    ProviderArtifact,
		CommandName: "make:provider",
		Description: "Create a new service provider",
		Directory:   "app/providers",
		Stub:        "provider.stub",
		Package:     "providers",
		Hint:        "Register this provider in bootstrap/provider.go when needed.",
	},
	ResourceArtifact: {
		Artifact:    ResourceArtifact,
		CommandName: "make:resource",
		Description: "Create a new API resource transformer",
		Directory:   "app/http/resources",
		Stub:        "resource.stub",
		Package:     "resources",
	},
	SeederArtifact: {
		Artifact:    SeederArtifact,
		CommandName: "make:seeder",
		Description: "Create a new database seeder",
		Directory:   "database/seeders",
		Stub:        "seeder.stub",
		Package:     "seeders",
		Hint:        "Call or import this seeder from your database seeding flow when needed.",
	},
}

func allArtifacts() []Artifact {
	return []Artifact{
		CommandArtifact,
		ControllerArtifact,
		EventArtifact,
		JobArtifact,
		ListenerArtifact,
		MiddlewareArtifact,
		MigrationArtifact,
		ModelArtifact,
		ProviderArtifact,
		ResourceArtifact,
		SeederArtifact,
	}
}

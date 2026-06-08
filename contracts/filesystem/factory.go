package filesystem

type Factory interface {
	Disk(name string) Repository
}

type Manager interface {
	Factory
	Default() Repository
	Cloud() Cloud
	DefaultName() string
	CloudName() string
	Close() error
}

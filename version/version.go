package version

// Name is the public framework name shown in CLI metadata.
const Name = "PrismGo"

// Framework is the PrismGo framework version.
const Framework = "0.1.0"

// Banner returns the CLI version banner.
func Banner() string {
	return Name + " Framework " + Framework
}

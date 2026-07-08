package migration

type fakeInput struct {
	args    map[string][]string
	options map[string]string
	bools   map[string]bool
}

func (i fakeInput) Argument(name string) string {
	values := i.args[name]
	if len(values) == 0 {
		return ""
	}
	return values[0]
}

func (i fakeInput) Arguments(name string) []string { return append([]string(nil), i.args[name]...) }
func (i fakeInput) Option(name string) string      { return i.options[name] }
func (i fakeInput) OptionStrings(name string) []string {
	value := i.options[name]
	if value == "" {
		return nil
	}
	return []string{value}
}
func (i fakeInput) OptionBool(name string) bool { return i.bools[name] }
func (i fakeInput) OptionInt(name string) (int, error) {
	return parsePositiveInt(i.options[name]), nil
}
func (i fakeInput) HasOption(name string) bool {
	_, ok := i.options[name]
	if ok {
		return true
	}
	return i.bools[name]
}

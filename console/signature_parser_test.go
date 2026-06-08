package console

import "testing"

func TestParseSignatureWithArgumentsAndOptions(t *testing.T) {
	definition, err := ParseSignature("report:send {tenant} {user?} {tags?*} {--Q|queue=} {--force} {--tag=*}")
	if err != nil {
		t.Fatalf("ParseSignature returned error: %v", err)
	}

	if definition.Name != "report:send" {
		t.Fatalf("definition.Name = %q, want report:send", definition.Name)
	}
	if len(definition.Arguments) != 3 {
		t.Fatalf("len(definition.Arguments) = %d, want 3", len(definition.Arguments))
	}
	if !definition.Arguments[0].Required || definition.Arguments[0].Name != "tenant" {
		t.Fatalf("unexpected first argument: %+v", definition.Arguments[0])
	}
	if definition.Arguments[1].Required || definition.Arguments[1].Name != "user" {
		t.Fatalf("unexpected second argument: %+v", definition.Arguments[1])
	}
	if !definition.Arguments[2].IsArray || definition.Arguments[2].Name != "tags" {
		t.Fatalf("unexpected third argument: %+v", definition.Arguments[2])
	}
	if len(definition.Options) != 3 {
		t.Fatalf("len(definition.Options) = %d, want 3", len(definition.Options))
	}
	if definition.Options[0].Name != "queue" || definition.Options[0].Shortcut != "Q" || definition.Options[0].ValueMode != OptionValueOptional {
		t.Fatalf("unexpected first option: %+v", definition.Options[0])
	}
	if definition.Options[1].Name != "force" || definition.Options[1].ValueMode != OptionValueNone {
		t.Fatalf("unexpected second option: %+v", definition.Options[1])
	}
	if definition.Options[2].Name != "tag" || !definition.Options[2].IsArray || definition.Options[2].ValueMode != OptionValueOptional {
		t.Fatalf("unexpected third option: %+v", definition.Options[2])
	}
}

func TestParseSignatureSplitsDescriptionsWithFlexibleWhitespace(t *testing.T) {
	// 需求背景：Laravel 允许冒号两侧有多个空白字符；这里验证参数和选项
	// 都能保留描述文本，避免 help 输出丢失 input description。
	definition, err := ParseSignature("report:send {user   :   The ID of the user} {--queue   :   Whether the job should be queued}")
	if err != nil {
		t.Fatalf("ParseSignature returned error: %v", err)
	}

	if definition.Arguments[0].Name != "user" || definition.Arguments[0].Description != "The ID of the user" {
		t.Fatalf("unexpected argument description: %+v", definition.Arguments[0])
	}
	if definition.Options[0].Name != "queue" || definition.Options[0].Description != "Whether the job should be queued" {
		t.Fatalf("unexpected option description: %+v", definition.Options[0])
	}
}

func TestParseSignatureRejectsBrokenSignature(t *testing.T) {
	if _, err := ParseSignature("broken {tenant"); err == nil {
		t.Fatal("expected ParseSignature to reject unclosed brace")
	}
}

func TestParseSignatureParsesOptionalValueOptions(t *testing.T) {
	definition, err := ParseSignature("sample:run {--queue=} {--queue-default=redis} {--tag=*}")
	if err != nil {
		t.Fatalf("ParseSignature returned error: %v", err)
	}
	if definition.Options[0].ValueMode != OptionValueOptional {
		t.Fatalf("queue value mode = %v, want optional", definition.Options[0].ValueMode)
	}
	if definition.Options[1].ValueMode != OptionValueOptional || definition.Options[1].DefaultValue == nil || *definition.Options[1].DefaultValue != "redis" {
		t.Fatalf("queue-default option = %+v, want optional with default", definition.Options[1])
	}
	if definition.Options[2].ValueMode != OptionValueOptional || !definition.Options[2].IsArray {
		t.Fatalf("tag option = %+v, want optional array", definition.Options[2])
	}
}

func TestMustDefinitionAppliesDescription(t *testing.T) {
	definition := MustDefinition("sample:run {tenant}", "sample command")
	if definition.Name != "sample:run" {
		t.Fatalf("definition.Name = %q, want sample:run", definition.Name)
	}
	if definition.Description != "sample command" {
		t.Fatalf("definition.Description = %q, want sample command", definition.Description)
	}
}

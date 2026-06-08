package console

import (
	"fmt"
	"strings"
	"unicode"
)

// ParseSignature 解析 Laravel 风格 command signature DSL。
//
// 用途：支持把 `report:send {tenant} {--Q|queue=} {--force}` 这类声明转换成结构化 Definition，
// 供 kernel 注册、参数校验、help 展示与运行时输入读取复用。
// 设计原因：当前命令只支持 `Signature() string`，通过兼容式 DSL 解析可以在不破坏旧命令的前提下
// 增加 Laravel 风格 arguments/options 能力。
func ParseSignature(signature string) (Definition, error) {
	signature = strings.TrimSpace(signature)
	if signature == "" {
		return Definition{}, fmt.Errorf("console signature: empty signature")
	}

	name, tokens, err := splitSignature(signature)
	if err != nil {
		return Definition{}, err
	}
	def := Definition{Name: name}
	for _, token := range tokens {
		if err := parseSignatureToken(token, &def); err != nil {
			return Definition{}, err
		}
	}
	return NormalizeDefinition(def)
}

func splitSignature(signature string) (string, []string, error) {
	brace := strings.IndexRune(signature, '{')
	if brace == -1 {
		return strings.TrimSpace(signature), nil, nil
	}

	name := strings.TrimSpace(signature[:brace])
	if name == "" {
		return "", nil, fmt.Errorf("console signature: command name is required")
	}

	var (
		tokens []string
		depth  int
		start  = -1
	)
	for i, r := range signature[brace:] {
		index := brace + i
		switch r {
		case '{':
			if depth == 0 {
				start = index + 1
			}
			depth++
		case '}':
			depth--
			if depth < 0 {
				return "", nil, fmt.Errorf("console signature: unexpected closing brace")
			}
			if depth == 0 {
				tokens = append(tokens, signature[start:index])
				start = -1
			}
		}
	}
	if depth != 0 {
		return "", nil, fmt.Errorf("console signature: unclosed brace")
	}
	return name, tokens, nil
}

func parseSignatureToken(token string, def *Definition) error {
	token = strings.TrimSpace(token)
	if token == "" {
		return fmt.Errorf("console signature: empty token")
	}

	body, description := splitTokenDescription(token)
	if strings.HasPrefix(body, "--") {
		opt, err := parseOptionToken(body)
		if err != nil {
			return err
		}
		opt.Description = description
		def.Options = append(def.Options, opt)
		return nil
	}

	arg, err := parseArgumentToken(body)
	if err != nil {
		return err
	}
	arg.Description = description
	def.Arguments = append(def.Arguments, arg)
	return nil
}

func splitTokenDescription(token string) (string, string) {
	runes := []rune(token)
	for i, r := range runes {
		if r != ':' || i == 0 || i == len(runes)-1 {
			continue
		}
		// 逻辑说明：Laravel signature parser 只把两侧存在空白的冒号视为
		// 描述分隔符，避免把 URL、时间或普通参数值中的冒号误切开。
		if unicode.IsSpace(runes[i-1]) && unicode.IsSpace(runes[i+1]) {
			return strings.TrimSpace(string(runes[:i])), strings.TrimSpace(string(runes[i+1:]))
		}
	}
	return strings.TrimSpace(token), ""
}

func parseArgumentToken(body string) (Argument, error) {
	arg := Argument{Required: true}

	switch {
	case strings.HasSuffix(body, "?*"):
		arg.IsArray = true
		arg.Required = false
		body = strings.TrimSuffix(body, "?*")
	case strings.HasSuffix(body, "*"):
		arg.IsArray = true
		body = strings.TrimSuffix(body, "*")
	case strings.HasSuffix(body, "?"):
		arg.Required = false
		body = strings.TrimSuffix(body, "?")
	}

	if name, defaultValue, hasDefault := strings.Cut(body, "="); hasDefault {
		body = name
		arg.Required = false
		arg.DefaultValue = stringPtr(defaultValue)
	}

	arg.Name = strings.TrimSpace(body)
	if arg.Name == "" {
		return Argument{}, fmt.Errorf("console signature: argument name is required")
	}
	return arg, nil
}

func parseOptionToken(body string) (Option, error) {
	opt := Option{}
	body = strings.TrimSpace(strings.TrimPrefix(body, "--"))
	if body == "" {
		return Option{}, fmt.Errorf("console signature: option name is required")
	}

	if left, right, hasShortcut := strings.Cut(body, "|"); hasShortcut {
		opt.Shortcut = strings.TrimSpace(left)
		body = strings.TrimSpace(right)
		if opt.Shortcut == "" {
			return Option{}, fmt.Errorf("console signature: option shortcut is required")
		}
	}

	if strings.HasSuffix(body, "*") {
		opt.IsArray = true
		body = strings.TrimSuffix(body, "*")
	}

	if name, defaultValue, acceptsValue := strings.Cut(body, "="); acceptsValue {
		opt.Name = strings.TrimSpace(name)
		opt.ValueMode = OptionValueOptional
		if defaultValue != "" {
			opt.DefaultValue = stringPtr(defaultValue)
		}
	} else {
		opt.Name = strings.TrimSpace(body)
		opt.ValueMode = OptionValueNone
	}

	if opt.Name == "" {
		return Option{}, fmt.Errorf("console signature: option name is required")
	}
	return opt, nil
}

func stringPtr(value string) *string {
	cloned := value
	return &cloned
}

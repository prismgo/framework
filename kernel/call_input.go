package kernel

import (
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"strings"

	"github.com/prismgo/framework/console"
)

func encodeCallInput(definition console.Definition, input console.CallInput) ([]string, error) {
	args := make([]string, 0, len(definition.Arguments)+len(input.Options)*2)
	for _, argument := range definition.Arguments {
		value, ok := input.Arguments[argument.Name]
		if !ok || value == nil {
			continue
		}
		values, err := callValues(value, 0)
		if err != nil {
			return nil, fmt.Errorf("kernel call input argument %q: %w", argument.Name, err)
		}
		args = append(args, values...)
	}

	optionNames := make(map[string]console.Option, len(definition.Options))
	for _, option := range definition.Options {
		optionNames[option.Name] = option
	}
	keys := make([]string, 0, len(input.Options))
	for key := range input.Options {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		value := input.Options[key]
		if value == nil {
			continue
		}
		option, known := optionNames[key]
		values, err := callValues(value, 0)
		if err != nil {
			return nil, fmt.Errorf("kernel call input option %q: %w", key, err)
		}
		if len(values) == 0 {
			continue
		}
		if b, ok := value.(bool); ok {
			if !b {
				continue
			}
			args = append(args, "--"+key)
			continue
		}
		if known && option.ValueMode == console.OptionValueNone {
			enabled, err := strconv.ParseBool(values[0])
			if err == nil {
				if enabled {
					args = append(args, "--"+key)
				}
				continue
			}
		}
		for _, item := range values {
			args = append(args, "--"+key+"="+item)
		}
	}
	return args, nil
}

// callValues 将任意类型的值转换为字符串切片，用于命令行参数和选项。
// 支持基本类型、切片、数组以及实现了 fmt.Stringer 的类型。
// depth 参数用于限制递归深度，防止无限递归导致栈溢出。
func callValues(value any, depth int) ([]string, error) {
	// 限制递归深度为 10 层，防止循环引用或过深嵌套
	if depth > 10 {
		return nil, fmt.Errorf("callValues: maximum recursion depth exceeded")
	}

	switch typed := value.(type) {
	case string:
		if strings.TrimSpace(typed) == "" {
			return nil, nil
		}
		return []string{typed}, nil
	case fmt.Stringer:
		return []string{typed.String()}, nil
	case []string:
		return append([]string(nil), typed...), nil
	case []int:
		result := make([]string, 0, len(typed))
		for _, item := range typed {
			result = append(result, strconv.Itoa(item))
		}
		return result, nil
	case bool:
		return []string{strconv.FormatBool(typed)}, nil
	case int:
		return []string{strconv.Itoa(typed)}, nil
	case int64:
		return []string{strconv.FormatInt(typed, 10)}, nil
	case uint:
		return []string{strconv.FormatUint(uint64(typed), 10)}, nil
	case uint64:
		return []string{strconv.FormatUint(typed, 10)}, nil
	case float64:
		return []string{strconv.FormatFloat(typed, 'f', -1, 64)}, nil
	}
	rv := reflect.ValueOf(value)
	if rv.Kind() == reflect.Slice || rv.Kind() == reflect.Array {
		result := make([]string, 0, rv.Len())
		for i := 0; i < rv.Len(); i++ {
			values, err := callValues(rv.Index(i).Interface(), depth+1)
			if err != nil {
				return nil, err
			}
			result = append(result, values...)
		}
		return result, nil
	}
	return []string{fmt.Sprint(value)}, nil
}

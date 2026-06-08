package queue

import (
	"encoding/json"
	"fmt"

	"github.com/prismgo/framework/queue/payload"
)

func encryptedRawMessage(token string) (payload.Payload, error) {
	body, err := json.Marshal(token)
	if err != nil {
		return nil, err
	}
	return payload.Payload(body), nil
}

func encryptedToken(payload payload.Payload) (string, error) {
	var token string
	if err := json.Unmarshal(payload, &token); err != nil {
		return "", fmt.Errorf("queue: parse encrypted payload: %w", err)
	}
	return token, nil
}

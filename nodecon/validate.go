package nodecon

import (
	"errors"
	"fmt"
	"net/url"
	"strings"
)

var (
	ErrEmptyOrNilUrl    = errors.New("url is empty or nil")
	ErrInvalidRpcUrl    = errors.New("invalid rpc url: ")
	ErrInvalidSubRpcUrl = errors.New("invalid sub rpc url: ")
)

var (
	subRpcAllowedSchemes = map[string]struct{}{"ws": {}, "wss": {}, "ipc": {}}
)

// ValidateSubRpcUrl validates url for a subscription rpc node.
//
// Only websocket and ipc urls are supported.
func ValidateSubRpcUrl(rawUrl string) error {
	if rawUrl == "" {
		return ErrEmptyOrNilUrl
	}

	if err := validateScheme(rawUrl, subRpcAllowedSchemes); err != nil {
		return fmt.Errorf("%w%s: %s", ErrInvalidSubRpcUrl, rawUrl, err)
	}

	return nil
}

func validateScheme(rawUrl string, allowed map[string]struct{}) error {
	if isIpcPath(rawUrl) {
		if _, ok := allowed["ipc"]; !ok {
			return fmt.Errorf("scheme ipc not allowed")
		}
		return nil
	}

	parsed, err := url.Parse(rawUrl)
	if err != nil {
		return fmt.Errorf("parse url: %w", err)
	}

	scheme := strings.ToLower(parsed.Scheme)
	if scheme == "" {
		return fmt.Errorf("missing scheme")
	}

	if _, ok := allowed[scheme]; !ok {
		return fmt.Errorf("scheme %s not allowed", scheme)
	}

	if scheme != "ipc" && parsed.Host == "" {
		return fmt.Errorf("missing host")
	}

	return nil
}

func isIpcPath(rawUrl string) bool {
	if strings.HasSuffix(rawUrl, ".ipc") {
		return true
	}
	if strings.HasPrefix(rawUrl, "/") || strings.HasPrefix(rawUrl, `\\`) {
		return true
	}
	return false
}

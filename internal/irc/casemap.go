package irc

import "strings"

// CaseMapping is an ISUPPORT CASEMAPPING algorithm.
type CaseMapping int

const (
	CaseRFC1459 CaseMapping = iota
	CaseStrictRFC1459
	CaseASCII
)

// ParseCaseMapping parses an ISUPPORT CASEMAPPING value.
func ParseCaseMapping(s string) CaseMapping {
	switch strings.ToLower(s) {
	case "ascii":
		return CaseASCII
	case "strict-rfc1459":
		return CaseStrictRFC1459
	default:
		return CaseRFC1459
	}
}

// Fold lowercases s according to the casemapping.
func (c CaseMapping) Fold(s string) string {
	switch c {
	case CaseASCII:
		return foldASCII(s)
	case CaseStrictRFC1459:
		return foldStrictRFC1459(s)
	default:
		return foldRFC1459(s)
	}
}

// Equal reports whether a and b are equal under casemapping.
func (c CaseMapping) Equal(a, b string) bool {
	return c.Fold(a) == c.Fold(b)
}

// Canonical is an alias for Fold (folded form for maps/keys).
func (c CaseMapping) Canonical(s string) string {
	return c.Fold(s)
}

func foldASCII(s string) string {
	b := []byte(s)
	for i := 0; i < len(b); i++ {
		if b[i] >= 'A' && b[i] <= 'Z' {
			b[i] += 'a' - 'A'
		}
	}
	return string(b)
}

func foldRFC1459(s string) string {
	b := []byte(s)
	for i := 0; i < len(b); i++ {
		switch {
		case b[i] >= 'A' && b[i] <= 'Z':
			b[i] += 'a' - 'A'
		case b[i] == '[':
			b[i] = '{'
		case b[i] == ']':
			b[i] = '}'
		case b[i] == '\\':
			b[i] = '|'
		case b[i] == '~':
			b[i] = '^'
		}
	}
	return string(b)
}

func foldStrictRFC1459(s string) string {
	b := []byte(s)
	for i := 0; i < len(b); i++ {
		switch {
		case b[i] >= 'A' && b[i] <= 'Z':
			b[i] += 'a' - 'A'
		case b[i] == '[':
			b[i] = '{'
		case b[i] == ']':
			b[i] = '}'
		case b[i] == '\\':
			b[i] = '|'
		}
	}
	return string(b)
}

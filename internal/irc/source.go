package irc

import "strings"

// ServerSource reports whether a line's source is a server rather than a person: none at all,
// or a server name (no "!" or "@", contains ".").
func ServerSource(src string) bool {
	return src == "" || (!strings.Contains(src, "!") && !strings.Contains(src, "@") && strings.Contains(src, "."))
}

// IsServerName reports whether a stored history target is a server's name: it contains a dot
// and no "!" or "@", and is neither a channel (by the default prefixes #&+!) nor a $mask. A
// nick cannot contain a dot, so no query target is one.
func IsServerName(target string) bool {
	return target != "" && strings.IndexByte("#&+!$", target[0]) < 0 && ServerSource(target)
}

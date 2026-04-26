package dataapi

import (
	"fmt"
	"strings"
)

// ParseNamedParams converts SQL using :name style parameters into PostgreSQL's
// positional $N style. It returns the rewritten SQL and an ordered slice of
// argument values built from the provided params map.
//
// Rules:
//   - :name tokens are replaced with $1, $2, … in order of first appearance.
//   - If the same :name appears more than once, all occurrences share the same $N.
//   - :name inside single-quoted string literals ('…') is left untouched.
//   - :name inside double-quoted identifiers ("…") is left untouched.
//   - :name inside line comments (--…) is left untouched.
//   - :name inside block comments (/*…*/) is left untouched.
//   - :name inside dollar-quoted strings ($tag$…$tag$) is left untouched.
//   - Returns an error if a :name token has no matching entry in params.
//
// If params is nil or empty and the SQL has no :name tokens the original SQL is
// returned unchanged with a nil args slice.
func ParseNamedParams(query string, params map[string]any) (string, []any, error) {
	if len(params) == 0 && !strings.ContainsRune(query, ':') {
		return query, nil, nil
	}

	var out strings.Builder
	out.Grow(len(query))

	args := make([]any, 0, len(params))
	nameToIdx := make(map[string]int, len(params))

	i := 0
	n := len(query)

	for i < n {
		c := query[i]

		switch {
		// ── single-quoted string literal ─────────────────────────────────────
		case c == '\'':
			out.WriteByte(c)
			i++
			for i < n {
				ch := query[i]
				out.WriteByte(ch)
				i++
				if ch == '\'' {
					if i < n && query[i] == '\'' {
						out.WriteByte(query[i])
						i++
					} else {
						break
					}
				}
			}

		// ── double-quoted identifier ──────────────────────────────────────────
		case c == '"':
			out.WriteByte(c)
			i++
			for i < n {
				ch := query[i]
				out.WriteByte(ch)
				i++
				if ch == '"' {
					if i < n && query[i] == '"' {
						out.WriteByte(query[i])
						i++
					} else {
						break
					}
				}
			}

		// ── line comment ──────────────────────────────────────────────────────
		case c == '-' && i+1 < n && query[i+1] == '-':
			for i < n && query[i] != '\n' {
				out.WriteByte(query[i])
				i++
			}

		// ── block comment ─────────────────────────────────────────────────────
		case c == '/' && i+1 < n && query[i+1] == '*':
			out.WriteByte(c)
			i++
			out.WriteByte(query[i])
			i++
			for i < n {
				if query[i] == '*' && i+1 < n && query[i+1] == '/' {
					out.WriteByte(query[i])
					out.WriteByte(query[i+1])
					i += 2
					break
				}
				out.WriteByte(query[i])
				i++
			}

		// ── dollar-quoted string ($tag$…$tag$) ────────────────────────────────
		case c == '$':
			j := i + 1
			for j < n && (isAlphaUnderscore(query[j]) || (j > i+1 && isAlphaNumUnderscore(query[j]))) {
				j++
			}
			if j < n && query[j] == '$' {
				tag := query[i : j+1]
				out.WriteString(tag)
				i = j + 1
				for i < n {
					if strings.HasPrefix(query[i:], tag) {
						out.WriteString(tag)
						i += len(tag)
						break
					}
					out.WriteByte(query[i])
					i++
				}
			} else {
				out.WriteByte(c)
				i++
			}

		// ── named parameter ───────────────────────────────────────────────────
		case c == ':' && i+1 < n && isAlphaUnderscore(query[i+1]):
			i++ // skip ':'
			j := i
			for j < n && isAlphaNumUnderscore(query[j]) {
				j++
			}
			name := query[i:j]
			i = j

			idx, seen := nameToIdx[name]
			if !seen {
				val, ok := params[name]
				if !ok {
					return "", nil, fmt.Errorf("Cannot find parameter: %s", name)
				}
				args = append(args, val)
				idx = len(args)
				nameToIdx[name] = idx
			}
			fmt.Fprintf(&out, "$%d", idx)

		// ── everything else ───────────────────────────────────────────────────
		default:
			out.WriteByte(c)
			i++
		}
	}

	return out.String(), args, nil
}

func isAlphaUnderscore(c byte) bool {
	return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c == '_'
}

func isAlphaNumUnderscore(c byte) bool {
	return isAlphaUnderscore(c) || (c >= '0' && c <= '9')
}

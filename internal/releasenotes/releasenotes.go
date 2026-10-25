// Package releasenotes extracts a single release section from the cumulative
// RELEASE_NOTES.md file.
package releasenotes

import (
	"errors"
	"fmt"
	"strings"
)

const (
	headingPrefix = "# smob "
	emDashSep     = " — "
	hyphenSep     = " - "
	sectionRule   = "---"
)

// Section is one release's notes.
type Section struct {
	Title string // text after "# smob vX.Y.Z — ", may be empty
	Body  string // markdown body with the heading removed, outer blank lines trimmed, no trailing newline
}

// Extract returns the section for tag from md.
func Extract(md, tag string) (Section, error) {
	if tag == "" {
		return Section{}, errors.New("tag is empty")
	}
	tag = normalizeTag(tag)
	lines := strings.Split(strings.ReplaceAll(md, "\r\n", "\n"), "\n")

	start := -1
	var title string
	for i, line := range lines {
		if t, ok := heading(line, tag); ok {
			start, title = i, t
			break
		}
	}
	if start < 0 {
		return Section{}, fmt.Errorf("RELEASE_NOTES.md has no section for %s; expected a heading %q",
			tag, headingPrefix+tag+emDashSep+"<title>")
	}

	end := len(lines)
	for i := start + 1; i < len(lines); i++ {
		if isTerminator(lines[i]) {
			end = i
			break
		}
	}

	body := trimBlankLines(lines[start+1 : end])
	if len(body) == 0 {
		return Section{}, fmt.Errorf("release notes for %s are empty", tag)
	}
	return Section{Title: title, Body: strings.Join(body, "\n")}, nil
}

// normalizeTag returns tag with the leading "v" that release headings use.
func normalizeTag(tag string) string {
	if strings.HasPrefix(tag, "v") {
		return tag
	}
	return "v" + tag
}

// heading reports whether line is the section heading for tag and returns its
// title.
func heading(line, tag string) (string, bool) {
	rest, ok := strings.CutPrefix(line, headingPrefix)
	if !ok {
		return "", false
	}
	after, ok := strings.CutPrefix(rest, tag)
	if !ok {
		return "", false
	}
	// The tag must be followed by the end of the line, whitespace, or a
	// separator, so a heading for v0.2.30 does not match a request for v0.2.3.
	// A separator keeps its surrounding spaces: " — " or " - " only.
	if strings.TrimSpace(after) == "" {
		return "", true
	}
	if title, ok := strings.CutPrefix(after, emDashSep); ok {
		return strings.TrimSpace(title), true
	}
	if title, ok := strings.CutPrefix(after, hyphenSep); ok {
		return strings.TrimSpace(title), true
	}
	return "", false
}

// isTerminator reports whether line ends the current section: a horizontal rule
// or the next release heading.
func isTerminator(line string) bool {
	return strings.HasPrefix(line, headingPrefix) || strings.TrimSpace(line) == sectionRule
}

// trimBlankLines drops leading and trailing blank lines, preserving internal
// ones.
func trimBlankLines(lines []string) []string {
	start, end := 0, len(lines)
	for start < end && strings.TrimSpace(lines[start]) == "" {
		start++
	}
	for end > start && strings.TrimSpace(lines[end-1]) == "" {
		end--
	}
	return lines[start:end]
}

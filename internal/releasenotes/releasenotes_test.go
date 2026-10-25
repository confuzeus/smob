package releasenotes

import (
	"strings"
	"testing"
)

func TestExtract(t *testing.T) {
	tests := []struct {
		name      string
		md        string
		tag       string
		wantTitle string
		wantBody  string
	}{
		{
			name: "middle section stops at rule",
			md: "# smob v0.2.3 — Middle\n" +
				"Middle body line.\n" +
				"\n" +
				"---\n" +
				"\n" +
				"# smob v0.2.2 — Older\n" +
				"Older body line.\n",
			tag:       "v0.2.3",
			wantTitle: "Middle",
			wantBody:  "Middle body line.",
		},
		{
			name: "oldest section runs to end of file",
			md: "# smob v0.2.3 — Oldest\n" +
				"Line one.\n" +
				"Line two.\n",
			tag:       "v0.2.3",
			wantTitle: "Oldest",
			wantBody:  "Line one.\nLine two.",
		},
		{
			name: "rule with surrounding spaces terminates",
			md: "# smob v0.2.3 — Spaced rule\n" +
				"Body.\n" +
				"  ---  \n" +
				"# smob v0.2.2 — Older\n" +
				"Not this one.\n",
			tag:       "v0.2.3",
			wantTitle: "Spaced rule",
			wantBody:  "Body.",
		},
		{
			name: "ascii hyphen separator",
			md: "# smob v0.2.2 - ASCII\n" +
				"Hyphen body.\n",
			tag:       "v0.2.2",
			wantTitle: "ASCII",
			wantBody:  "Hyphen body.",
		},
		{
			name: "bare heading has no title",
			md: "# smob v0.2.3\n" +
				"Untitled body.\n",
			tag:       "v0.2.3",
			wantTitle: "",
			wantBody:  "Untitled body.",
		},
		{
			name: "title with whitespace-only remainder",
			md: "# smob v0.2.3 —   \n" +
				"Body after spaced separator.\n",
			tag:       "v0.2.3",
			wantTitle: "",
			wantBody:  "Body after spaced separator.",
		},
		{
			name: "tag without v prefix resolves the same section",
			md: "# smob v0.2.3 — Same\n" +
				"Body.\n",
			tag:       "0.2.3",
			wantTitle: "Same",
			wantBody:  "Body.",
		},
		{
			name: "version boundary falls through to exact heading",
			md: "# smob v0.2.30 — Thirty\n" +
				"Thirty body.\n" +
				"\n" +
				"---\n" +
				"\n" +
				"# smob v0.2.3 — Three\n" +
				"Three body.\n",
			tag:       "v0.2.3",
			wantTitle: "Three",
			wantBody:  "Three body.",
		},
		{
			name: "body keeps internal blank lines and indentation",
			md: "# smob v0.2.3 — Formatting\n" +
				"\n" +
				"    indented line\n" +
				"\n" +
				"- bullet\n" +
				"   - nested\n" +
				"\n" +
				"---\n",
			tag:       "v0.2.3",
			wantTitle: "Formatting",
			wantBody:  "    indented line\n\n- bullet\n   - nested",
		},
		{
			name: "lf input",
			md: "# smob v0.2.3 — Line endings\n" +
				"First.\n" +
				"\n" +
				"Second.\n",
			tag:       "v0.2.3",
			wantTitle: "Line endings",
			wantBody:  "First.\n\nSecond.",
		},
		{
			name: "crlf input matches lf input",
			md: "# smob v0.2.3 — Line endings\r\n" +
				"First.\r\n" +
				"\r\n" +
				"Second.\r\n",
			tag:       "v0.2.3",
			wantTitle: "Line endings",
			wantBody:  "First.\n\nSecond.",
		},
		{
			name: "indented heading is body text",
			md: "# smob v0.2.3 — Real\n" +
				"Before.\n" +
				"  # smob v0.2.2 — Indented\n" +
				"After.\n",
			tag:       "v0.2.3",
			wantTitle: "Real",
			wantBody:  "Before.\n  # smob v0.2.2 — Indented\nAfter.",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Extract(tt.md, tt.tag)
			if err != nil {
				t.Fatalf("Extract() error = %v", err)
			}
			if got.Title != tt.wantTitle {
				t.Errorf("Extract() Title = %q, want %q", got.Title, tt.wantTitle)
			}
			if got.Body != tt.wantBody {
				t.Errorf("Extract() Body = %q, want %q", got.Body, tt.wantBody)
			}
		})
	}
}

func TestExtractErrors(t *testing.T) {
	tests := []struct {
		name    string
		md      string
		tag     string
		wantErr string
	}{
		{
			name:    "empty tag",
			md:      "# smob v0.2.3 — Present\nBody.\n",
			tag:     "",
			wantErr: "tag is empty",
		},
		{
			name:    "missing tag",
			md:      "# smob v0.2.3 — Present\nBody.\n",
			tag:     "v9.9.9",
			wantErr: "has no section for v9.9.9",
		},
		{
			name:    "version boundary does not match longer version",
			md:      "# smob v0.2.30 — Thirty\nOnly a longer version.\n",
			tag:     "v0.2.3",
			wantErr: "has no section for v0.2.3",
		},
		{
			name:    "heading with empty body",
			md:      "# smob v0.2.3 — Empty\n\n   \n",
			tag:     "v0.2.3",
			wantErr: "release notes for v0.2.3 are empty",
		},
		{
			name:    "heading followed by only a rule",
			md:      "# smob v0.2.3 — Rule only\n---\n# smob v0.2.2 — Older\nBody.\n",
			tag:     "v0.2.3",
			wantErr: "are empty",
		},
		{
			name:    "indented heading does not resolve",
			md:      "# smob v0.2.3 — Real\nBody.\n  # smob v0.2.2 — Indented\n",
			tag:     "v0.2.2",
			wantErr: "has no section for v0.2.2",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Extract(tt.md, tt.tag)
			if err == nil {
				t.Fatalf("Extract() = %+v, want error containing %q", got, tt.wantErr)
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("Extract() error = %q, want it to contain %q", err, tt.wantErr)
			}
		})
	}
}

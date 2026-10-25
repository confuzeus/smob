// Command release-notes extracts a single release section from
// RELEASE_NOTES.md so a maintainer can preview it before tagging or write the
// title and body to files for a release.
package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/confuzeus/smob/internal/releasenotes"
)

func main() {
	if err := run(os.Args[1:], os.Stdout, os.Stderr); err != nil {
		fmt.Fprintf(os.Stderr, "release-notes: %v\n", err)
		os.Exit(1)
	}
}

// run parses args and previews or writes the requested release section.
func run(args []string, stdout, stderr io.Writer) error {
	fs := flag.NewFlagSet("release-notes", flag.ContinueOnError)
	fs.SetOutput(stderr)
	tag := fs.String("tag", "", "release tag, with or without a leading v (required)")
	file := fs.String("file", "RELEASE_NOTES.md", "path to the release notes file")
	titleFile := fs.String("title-file", "", "write the section title to this file")
	bodyFile := fs.String("body-file", "", "write the section body to this file")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}
	if *tag == "" {
		fs.Usage()
		return errors.New("-tag is required")
	}

	md, err := os.ReadFile(*file)
	if err != nil {
		return err
	}
	section, err := releasenotes.Extract(string(md), *tag)
	if err != nil {
		return err
	}

	if *titleFile == "" && *bodyFile == "" {
		fmt.Fprintf(stdout, "%s\n\n%s\n", section.Title, section.Body)
		return nil
	}
	if *titleFile != "" {
		if err := os.WriteFile(*titleFile, []byte(section.Title+"\n"), 0o644); err != nil {
			return err
		}
	}
	if *bodyFile != "" {
		if err := os.WriteFile(*bodyFile, []byte(section.Body+"\n"), 0o644); err != nil {
			return err
		}
	}
	return nil
}

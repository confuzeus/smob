---
name: release
description: Use when a change to smob is complete and ready to land. Runs the commit, SEMVER tag, and release-record workflow (RELEASE_NOTES.md and CHANGELOG.md) so every completed change ships as a tagged release.
---

# Release Workflow for smob

## Stance

- Every completed change is a release. One coherent change becomes one commit,
  one release-record update, and one annotated SEMVER tag.
- The release record has two audiences. `RELEASE_NOTES.md` describes changes a
  user can observe; `CHANGELOG.md` records the developer-facing history of
  every release.
- The version lives in git tags only. The `justfile` derives `VERSION` from
  `git describe --tags`, so there is no version constant to edit in source.
- The tag points at the commit that contains both the change and its release
  notes. Never tag a tree that does not describe itself.
- Do not move, reuse, or delete a tag. A published tag is immutable; fix forward
  with the next patch version.
- Do not push. No remote is configured, and publishing commits, tags, release
  artifacts, and images is a maintainer decision.
- An explicit user instruction wins: "commit but do not tag yet" or "this is
  WIP" means commit without notes and without a tag, and finish the workflow
  when the change is green. Never invent an exception yourself.

## Preconditions

Run from the repository root and confirm all of the following before you stage
anything:

1. `git status --short` shows only files that belong to this change. If it shows
   work you did not make, stop and ask. Never stage or commit someone else's
   changes, and never `git add -A` blindly.
2. You are on the branch the change belongs on. `master` is the only branch
   today; do not create a branch unless the user asks.
3. The change is complete. Partial work is committed only when the user asks
   for a WIP commit, and a WIP commit is not tagged.

## Verification gate

Run the checks that match the change, and tag only when they pass:

```sh
gofmt -l .                              # must print nothing
just vet                                # go vet -tags=integration ./...
just lint                               # staticcheck -tags=integration ./...
just test                               # unit tests, then the fast integration suite
go mod tidy && git diff --exit-code -- go.mod go.sum
```

- Storage, Docker, collector, or concurrency changes also need
  `just test-all` with MinIO and Docker available, and `just test-race` when the
  change touches concurrency. If the services are unavailable, say so and let
  the user decide rather than tagging an unverified change.
- A documentation-only change still runs `gofmt -l .` and `just test`: the gate
  proves the tree was green when the tag was created.
- If a check fails, fix the failure or report it. Do not tag, and do not
  suppress a check to get to the tag.

## Choose the next version

Read the latest tag, then bump it according to the change you are landing:

```sh
git describe --tags --abbrev=0 --match 'v*'
```

| Change | Bump | Example (latest `v0.2.1`) |
| --- | --- | --- |
| `feat` — new user-visible behavior | minor | `v0.3.0` |
| `fix`, `perf`, `refactor`, `docs`, `test`, `ci`, `build`, `chore` | patch | `v0.2.2` |
| `feat!` or `BREAKING CHANGE:` footer | minor while major is `0` | `v0.3.0` |
| `feat!` or `BREAKING CHANGE:` footer at `1.0.0` or later | major | `v2.0.0` |

- While the major version is `0`, semver's public-API rule applies: a breaking
  change bumps the minor version, not the major. Do not jump to `1.0.0` unless
  the user declares the API stable.
- A commit that mixes types takes the highest bump among them.
- If the computed tag already exists, stop. Do not re-tag, and do not bump past
  a version you did not create without asking.
- Tag format is always `vMAJOR.MINOR.PATCH`, lowercase `v`, no suffix, no build
  metadata.

## Update the release record

Two files record a release, split by audience:

- `RELEASE_NOTES.md` — what changed for someone running smob.
- `CHANGELOG.md` — what changed inside the repository, for contributors.

Every release gets exactly one `CHANGELOG.md` entry. A release with user-visible
changes also gets a `RELEASE_NOTES.md` section; a release without them gets a
short stub there instead. Both files are cumulative and newest first: prepend to
both, and never rewrite or delete an older entry.

### `RELEASE_NOTES.md`

```markdown
# smob vX.Y.Z — <Short Title>

<One short paragraph: what changed for a user of the binary and why it matters.
Say plainly when nothing user-visible changed.>

## Highlights

- **<Capability or change>.** <What it does, in one or two sentences.>

## Upgrade notes

<Only when behavior, defaults, configuration, or storage compatibility changed.
Omit the section entirely otherwise.>

## Known limitations

<Only when the new release adds or removes a limitation. Omit otherwise.>

---

<previous release sections unchanged below the rule>
```

Rules for the section:

- Prepend it above the existing `# smob …` heading, separated by a `---` rule.
- Write for an operator: name the new or changed command, flag, config field,
  metric, or storage behavior. "Various improvements" is not a release note.
- Include only what a user can observe: features, behavior changes, bug fixes,
  breaking changes, upgrade actions, and new or removed limitations.
- Exclude repository internals: skills, `AGENTS.md`, CI, tests, internal
  refactors with no behavior change, commit hashes, development tags, and build
  instructions already in `README.md` or `docs/quickstart.md`.
- Do not repeat the metric catalogue, CLI reference, quick start, storage
  layout, documentation index, or license from `README.md`; link to it when a
  reader needs it.
- Keep it accurate and small. An internal-only release gets a two- or three-line
  section saying the binary is unchanged and there is no upgrade action; do not
  list the tooling changes there.

### `CHANGELOG.md`

```markdown
## vX.Y.Z — <Short Title>

<Whether anything user-visible changed, and a link to `RELEASE_NOTES.md` when
it did.>

- <Developer-facing change: tooling, tests, CI, build, docs, or repository
  process.>
```

- Add one entry for every release, including internal-only and documentation
  releases, above the previous entry.
- Record what a contributor needs: the changed package or area, a new
  dependency or test service, a migration of internal layout, or a change to
  the release process. Do not restate the user-facing highlights.

## Commit

Stage only the files in this change, including both release-record files:

```sh
git add <changed files>
git status --short          # re-read the staged set before committing
```

Write one conventional commit:

- Subject: `type(scope): imperative summary`, at most 72 characters. The type
  matches the bump you chose (`feat`, `fix`, `docs`, `chore`, …). Scope is the
  package or area, for example `cli`, `store`, `s3`, `docker`.
- Body: why the change was made, and anything the diff cannot show — a
  trade-off, a deliberate exclusion, a migration note. Wrap at 72 columns.
- No emoji, no trailing period in the subject, no `Co-Authored-By` unless the
  user asks for it.
- Never pass `--no-verify`, never amend a commit that is already tagged, and
  never commit ignored artifacts (`smob`, `dist/`).

## Tag

Create an annotated tag on the release commit, matching the existing message
style (`smob vX.Y.Z: <summary>`):

```sh
git tag -a vX.Y.Z -m "smob vX.Y.Z: <one-line summary>"
```

Then verify before reporting:

```sh
git tag -n1 -l vX.Y.Z                  # tag exists and points at the new commit
git describe --tags --abbrev=0         # reports vX.Y.Z
git status --short                     # clean
```

`git describe` may later report a suffix such as `v0.2.2-1-gabc1234` for commits
after the tag. That is expected: the tag marks the release, not the branch tip.

## Report

Tell the user, in one short block:

- The version and tag message.
- The commit subject and short hash.
- That the working tree is clean and nothing was pushed.
- Any check that was skipped and why (`test-all` without MinIO/Docker, for
  example).

## Worked example

Latest tag is `v0.2.1` and the change is a new `query --sort` flag.

```sh
git status --short
gofmt -l .
just vet
just lint
just test
git describe --tags --abbrev=0 --match 'v*'      # v0.2.1

# feat -> minor -> v0.3.0
# Edit RELEASE_NOTES.md: prepend "# smob v0.3.0 — Sorted Query Output".
# Edit CHANGELOG.md: prepend the v0.3.0 entry.

git add internal/query docs/README-links README.md RELEASE_NOTES.md CHANGELOG.md
git commit -m "feat(query): add --sort for deterministic output" \
  -m "Sort samples by timestamp, metric, or label so scripts can diff query output."
git tag -a v0.3.0 -m "smob v0.3.0: query --sort and release notes"
git tag -n1 -l v0.3.0
git describe --tags --abbrev=0
git status --short
```

## Edge cases

- **The user asks for several unrelated changes in one task.** Land them as
  separate commits, each with its own tag and notes section, in the order they
  were completed.
- **A tag exists but the notes section is missing.** The tag is immutable. Add
  the missing section under the next patch version and say what happened; do
  not rewrite history.
- **The change is only the release record.** Still commit and tag it; the
  release record is part of the contract.
- **The tree is dirty with unrelated work.** Commit nothing until the user says
  what to do with it.
- **A breaking change lands at `0.x`.** Bump the minor version and call the
  break out under `## Upgrade notes`. Do not bump the major version.

## Anti-patterns

- Tagging before the notes commit, or tagging a commit that lacks its section.
- Using `git tag -f`, deleting a tag, or reusing a version.
- Hand-editing a version constant; the tag is the only source of truth.
- `git add -A` or `git commit -am` in a tree with unrelated changes.
- Pushing, opening a GitHub release, or publishing artifacts and images.
- Writing "misc fixes" or a changelog that just repeats commit subjects.
- Putting repository, tooling, or process detail in `RELEASE_NOTES.md`.
- Listing user-facing highlights in `CHANGELOG.md`, or leaving a release out of
  it because nothing was user-visible.
- Tagging a tree whose tests fail because "the tag is only local".

## Definition of done

- [ ] `gofmt -l .` is empty and the applicable `just` checks pass.
- [ ] The version is the correct SEMVER bump from the latest tag.
- [ ] `CHANGELOG.md` has one entry for the version, and `RELEASE_NOTES.md` has
      a section for it, including the stub when nothing user-visible changed.
- [ ] One conventional commit contains the change and the notes.
- [ ] An annotated `vX.Y.Z` tag points at that commit.
- [ ] `git status` is clean and nothing was pushed.
- [ ] The user knows the version, commit, tag, and any skipped check.

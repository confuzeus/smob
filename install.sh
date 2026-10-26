#!/bin/sh
#
# Install smob from a GitHub release.
#
#   curl -fsSL https://github.com/confuzeus/smob/releases/latest/download/install.sh | sudo sh
#
# The script verifies the downloaded binary against the release's SHA256SUMS,
# installs it to /usr/local/bin/smob, writes /etc/smob/smob.yaml only when that
# file does not exist, installs the systemd unit, and enables and starts the
# service. Pass --no-service to install the binary alone.
#
# Download and read the script before running it if you prefer:
#
#   curl -fsSLO https://github.com/confuzeus/smob/releases/latest/download/install.sh
#   less install.sh
#   sudo sh install.sh
#
# Pin a release with --version or SMOB_VERSION. Advanced mirror and test use is
# served by SMOB_RELEASES_BASE_URL, which replaces the release download prefix.
#
# This script requires Linux on amd64 or arm64, curl, sha256sum, and root. It
# never prompts: standard input is the script itself when piped.

set -eu

BASE_URL=${SMOB_RELEASES_BASE_URL:-https://github.com/confuzeus/smob/releases/download}
LATEST_URL=https://github.com/confuzeus/smob/releases/latest
VERSION=${SMOB_VERSION:-}
INSTALL_DIR=${SMOB_INSTALL_DIR:-/usr/local/bin}
NO_SERVICE=${SMOB_NO_SERVICE:-0}
INSTALL_DIR_SET=0
CONFIG=/etc/smob/smob.yaml
UNIT=/etc/systemd/system/smob.service
SERVICE=smob

tmpdir=
staged=

cleanup() {
	if [ -n "$tmpdir" ]; then
		rm -rf "$tmpdir"
	fi
	if [ -n "$staged" ]; then
		rm -f "$staged"
	fi
}
trap cleanup EXIT
trap 'exit 1' HUP INT TERM

die() {
	printf 'smob install: %s\n' "$*" >&2
	exit 1
}

usage() {
	cat <<'EOF'
Install smob from a GitHub release.

Usage: install.sh [options]

Options:
  --version VERSION    Release to install, for example v0.2.5. A bare
                       0.2.5 is accepted. Defaults to the latest release.
  --install-dir DIR    Directory for the binary. Defaults to /usr/local/bin.
  --no-service         Install the binary only; do not write the configuration
                       or install and start the systemd unit.
  -h, --help           Show this help and exit.

Environment:
  SMOB_VERSION             Same as --version.
  SMOB_INSTALL_DIR         Same as --install-dir.
  SMOB_NO_SERVICE=1        Same as --no-service.
  SMOB_RELEASES_BASE_URL   Replace the release download prefix. Intended for
                           mirrors and the release smoke test.

Without --no-service, installation requires root, writes
/etc/smob/smob.yaml if it does not exist, installs the hardened systemd unit,
and enables and starts the smob service. A non-root run must pass both
--no-service and --install-dir.

Examples:
  curl -fsSL https://github.com/confuzeus/smob/releases/latest/download/install.sh | sudo sh
  curl -fsSL https://github.com/confuzeus/smob/releases/latest/download/install.sh | sudo sh -s -- --version v0.2.5
  curl -fsSL https://github.com/confuzeus/smob/releases/latest/download/install.sh | sh -s -- --no-service --install-dir "$HOME/.local/bin"
EOF
}

while [ "$#" -gt 0 ]; do
	case "$1" in
	--version)
		[ "$#" -ge 2 ] || die "--version requires a release tag"
		VERSION=$2
		shift 2
		;;
	--version=*)
		VERSION=${1#*=}
		shift
		;;
	--install-dir)
		[ "$#" -ge 2 ] || die "--install-dir requires a directory"
		INSTALL_DIR=$2
		INSTALL_DIR_SET=1
		shift 2
		;;
	--install-dir=*)
		INSTALL_DIR=${1#*=}
		INSTALL_DIR_SET=1
		shift
		;;
	--no-service)
		NO_SERVICE=1
		shift
		;;
	-h | --help)
		usage
		exit 0
		;;
	*)
		die "unknown argument: $1 (try --help)"
		;;
	esac
done

if [ "$NO_SERVICE" != 1 ] && [ "$INSTALL_DIR" != /usr/local/bin ]; then
	die "the systemd unit runs /usr/local/bin/smob; pass --no-service with --install-dir"
fi

if [ "$(id -u)" -ne 0 ]; then
	if [ "$NO_SERVICE" = 1 ] && [ "$INSTALL_DIR_SET" = 1 ]; then
		:
	else
		die "installation requires root; re-run with sudo, or pass --no-service --install-dir DIR"
	fi
fi

if [ "$(uname -s)" != Linux ]; then
	die "smob runs on Linux only; this host reports $(uname -s)"
fi

case "$(uname -m)" in
x86_64 | amd64)
	arch=amd64
	;;
aarch64 | arm64)
	arch=arm64
	;;
*)
	die "unsupported architecture $(uname -m); releases cover linux/amd64 and linux/arm64"
	;;
esac

for tool in curl sha256sum mktemp; do
	if ! command -v "$tool" >/dev/null 2>&1; then
		die "$tool is required but was not found on PATH"
	fi
done

if [ "$NO_SERVICE" != 1 ]; then
	if [ ! -d /run/systemd/system ]; then
		die "systemd is not running; pass --no-service to install the binary only"
	fi
	if ! command -v systemctl >/dev/null 2>&1; then
		die "systemctl is required but was not found on PATH; pass --no-service to install the binary only"
	fi
fi

if [ -z "$VERSION" ]; then
	effective=$(curl -fsSL -o /dev/null -w '%{url_effective}' "$LATEST_URL") ||
		die "could not resolve the latest release from $LATEST_URL"
	VERSION=${effective##*/}
	[ -n "$VERSION" ] || die "could not resolve the latest release from $LATEST_URL"
fi

case "$VERSION" in
v*) ;;
[0-9]*) VERSION="v$VERSION" ;;
esac

if ! printf '%s\n' "$VERSION" | grep -Eq '^v[0-9]+\.[0-9]+\.[0-9]+$'; then
	die "invalid release '$VERSION'; expected vMAJOR.MINOR.PATCH such as v0.2.5"
fi

asset="smob-linux-$arch"
release_url="$BASE_URL/$VERSION"
tmpdir=$(mktemp -d "${TMPDIR:-/tmp}/smob-install.XXXXXX") ||
	die "could not create a temporary directory"

download() {
	if ! curl -fsSL --retry 3 --retry-delay 1 -o "$2" "$1"; then
		die "download failed: $1"
	fi
}

printf 'installing smob %s for linux/%s\n' "$VERSION" "$arch"
download "$release_url/$asset" "$tmpdir/$asset"
download "$release_url/SHA256SUMS" "$tmpdir/SHA256SUMS"

expected=
while read -r hash name; do
	if [ "$name" = "$asset" ]; then
		expected=$hash
		break
	fi
done <"$tmpdir/SHA256SUMS"
[ -n "$expected" ] || die "SHA256SUMS for $VERSION has no entry for $asset"

actual=$(sha256sum "$tmpdir/$asset")
actual=${actual%% *}
if [ "$expected" != "$actual" ]; then
	die "checksum mismatch for $asset: SHA256SUMS says $expected, downloaded file is $actual"
fi
chmod 0755 "$tmpdir/$asset"

if [ "$NO_SERVICE" != 1 ]; then
	if [ ! -e "$CONFIG" ] && [ ! -L "$CONFIG" ]; then
		if [ ! -d /etc/smob ]; then
			mkdir -p /etc/smob
			chmod 0750 /etc/smob
		fi
		cat >"$tmpdir/smob.yaml" <<'EOF'
# Generated by the smob installer. See
# https://github.com/confuzeus/smob/blob/master/docs/configuration.md
data_dir: /var/lib/smob
sources:
  system:
    enabled: true
storage:
  primary: disk
EOF
		cp "$tmpdir/smob.yaml" "$CONFIG"
		chmod 0640 "$CONFIG"
		printf 'wrote %s\n' "$CONFIG"
	else
		printf 'keeping existing %s\n' "$CONFIG"
	fi

	if ! "$tmpdir/$asset" config --validate --config "$CONFIG" >/dev/null; then
		die "configuration $CONFIG is invalid; fix it and re-run"
	fi
fi

mkdir -p "$INSTALL_DIR" || die "could not create $INSTALL_DIR"
staged="$INSTALL_DIR/.smob-install.$$"
cp "$tmpdir/$asset" "$staged"
chmod 0755 "$staged"
mv -f "$staged" "$INSTALL_DIR/smob"
staged=
printf 'installed %s\n' "$INSTALL_DIR/smob"

if [ "$NO_SERVICE" != 1 ]; then
	download "$release_url/smob.service" "$tmpdir/smob.service"
	cp "$tmpdir/smob.service" "$UNIT"
	chmod 0644 "$UNIT"
	systemctl daemon-reload
	if systemctl is-active --quiet "$SERVICE"; then
		systemctl restart "$SERVICE" || die "could not restart the $SERVICE service"
	else
		systemctl enable --now "$SERVICE" || die "could not enable and start the $SERVICE service"
	fi
	if ! systemctl is-active --quiet "$SERVICE"; then
		systemctl status --no-pager "$SERVICE" >&2 || true
		die "the $SERVICE service did not start; see journalctl -u $SERVICE"
	fi
fi

printf '\nsmob %s installed at %s/smob\n' "$VERSION" "$INSTALL_DIR"
"$INSTALL_DIR/smob" --version || die "the installed binary failed to run"

if [ "$NO_SERVICE" != 1 ]; then
	printf 'configuration: %s\n' "$CONFIG"
	printf 'service:       systemctl status %s\n' "$SERVICE"
	printf 'logs:          journalctl -u %s -f\n' "$SERVICE"
fi

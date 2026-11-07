#!/bin/sh
# Small POSIX Maven bootstrap; all downloaded build state stays in this project.
set -eu
if [ -z "${JAVA_HOME:-}" ] && [ -d "/Library/Java/JavaVirtualMachines/temurin-21.jdk/Contents/Home" ]; then
  JAVA_HOME="/Library/Java/JavaVirtualMachines/temurin-21.jdk/Contents/Home"
  export JAVA_HOME
fi
ROOT=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
VERSION=3.9.16
MAVEN="$ROOT/.tools/apache-maven-$VERSION"
if [ ! -x "$MAVEN/bin/mvn" ]; then
  mkdir -p "$ROOT/.tools"
  ARCHIVE="$ROOT/.tools/apache-maven-$VERSION-bin.tar.gz"
  curl -fsSL --retry 3 "https://repo.maven.apache.org/maven2/org/apache/maven/apache-maven/$VERSION/apache-maven-$VERSION-bin.tar.gz" -o "$ARCHIVE"
  EXPECTED=831a8591fe20c8243b1dbe7d71e3244f31d1665b0804b2e825e38cbbe5ce0cafb8338851f90780735568773e0a6cd07bbec107cda0b896b008b861075358b6f6
  ACTUAL=$(shasum -a 512 "$ARCHIVE" | cut -d ' ' -f 1)
  if [ "$EXPECTED" != "$ACTUAL" ]; then echo 'Maven checksum mismatch' >&2; exit 1; fi
  tar -xzf "$ARCHIVE" -C "$ROOT/.tools"
  rm -f -- "$ARCHIVE"
fi
exec "$MAVEN/bin/mvn" "-Dmaven.repo.local=$ROOT/.m2" "$@"

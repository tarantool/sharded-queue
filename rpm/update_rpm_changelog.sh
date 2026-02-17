#!/bin/bash
set -e

SPEC_FILE="rpm/sharded-queue.spec"
CHANGELOG_MD="CHANGELOG.md"
MAINTAINER="Oleg Jukovec <oleg.jukovec@tarantool.org>"

VERSION="$1"
if [ -z "$VERSION" ]; then
  echo "Error: Version is required. Usage: $0 <version>"
  exit 1
fi
VERSION="${VERSION#v}"

if grep -q "$VERSION-1" "$SPEC_FILE" 2>/dev/null; then
    echo "Entry for version $VERSION already exists in $SPEC_FILE. Nothing to do."
    exit 0
fi

echo "Updating rpm changelog for version $VERSION from $CHANGELOG_MD"

# Get a corresponding release note from CHANGELOG.md.
SECTION=$(awk -v ver="$VERSION" '
    BEGIN { gsub(/\./, "\\.", ver) }
    $0 ~ "^## " ver " " {flag=1; next}
    /^## / {flag=0}
    flag {print}
' "$CHANGELOG_MD" | sed -e '/./,$!d' -e ':a;N;$!ba;s/\n*$//')

if [ -z "$SECTION" ]; then
    echo "Warning: No section found for version $VERSION in $CHANGELOG_MD. Aborting."
    exit 1
fi

# Convert release note to the rpm format.
RPM_ENTRIES=$(echo "$SECTION" | awk '
    /^[-*] / {
        sub(/^[-*] /, "- ");
        print;
        next;
    }
')

DATE_LINE=$(LC_TIME=C date +"%a %b %d %Y")
HEADER="* $DATE_LINE $MAINTAINER - $VERSION-1"

TMP_FILE=$(mktemp)
cat > "$TMP_FILE" <<EOF

$HEADER
$RPM_ENTRIES


EOF

sed -i "s/^Version:.*/Version: $VERSION-1/" "$SPEC_FILE"

awk -v newblock="$(cat $TMP_FILE)" '
    /^%changelog/ {
        print "%changelog"
        print newblock
        next
    }
    { print }
' "$SPEC_FILE" > "${SPEC_FILE}.new"
mv "${SPEC_FILE}.new" "$SPEC_FILE"

rm -f "$TMP_FILE"
echo "RPM changelog automatically updated for version $VERSION."

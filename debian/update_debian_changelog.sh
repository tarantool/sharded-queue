#!/bin/bash
set -e

CHANGELOG_MD="CHANGELOG.md"
DEBIAN_CHANGELOG="debian/changelog"
PACKAGE_NAME="sharded-queue"
MAINTAINER="Oleg Jukovec <oleg.jukovec@tarantool.org>"

VERSION="$1"
if [ -z "$VERSION" ]; then
    echo "Error: Version is required. Usage: $0 <version>"
    exit 1
fi
VERSION="${VERSION#v}"

if grep -q "^$PACKAGE_NAME ($VERSION-1) " "$DEBIAN_CHANGELOG" 2>/dev/null; then
    echo "Entry for version $VERSION already exists in $DEBIAN_CHANGELOG. Nothing to do."
    exit 0
fi

echo "Updating debian/changelog for version $VERSION from $CHANGELOG_MD"

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

# Convert release note to the debian format.
DEBIAN_ENTRIES=$(echo "$SECTION" | awk '
    /^[-*] / {
        sub(/^[-*] /, "  * ");
        print;
        next;
    }
    /^[A-Za-z]/ { print "  " $0; next }
')

TMP_FILE=$(mktemp)
cat > "$TMP_FILE" <<EOF
$PACKAGE_NAME ($VERSION-1) unstable; urgency=medium

$DEBIAN_ENTRIES

 -- $MAINTAINER  $(date -R)

EOF

cat "$TMP_FILE" "$DEBIAN_CHANGELOG" > "${DEBIAN_CHANGELOG}.new"
mv "${DEBIAN_CHANGELOG}.new" "$DEBIAN_CHANGELOG"

rm -f "$TMP_FILE"
echo "debian/changelog was automatically updated for version $VERSION."

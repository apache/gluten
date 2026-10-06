#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Verify the bundled gluten-velox jar's Arrow C-Data classes reference the
# *unshaded* Apache Arrow API — both in their method signatures and in their
# constant pools.
#
# Background: org.apache.arrow.c.* must NOT be relocated (its native JNI binds
# to the original class names), but it reaches into three other Arrow packages:
# org.apache.arrow.memory.*, org.apache.arrow.vector.* (public signatures) and
# org.apache.arrow.util.* (internal calls — Preconditions, AutoCloseables,
# Collections2). All three must stay unshaded in the bundle:
#
#   - a shaded *signature* type re-binds the bundled ArrowArrayStream/ArrowSchema
#     so any caller passing a vanilla Apache Arrow allocator hits
#     `NoSuchMethodError` (gluten#12225);
#   - a shaded *constant-pool* reference is worse when Arrow is no longer
#     bundled at all: the shaded target does not exist anywhere on the
#     classpath and the call site throws `ClassNotFoundException`.
#
# Usage:
#   dev/check-arrow-c-shading.sh <path-to-gluten-velox-bundle.jar> \
#     [shade-package-name] [arrow-deps-scope]
#
# The shade package name defaults to org.apache.gluten.shaded and is passed by
# package/pom.xml as ${gluten.shade.packageName}. Keep it parameterized: if this
# script hard-coded the prefix and the Maven property were ever changed, both
# checks below would silently match nothing and the whole guard would pass
# vacuously.
#
# The arrow-deps-scope is the value of the Maven property ${arrow.deps.scope}
# and drives the bundle-content assertion:
#   - `compile`   (Spark 3.x): gluten ships its own Arrow inside the bundle, so
#                              arrow-memory / arrow-vector classes MUST be
#                              present (the bundle is self-contained).
#   - `provided`  (Spark 4.x): Arrow is expected to come from the Spark
#                              distribution at runtime, so those packages MUST
#                              NOT be inside the bundle (otherwise the bundle
#                              has silently regressed to shipping its own copy).
# Any other value is not asserted against.
#
# Exit codes:
#   0 — bundle is well-shaded (Arrow C-Data API uses public Apache Arrow API)
#   1 — bundle is broken (Arrow C-Data references gluten-shaded types, OR
#       Arrow content does not match the declared arrow-deps-scope)
#   2 — usage / setup error

set -euo pipefail

JAR="${1:?usage: $0 <path-to-gluten-velox-bundle.jar> [shade-package-name] [arrow-deps-scope]}"
if [[ ! -f "$JAR" ]]; then
  echo "error: jar not found: $JAR" >&2
  exit 2
fi

# Dotted form for javap signatures, slashed form for JVM internal names in
# constant pools. `.` is escaped so the dotted form is a literal regex.
SHADE_PACKAGE="${2:-org.apache.gluten.shaded}"
SHADE_DOTS_RE="${SHADE_PACKAGE//./\\.}"
SHADE_SLASHES="${SHADE_PACKAGE//.//}"
ARROW_DEPS_SCOPE="${3:-}"

if ! command -v javap >/dev/null; then
  echo "error: javap not found on PATH" >&2
  exit 2
fi

WORKDIR=$(mktemp -d)
trap 'rm -rf "$WORKDIR"' EXIT

# Classes whose public API touches the unshaded boundary.
CLASSES=(
  "org/apache/arrow/c/ArrowArrayStream"
  "org/apache/arrow/c/ArrowSchema"
  "org/apache/arrow/c/ArrowArray"
  "org/apache/arrow/c/Data"
)

failures=0
# Track whether the bundle actually contains any org.apache.arrow.c.* class.
# When it doesn't, this jar is not the velox bundle (e.g. the intermediate
# jar-plugin output built without the data-lake profiles that would pull Arrow
# into the shade artifactSet), and the bundle-content assertion below has
# nothing to say about it. The two shading checks are already SKIP-safe in
# that case; the content assertion has to be too.
cdata_present=0
for cls in "${CLASSES[@]}"; do
  if ! unzip -p "$JAR" "${cls}.class" > "$WORKDIR/$(basename "$cls").class" 2>/dev/null; then
    echo "  SKIP $cls (not in bundle)"
    continue
  fi
  cdata_present=1
  signatures=$(javap -p "$WORKDIR/$(basename "$cls").class" 2>/dev/null || true)
  # Any method signature mentioning the shaded Arrow path is the bug.
  bad=$(echo "$signatures" | grep -E "${SHADE_DOTS_RE}\.org\.apache\.arrow\.(memory|vector)\." || true)
  if [[ -n "$bad" ]]; then
    echo "  FAIL $cls — public API references gluten-shaded Arrow types:"
    echo "$bad" | sed 's/^/    /'
    failures=$((failures + 1))
  else
    echo "  OK   $cls"
  fi
done

# Second check: no class under org/apache/arrow/c/ may *call* a shaded Arrow
# class. Signatures alone miss org.apache.arrow.util.Preconditions & friends,
# which are invoked from constructors but never appear in a descriptor.
#
# Both org.apache.arrow.c.* and org.apache.arrow.c.jni.* are excluded from
# relocation in package/pom.xml, so both are scanned. The jni subpackage is
# named explicitly rather than relying on `unzip` treating `c/*` as recursive —
# that is implementation-defined, and the existence check below would otherwise
# see an empty top level and skip the scan entirely.
#
# The name pattern covers every character legal in a JVM internal name after the
# package prefix: identifier chars (letters, digits, `_`, `$`), `/` for nested
# packages, and `-` for the synthetic `package-info` / `module-info` entries.
mkdir -p "$WORKDIR/all"
unzip -qo "$JAR" 'org/apache/arrow/c/*' 'org/apache/arrow/c/jni/*' \
  -d "$WORKDIR/all" 2>/dev/null || true
if compgen -G "$WORKDIR/all/org/apache/arrow/c/**/*.class" > /dev/null ||
   compgen -G "$WORKDIR/all/org/apache/arrow/c/*.class" > /dev/null; then
  cdata_present=1
  refs=$(grep -rahoE "${SHADE_SLASHES}/org/apache/arrow/[a-zA-Z0-9_$/-]+" \
    "$WORKDIR/all/org/apache/arrow/c" 2>/dev/null | sort -u || true)
  if [[ -n "$refs" ]]; then
    echo "  FAIL org/apache/arrow/c/** — calls into gluten-shaded Arrow:"
    echo "$refs" | sed 's/^/    /'
    failures=$((failures + 1))
  else
    echo "  OK   org/apache/arrow/c/** constant pools"
  fi
fi

# Third check: the bundle's Arrow content must match ${arrow.deps.scope}.
# This is the regression guard for #12737 — if any Arrow dependency ever slips
# from `provided`/`runtime` back to `compile` on a Spark 4.x profile, the memory
# and vector packages silently re-enter the bundle, undoing the size win and
# re-introducing the Spark-vs-gluten Arrow version conflict. Assert directly on
# the jar contents so the mistake fails the build instead of shipping.
#
# Gated on arrow-c-data being present. Whether Arrow lands in the jar at all is
# a function of the dependency closure and the shade artifactSet, not just of
# ${arrow.deps.scope}: `mvn install -Pspark-3.5 -Pbackends-velox` (no data-lake
# profiles) produces an intermediate jar with no Arrow whatsoever. arrow-c-data
# is bundled on every profile precisely because Spark never ships it, so its
# presence is the reliable marker for "this is the velox bundle". Without that
# gate, the `compile` branch below fires on jars that were never meant to carry
# Arrow at all.
if [[ -n "$ARROW_DEPS_SCOPE" && "$cdata_present" -eq 0 ]]; then
  echo "  SKIP bundle-content assertion (no org.apache.arrow.c.* in jar —"
  echo "       not a velox bundle, so its Arrow content is not asserted)"
elif [[ -n "$ARROW_DEPS_SCOPE" ]]; then
  # arrow-memory-core / arrow-vector classes, excluding the always-bundled
  # org.apache.arrow.c.* (arrow-c-data) which Spark never ships.
  arrow_impl=$(unzip -l "$JAR" 2>/dev/null \
    | grep -oE "org/apache/arrow/(memory|vector)/[^ ]*\.class" | sort -u || true)
  impl_count=$(printf '%s' "$arrow_impl" | grep -c . || true)
  case "$ARROW_DEPS_SCOPE" in
    provided)
      if [[ "$impl_count" -gt 0 ]]; then
        echo "  FAIL bundle content — arrow.deps.scope=provided but the bundle"
        echo "       still ships $impl_count arrow-memory/arrow-vector class(es):"
        printf '%s\n' "$arrow_impl" | head -5 | sed 's/^/    /'
        echo "    A dependency likely regressed to compile scope. Arrow must come"
        echo "    from the Spark distribution at runtime on Spark 4.x."
        failures=$((failures + 1))
      else
        echo "  OK   bundle carries no arrow-memory/arrow-vector (scope=provided)"
      fi
      ;;
    compile)
      if [[ "$impl_count" -eq 0 ]]; then
        echo "  FAIL bundle content — arrow.deps.scope=compile but the bundle"
        echo "       ships no arrow-memory/arrow-vector classes; the self-contained"
        echo "       bundle is incomplete and will fail to allocate Arrow buffers."
        failures=$((failures + 1))
      else
        echo "  OK   bundle carries arrow-memory/arrow-vector (scope=compile)"
      fi
      ;;
    *)
      echo "  SKIP bundle-content assertion (unrecognized arrow.deps.scope='$ARROW_DEPS_SCOPE')"
      ;;
  esac
fi

if (( failures > 0 )); then
  echo
  echo "Bundle has $failures Arrow shading/content problem(s)."
  echo "For shading failures, see gluten#12225 and update package/pom.xml's"
  echo "<relocation org.apache.arrow> excludes so every package reachable"
  echo "from org.apache.arrow.c stays unshaded (memory, vector, util)."
  echo "For content failures, see gluten#12737 and check each Arrow"
  echo "dependency's <scope> against \${arrow.deps.scope} for this profile."
  exit 1
fi

echo
echo "All Arrow C-Data classes use unshaded public Apache Arrow API. ✓"

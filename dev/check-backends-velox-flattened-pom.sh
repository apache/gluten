#!/usr/bin/env bash

# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

set -euo pipefail

readonly REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)"
cd "${REPO_ROOT}"

readonly SOURCE_POM="backends-velox/pom.xml"
readonly ROOT_POM="pom.xml"
readonly TARGET_DIR="backends-velox/target"
readonly FLATTENED_POM="${TARGET_DIR}/flattened-pom.xml"
readonly LOCK_DIR="${TARGET_DIR}/.flattened-pom-check.lock"
readonly OVERALL_START="${SECONDS}"
readonly REAL_BACKEND_REPO="${HOME:?HOME must be set}/.m2/repository/org/apache/gluten/backends-velox"

LOCK_HELD=0
VALIDATION_DIR=""
VALIDATION_PARENT=""
TMP_PARENT=""
ISOLATED_REPO=""
SOURCE_HASH_BEFORE=""
FLATTEN_STATE_CAPTURED=0
HAD_FLATTENED_POM=0
FINAL_CHECKS_DONE=0

hash_file() {
  python3 - "$1" <<'PY'
import hashlib
import sys

with open(sys.argv[1], "rb") as stream:
    digest = hashlib.sha256()
    for chunk in iter(lambda: stream.read(1024 * 1024), b""):
        digest.update(chunk)
print(digest.hexdigest())
PY
}

parent_relative_path() {
  python3 - "$1" "$2" <<'PY'
import os
import sys

target, start = map(os.path.abspath, sys.argv[1:])
print(os.path.relpath(target, start).replace(os.sep, "/"))
PY
}

self_test_parent_relative_path() {
  local fixture_parent
  local relative_path

  fixture_parent="${REPO_ROOT}/${TARGET_DIR}/flattened-pom-path-self-test/fixture"
  relative_path="$(parent_relative_path "${REPO_ROOT}/${ROOT_POM}" "${fixture_parent}")"
  python3 - "${REPO_ROOT}/${ROOT_POM}" "${fixture_parent}" "${relative_path}" <<'PY'
import ntpath
import os
import sys

root_pom, fixture_parent, relative_path = sys.argv[1:]
resolved = os.path.abspath(
    os.path.join(fixture_parent, relative_path.replace("/", os.sep))
)
if os.path.normcase(resolved) != os.path.normcase(os.path.abspath(root_pom)):
    raise AssertionError(
        "repository-local parent path resolves to {}, expected {}".format(
            resolved, os.path.abspath(root_pom)
        )
    )

try:
    ntpath.relpath(
        r"D:\repo\pom.xml",
        r"C:\Temp\gluten-flattened-pom-check\fixture",
    )
except ValueError as error:
    if "mount" not in str(error):
        raise
else:
    raise AssertionError("synthetic cross-drive relpath unexpectedly succeeded")

print(
    "PASS parent.relativePath self-test: repository-local fixture resolves to "
    "{}; synthetic C:-to-D: relpath raises ValueError".format(relative_path)
)
PY
}

if [[ "${1:-}" == "--self-test-paths" ]]; then
  if [[ "$#" -ne 1 ]]; then
    echo "Usage: $0 [--self-test-paths]" >&2
    exit 2
  fi
  self_test_parent_relative_path
  exit 0
elif [[ "$#" -ne 0 ]]; then
  echo "Usage: $0 [--self-test-paths]" >&2
  exit 2
fi

snapshot_tree() {
  python3 - "$1" "$2" "$3" "$4" "$5" <<'PY'
import hashlib
import json
import os
import stat
import sys

root, output, mode, action, excluded_root = sys.argv[1:]
excluded_root = (
    os.path.normcase(os.path.abspath(excluded_root)) if excluded_root else None
)


def included(relative_path):
    if mode == "all":
        return True
    parts = relative_path.split(os.sep)
    name = parts[-1]
    return (
        name.endswith((".jar", ".class"))
        or "test-classes" in parts
        or "surefire-reports" in parts
        or "scalatest-reports" in parts
    )


def collect():
    result = {}
    if not os.path.isdir(root):
        return result
    for directory, names, files in os.walk(root, followlinks=False):
        if (
            excluded_root is not None
            and os.path.normcase(os.path.abspath(directory)) == excluded_root
        ):
            names[:] = []
            continue
        for name in files:
            path = os.path.join(directory, name)
            relative_path = os.path.relpath(path, root)
            if not included(relative_path):
                continue
            metadata = os.lstat(path)
            if stat.S_ISLNK(metadata.st_mode):
                value = ["symlink", os.readlink(path), metadata.st_mtime_ns]
            elif stat.S_ISREG(metadata.st_mode):
                digest = hashlib.sha256()
                with open(path, "rb") as stream:
                    for chunk in iter(lambda: stream.read(1024 * 1024), b""):
                        digest.update(chunk)
                value = [
                    "file",
                    metadata.st_size,
                    metadata.st_mode,
                    metadata.st_mtime_ns,
                    digest.hexdigest(),
                ]
            else:
                continue
            result[relative_path] = value
    return result


current = collect()
if action == "record":
    with open(output, "w", encoding="utf-8") as stream:
        json.dump(current, stream, sort_keys=True)
    aggregate = hashlib.sha256(
        json.dumps(current, sort_keys=True).encode("utf-8")
    ).hexdigest()
    print("Recorded {} snapshot: {} files, sha256={}".format(mode, len(current), aggregate))
else:
    with open(output, encoding="utf-8") as stream:
        expected = json.load(stream)
    if current != expected:
        expected_keys = set(expected)
        current_keys = set(current)
        details = []
        details.extend("added {}".format(path) for path in sorted(current_keys - expected_keys))
        details.extend("removed {}".format(path) for path in sorted(expected_keys - current_keys))
        details.extend(
            "changed {}".format(path)
            for path in sorted(expected_keys & current_keys)
            if expected[path] != current[path]
        )
        raise AssertionError(
            "{} snapshot changed: {}".format(mode, "; ".join(details[:20]))
        )
    print("Preserved {} snapshot: {} files unchanged".format(mode, len(current)))
PY
}

verify_safety_state() {
  local status=0
  local current_hash

  if [[ -n "${SOURCE_HASH_BEFORE}" ]]; then
    current_hash="$(hash_file "${SOURCE_POM}")" || status=1
    if [[ "${current_hash}" != "${SOURCE_HASH_BEFORE}" ]]; then
      echo "ERROR: source POM hash changed: ${SOURCE_POM}" >&2
      status=1
    else
      echo "Preserved source POM sha256=${SOURCE_HASH_BEFORE}"
    fi
  fi
  if [[ -n "${VALIDATION_DIR}" && -f "${VALIDATION_DIR}/target-artifacts.json" ]]; then
    snapshot_tree "${TARGET_DIR}" "${VALIDATION_DIR}/target-artifacts.json" \
      target compare "${VALIDATION_DIR}" || status=1
  fi
  if [[ -n "${VALIDATION_DIR}" && -f "${VALIDATION_DIR}/real-m2-backend.json" ]]; then
    snapshot_tree "${REAL_BACKEND_REPO}" "${VALIDATION_DIR}/real-m2-backend.json" \
      all compare "" || status=1
  fi
  return "${status}"
}

cleanup() {
  local status=$?
  local validation_cleanup_safe=1
  trap - EXIT HUP INT TERM
  set +e

  if [[ "${FINAL_CHECKS_DONE}" -eq 0 ]]; then
    verify_safety_state || status=1
  fi

  if [[ "${FLATTEN_STATE_CAPTURED}" -eq 1 ]]; then
    if [[ "${HAD_FLATTENED_POM}" -eq 1 ]]; then
      cp -p -- "${VALIDATION_DIR}/preexisting-flattened-pom.xml" "${FLATTENED_POM}" || {
        status=1
        validation_cleanup_safe=0
      }
    else
      rm -f -- "${FLATTENED_POM}" || status=1
    fi
  fi

  if [[ -n "${VALIDATION_DIR}" ]]; then
    case "${VALIDATION_DIR}" in
      "${VALIDATION_PARENT}"/flattened-pom-check.??????)
        if [[ "$(dirname "${VALIDATION_DIR}")" == "${VALIDATION_PARENT}" ]]; then
          if [[ "${validation_cleanup_safe}" -eq 1 ]]; then
            rm -rf -- "${VALIDATION_DIR}" || status=1
          else
            echo "ERROR: retained validation directory containing backup: ${VALIDATION_DIR}" >&2
          fi
        else
          echo "ERROR: refusing cleanup outside ${VALIDATION_PARENT}: ${VALIDATION_DIR}" >&2
          status=1
        fi
        ;;
      *)
        echo "ERROR: refusing unsafe validation cleanup: ${VALIDATION_DIR}" >&2
        status=1
        ;;
    esac
  fi

  if [[ -n "${ISOLATED_REPO}" ]]; then
    case "${ISOLATED_REPO}" in
      "${TMP_PARENT}"/gluten-flattened-pom-m2.??????)
        if [[ "$(dirname "${ISOLATED_REPO}")" == "${TMP_PARENT}" ]]; then
          rm -rf -- "${ISOLATED_REPO}" || status=1
        else
          echo "ERROR: refusing Maven repository cleanup outside ${TMP_PARENT}: ${ISOLATED_REPO}" >&2
          status=1
        fi
        ;;
      *)
        echo "ERROR: refusing unsafe Maven repository cleanup: ${ISOLATED_REPO}" >&2
        status=1
        ;;
    esac
  fi

  if [[ "${LOCK_HELD}" -eq 1 ]]; then
    rmdir -- "${LOCK_DIR}" || {
      echo "ERROR: could not remove owned lock ${LOCK_DIR}" >&2
      status=1
    }
  fi
  exit "${status}"
}

mkdir -p -- "${TARGET_DIR}"
if ! mkdir -- "${LOCK_DIR}"; then
  echo "ERROR: another flattened POM validation owns lock ${LOCK_DIR}" >&2
  exit 1
fi
LOCK_HELD=1
trap cleanup EXIT
trap 'exit 129' HUP
trap 'exit 130' INT
trap 'exit 143' TERM

VALIDATION_PARENT="$(cd "${TARGET_DIR}" && pwd -P)"
VALIDATION_DIR="$(mktemp -d "${VALIDATION_PARENT}/flattened-pom-check.XXXXXX")"
TMP_PARENT="$(cd "${TMPDIR:-/tmp}" && pwd -P)"
ISOLATED_REPO="$(mktemp -d "${TMP_PARENT}/gluten-flattened-pom-m2.XXXXXX")"
readonly VALIDATION_PARENT VALIDATION_DIR TMP_PARENT ISOLATED_REPO
readonly PROFILE_POMS="${VALIDATION_DIR}/profile-poms"
readonly FIXTURE_DIR="${VALIDATION_DIR}/fixture"
readonly FIXTURE_POM="${FIXTURE_DIR}/pom.xml"
readonly FIXTURE_FLATTENED_POM="${FIXTURE_DIR}/target/flattened-pom.xml"
readonly MAVEN_SETTINGS="${VALIDATION_DIR}/settings.xml"
mkdir -p -- "${ISOLATED_REPO}" "${PROFILE_POMS}" "${FIXTURE_DIR}"
python3 - "${HOME}/.m2/repository" "${MAVEN_SETTINGS}" <<'PY'
import pathlib
import sys

cache, output = sys.argv[1:]
uri = pathlib.Path(cache).resolve().as_uri()
template = """<settings xmlns="http://maven.apache.org/SETTINGS/1.0.0">
  <profiles>
    <profile>
      <id>read-only-local-cache</id>
      <repositories>
        <repository><id>read-only-local-cache</id><url>{0}</url><releases><enabled>true</enabled><updatePolicy>never</updatePolicy></releases><snapshots><enabled>true</enabled><updatePolicy>never</updatePolicy></snapshots></repository>
      </repositories>
      <pluginRepositories>
        <pluginRepository><id>read-only-local-cache-plugins</id><url>{0}</url><releases><enabled>true</enabled><updatePolicy>never</updatePolicy></releases><snapshots><enabled>true</enabled><updatePolicy>never</updatePolicy></snapshots></pluginRepository>
      </pluginRepositories>
    </profile>
  </profiles>
  <activeProfiles><activeProfile>read-only-local-cache</activeProfile></activeProfiles>
</settings>
""".format(uri)
pathlib.Path(output).write_text(template, encoding="utf-8")
PY
echo "Validation directory: ${VALIDATION_DIR}"
echo "Isolated Maven repository: ${ISOLATED_REPO} (real cache is read-only input)"

SOURCE_HASH_BEFORE="$(hash_file "${SOURCE_POM}")"
readonly SOURCE_HASH_BEFORE
snapshot_tree "${TARGET_DIR}" "${VALIDATION_DIR}/target-artifacts.json" \
  target record "${VALIDATION_DIR}"
snapshot_tree "${REAL_BACKEND_REPO}" "${VALIDATION_DIR}/real-m2-backend.json" \
  all record ""
if [[ -f "${FLATTENED_POM}" ]]; then
  cp -p -- "${FLATTENED_POM}" "${VALIDATION_DIR}/preexisting-flattened-pom.xml"
  HAD_FLATTENED_POM=1
fi
FLATTEN_STATE_CAPTURED=1

run_maven() {
  local label="$1"
  shift
  local start="${SECONDS}"

  echo "Running ${label}: ./build/mvn --no-transfer-progress -s ${MAVEN_SETTINGS} -Dmaven.repo.local=${ISOLATED_REPO} $*"
  ./build/mvn --no-transfer-progress -s "${MAVEN_SETTINGS}" "-Dmaven.repo.local=${ISOLATED_REPO}" "$@"
  echo "Completed ${label} in $((SECONDS - start))s"
}

seed_project_artifacts() {
  python3 - "${HOME}/.m2/repository/org/apache/gluten" \
    "${ISOLATED_REPO}/org/apache/gluten" <<'PY'
import os
import shutil
import sys

source, destination = map(os.path.abspath, sys.argv[1:])
if not os.path.isdir(source):
    raise AssertionError("real local repository has no Gluten prerequisites: {}".format(source))
os.makedirs(destination, exist_ok=True)
artifacts = []
for name in sorted(os.listdir(source)):
    if name == "backends-velox":
        continue
    source_path = os.path.join(source, name)
    if not os.path.isdir(source_path):
        continue
    shutil.copytree(source_path, os.path.join(destination, name))
    artifacts.append(name)
for required in ("gluten-parent", "gluten-substrait", "gluten-arrow"):
    if required not in artifacts:
        raise AssertionError("isolated repository seed missing {}".format(required))
print(
    "Seeded isolated repository with {} prerequisite Gluten artifacts "
    "(backends-velox excluded)".format(len(artifacts))
)
PY
}

validate_source_poms() {
  python3 - "${ROOT_POM}" "${SOURCE_POM}" <<'PY'
import collections
import sys
import xml.etree.ElementTree as ET

root_path, backend_path = sys.argv[1:]
namespace = {"m": "http://maven.apache.org/POM/4.0.0"}
root = ET.parse(root_path).getroot()
backend = ET.parse(backend_path).getroot()


def text(element, path, default=None):
    child = element.find(path, namespace)
    if child is None or child.text is None:
        return default
    return child.text.strip()


def require(actual, expected, field):
    if actual != expected:
        raise AssertionError("source {}: expected {!r}, found {!r}".format(field, expected, actual))


require(text(backend, "m:packaging"), "jar", "packaging")
root_coordinates = (
    text(root, "m:groupId"),
    text(root, "m:artifactId"),
    text(root, "m:version"),
)
backend_parent = (
    text(backend, "m:parent/m:groupId"),
    text(backend, "m:parent/m:artifactId"),
    text(backend, "m:parent/m:version"),
)
require(backend_parent, root_coordinates, "parent coordinates")

plugins = [
    plugin
    for plugin in backend.findall("m:build/m:plugins/m:plugin", namespace)
    if text(plugin, "m:groupId") == "org.codehaus.mojo"
    and text(plugin, "m:artifactId") == "flatten-maven-plugin"
]
require(len(plugins), 1, "flatten plugin declaration count")

dependencies = backend.findall("m:dependencies/m:dependency", namespace)
arrow = {}
for dependency in dependencies:
    if text(dependency, "m:groupId") == "org.apache.arrow":
        artifact = text(dependency, "m:artifactId")
        if artifact in arrow:
            raise AssertionError("source Arrow artifact duplicated: {}".format(artifact))
        arrow[artifact] = (
            text(dependency, "m:version"),
            text(dependency, "m:scope"),
        )
require(
    arrow,
    {
        "${arrow-memory.artifact}": ("${arrow.version}", "${arrow-memory.scope}"),
        "arrow-memory-core": ("${arrow.version}", "${arrow.deps.scope}"),
        "arrow-vector": ("${arrow.version}", "${arrow.deps.scope}"),
    },
    "Arrow dependency declarations",
)

spark_main = []
for dependency in dependencies:
    if text(dependency, "m:groupId") != "org.apache.spark":
        continue
    if text(dependency, "m:type", "jar") != "jar" or text(dependency, "m:scope") == "test":
        continue
    spark_main.append((text(dependency, "m:artifactId"), text(dependency, "m:scope")))
require(
    collections.Counter(spark_main),
    collections.Counter(
        (artifact + "_${scala.binary.version}", "provided")
        for artifact in ("spark-core", "spark-catalyst", "spark-network-common", "spark-hive")
    ),
    "Spark dependency artifact/scope declarations",
)

profiles = collections.defaultdict(list)
for profile in root.findall("m:profiles/m:profile", namespace):
    profiles[text(profile, "m:id")].append(profile)
expected_spark = {
    "spark-3.3": "3.3.1",
    "spark-3.4": "3.4.4",
    "spark-3.5": "3.5.5",
    "spark-4.0": "4.0.2",
    "spark-4.1": "4.1.1",
}
for profile_id, version in expected_spark.items():
    require(len(profiles[profile_id]), 1, "{} profile count".format(profile_id))
    require(
        text(profiles[profile_id][0], "m:properties/m:spark.version"),
        version,
        "{} spark.version".format(profile_id),
    )
require(text(root, "m:properties/m:scala.binary.version"), "2.12", "default Scala suffix")
require(len(profiles["scala-2.13"]), 1, "scala-2.13 profile count")
require(
    text(profiles["scala-2.13"][0], "m:properties/m:scala.binary.version"),
    "2.13",
    "scala-2.13 suffix",
)
require(text(root, "m:properties/m:arrow.version"), "15.0.0", "Spark 3 Arrow version")
require(text(root, "m:properties/m:arrow.deps.scope"), "compile", "Spark 3 Arrow core/vector scope")
require(text(root, "m:properties/m:arrow-memory.scope"), "runtime", "Spark 3 Arrow unsafe scope")
for profile_id, version in (("spark-4.0", "18.1.0"), ("spark-4.1", "18.3.0")):
    profile = profiles[profile_id][0]
    require(text(profile, "m:properties/m:arrow.version"), version, "{} Arrow version".format(profile_id))
    require(text(profile, "m:properties/m:arrow.deps.scope"), "provided", "{} Arrow core/vector scope".format(profile_id))
    require(text(profile, "m:properties/m:arrow-memory.scope"), "provided", "{} Arrow unsafe scope".format(profile_id))
print("Validated source POM: five Spark profiles, Arrow declarations, and flatten plugin")
PY
}

create_fixture() {
  local fixture_parent_relative_path

  fixture_parent_relative_path="$(
    parent_relative_path "${ROOT_POM}" "$(dirname "${FIXTURE_POM}")"
  )"
  python3 - "${SOURCE_POM}" "${ROOT_POM}" "${FIXTURE_POM}" \
    "${fixture_parent_relative_path}" <<'PY'
import copy
import os
import sys
import xml.etree.ElementTree as ET

source_path, root_pom_path, fixture_path = map(os.path.abspath, sys.argv[1:4])
parent_relative_path = sys.argv[4]
namespace_uri = "http://maven.apache.org/POM/4.0.0"
namespace = {"m": namespace_uri}
ET.register_namespace("", namespace_uri)
ET.register_namespace("xsi", "http://www.w3.org/2001/XMLSchema-instance")


def child(element, name):
    value = element.find("m:" + name, namespace)
    if value is None:
        raise AssertionError("fixture source missing {}".format(name))
    return value


def transform(root):
    packaging = child(root, "packaging")
    if packaging.text.strip() != "jar":
        raise AssertionError("fixture source packaging: expected jar, found {}".format(packaging.text))
    packaging.text = "pom"
    parent = child(root, "parent")
    relative_path = parent.find("m:relativePath", namespace)
    if relative_path is None:
        relative_path = ET.SubElement(parent, "{{{}}}relativePath".format(namespace_uri))
    relative_path.text = parent_relative_path


def semantic(element):
    tag = element.tag.partition("}")[2] or element.tag
    return (
        tag,
        tuple(sorted(element.attrib.items())),
        (element.text or "").strip(),
        tuple(semantic(item) for item in element),
    )


source_root = ET.parse(source_path).getroot()
fixture_root = copy.deepcopy(source_root)
transform(fixture_root)
os.makedirs(os.path.dirname(fixture_path), exist_ok=True)
ET.ElementTree(fixture_root).write(fixture_path, encoding="utf-8", xml_declaration=True)

expected_root = copy.deepcopy(source_root)
transform(expected_root)
actual_root = ET.parse(fixture_path).getroot()
if semantic(expected_root) != semantic(actual_root):
    raise AssertionError("fixture transformation changed XML outside parent.relativePath and packaging")
relative_path = actual_root.find("m:parent/m:relativePath", namespace).text.strip()
resolved_parent = os.path.abspath(os.path.join(os.path.dirname(fixture_path), relative_path))
if resolved_parent != root_pom_path:
    raise AssertionError(
        "fixture parent.relativePath resolves to {}, expected {}".format(resolved_parent, root_pom_path)
    )
source_dependencies = source_root.find("m:dependencies", namespace)
fixture_dependencies = actual_root.find("m:dependencies", namespace)
source_plugins = source_root.find("m:build/m:plugins", namespace)
fixture_plugins = actual_root.find("m:build/m:plugins", namespace)
if semantic(source_dependencies) != semantic(fixture_dependencies):
    raise AssertionError("fixture dependency XML differs from source")
if semantic(source_plugins) != semantic(fixture_plugins):
    raise AssertionError("fixture plugin XML differs from source")
print(
    "Validated fixture transformation: packaging and parent.relativePath only; "
    "{} dependencies and {} plugins preserved".format(
        len(list(fixture_dependencies)), len(list(fixture_plugins))
    )
)
PY
}

validate_profile_pom() {
  local pom="$1"
  local profile="$2"
  local spark_version="$3"
  local scala_suffix="$4"
  local arrow_version="$5"
  local unsafe_scope="$6"
  local core_vector_scope="$7"

  python3 - "${pom}" "${profile}" "${spark_version}" "${scala_suffix}" \
    "${arrow_version}" "${unsafe_scope}" "${core_vector_scope}" <<'PY'
import collections
import sys
import xml.etree.ElementTree as ET

pom_path, profile, spark_version, scala_suffix, arrow_version, unsafe_scope, core_scope = sys.argv[1:]
with open(pom_path, encoding="utf-8") as stream:
    raw = stream.read()
if "${arrow" in raw:
    raise AssertionError("{} Arrow placeholder remained in {}".format(profile, pom_path))

root = ET.fromstring(raw)
namespace_uri = root.tag.partition("}")[0]
prefix = "{" + namespace_uri[1:] + "}" if namespace_uri.startswith("{") else ""


def text(element, name, default=None):
    child = element.find(prefix + name)
    if child is None or child.text is None:
        return default
    return child.text.strip()


def require(actual, expected, field):
    if actual != expected:
        raise AssertionError("{} {}: expected {!r}, found {!r}".format(profile, field, expected, actual))


dependencies_node = root.find(prefix + "dependencies")
if dependencies_node is None:
    raise AssertionError("{} dependencies: direct dependencies are absent".format(profile))
dependencies = dependencies_node.findall(prefix + "dependency")

arrow = {}
for dependency in dependencies:
    if text(dependency, "groupId") != "org.apache.arrow":
        continue
    artifact = text(dependency, "artifactId")
    version = text(dependency, "version")
    scope = text(dependency, "scope", "<missing>")
    if artifact in arrow:
        raise AssertionError("{} Arrow artifact duplicated: {}".format(profile, artifact))
    arrow[artifact] = (version, scope)
require(
    arrow,
    {
        "arrow-memory-unsafe": (arrow_version, unsafe_scope),
        "arrow-memory-core": (arrow_version, core_scope),
        "arrow-vector": (arrow_version, core_scope),
    },
    "Arrow version/scope values",
)

spark = {}
for dependency in dependencies:
    if text(dependency, "groupId") != "org.apache.spark":
        continue
    artifact = text(dependency, "artifactId")
    coordinate = (text(dependency, "version"), text(dependency, "scope"), text(dependency, "type", "jar"))
    if artifact in spark:
        raise AssertionError("{} Spark artifact duplicated: {}".format(profile, artifact))
    spark[artifact] = coordinate
expected_spark = {
    artifact + "_" + scala_suffix: (spark_version, "provided", "jar")
    for artifact in ("spark-core", "spark-catalyst", "spark-network-common", "spark-hive")
}
require(spark, expected_spark, "Spark dependency coordinates")
print(
    "PASS {} activation: Spark {} (_{}) and Arrow {} ({}/{})".format(
        profile, spark_version, scala_suffix, arrow_version, unsafe_scope, core_scope
    )
)
PY
}

find_installed_pom() {
  python3 - "${FIXTURE_FLATTENED_POM}" "${ISOLATED_REPO}" <<'PY'
import os
import sys
import xml.etree.ElementTree as ET

pom_path, repository = map(os.path.abspath, sys.argv[1:])
root = ET.parse(pom_path).getroot()
namespace_uri = root.tag.partition("}")[0]
prefix = "{" + namespace_uri[1:] + "}" if namespace_uri.startswith("{") else ""


def value(name):
    element = root.find(prefix + name)
    if element is None or element.text is None:
        raise AssertionError("fixture flattened POM missing {}".format(name))
    result = element.text.strip()
    if "${" in result:
        raise AssertionError("fixture flattened POM {} is unresolved: {}".format(name, result))
    return result


group_id = value("groupId")
artifact_id = value("artifactId")
version = value("version")
version_dir = os.path.join(repository, group_id.replace(".", os.sep), artifact_id, version)
expected = os.path.join(version_dir, "{}-{}.pom".format(artifact_id, version))
if not os.path.isfile(expected):
    raise AssertionError("installed fixture POM missing from exact path {}".format(expected))
poms = sorted(
    os.path.join(version_dir, name)
    for name in os.listdir(version_dir)
    if name.endswith(".pom")
)
if poms != [expected]:
    raise AssertionError("installed fixture POM path is not unique: {}".format(poms))
main_jar = os.path.join(version_dir, "{}-{}.jar".format(artifact_id, version))
if os.path.exists(main_jar):
    raise AssertionError("POM-packaging fixture installed a main binary artifact: {}".format(main_jar))
print(expected)
PY
}

validate_source_poms
create_fixture
seed_project_artifacts

for profile_data in \
  "3.3 3.3.1 2.12 15.0.0 runtime compile" \
  "3.4 3.4.4 2.12 15.0.0 runtime compile" \
  "3.5 3.5.5 2.12 15.0.0 runtime compile" \
  "4.0 4.0.2 2.13 18.1.0 provided provided" \
  "4.1 4.1.1 2.13 18.3.0 provided provided"
do
  set -- ${profile_data}
  spark_profile="$1"
  spark_version="$2"
  scala_suffix="$3"
  arrow_version="$4"
  unsafe_scope="$5"
  core_vector_scope="$6"

  rm -f -- "${FLATTENED_POM}"
  maven_args=(
    process-resources
    -pl backends-velox
    -Pbackends-velox
    "-Pspark-${spark_profile}"
    -Dmaven.main.skip=true
    -Dmaven.resources.skip=true
    -DskipTests
    -Dprotoc.skip=true
    -Dspotless.check.skip=true
  )
  if [[ "${spark_profile}" == 4.* ]]; then
    maven_args+=(
      -Pjava-17
      -Pscala-2.13
      -Dmaven.compiler.release=17
    )
  else
    maven_args+=(
      -Pscala-2.12
    )
  fi

  run_maven "Spark ${spark_profile} process-resources" "${maven_args[@]}"
  if [[ ! -f "${FLATTENED_POM}" ]]; then
    echo "ERROR: spark-${spark_profile} flattened POM was not generated: ${FLATTENED_POM}" >&2
    exit 1
  fi
  saved_pom="${PROFILE_POMS}/spark-${spark_profile}-flattened-pom.xml"
  cp -- "${FLATTENED_POM}" "${saved_pom}"
  validate_profile_pom "${saved_pom}" "spark-${spark_profile}" "${spark_version}" \
    "${scala_suffix}" "${arrow_version}" "${unsafe_scope}" "${core_vector_scope}"
done

run_maven "Spark 4.1 POM-packaging fixture install" \
  -f "${FIXTURE_POM}" \
  install \
  -Pbackends-velox \
  -Pspark-4.1 \
  -Pjava-17 \
  -Pscala-2.13 \
  -Dmaven.compiler.release=17 \
  -Dmaven.main.skip=true \
  -Dmaven.resources.skip=true \
  -Dmaven.test.skip=true \
  -Dmaven.source.skip=true \
  -DskipTests \
  -Dprotoc.skip=true \
  -Dspotless.check.skip=true \
  -DupdatePomFile=true

if [[ ! -f "${FIXTURE_FLATTENED_POM}" ]]; then
  echo "ERROR: fixture lifecycle did not generate ${FIXTURE_FLATTENED_POM}" >&2
  exit 1
fi
validate_profile_pom "${FIXTURE_FLATTENED_POM}" "fixture spark-4.1" \
  "4.1.1" "2.13" "18.3.0" "provided" "provided"
INSTALLED_POM="$(find_installed_pom)"
readonly INSTALLED_POM
echo "Proved unique installed POM path: ${INSTALLED_POM}"
validate_profile_pom "${INSTALLED_POM}" "installed fixture spark-4.1" \
  "4.1.1" "2.13" "18.3.0" "provided" "provided"
if ! cmp -s -- "${FIXTURE_FLATTENED_POM}" "${INSTALLED_POM}"; then
  echo "ERROR: installed fixture POM is not byte-equal to lifecycle flattened POM" >&2
  exit 1
fi
echo "PASS fixture install: lifecycle and installed POMs are byte-equal"

verify_safety_state
FINAL_CHECKS_DONE=1
echo "All five profiles and fixture lifecycle install passed in $((SECONDS - OVERALL_START))s"
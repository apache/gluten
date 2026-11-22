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

import copy
import os
from pathlib import Path
import re
import shutil
import subprocess
import tarfile
import tempfile
import unittest
import xml.etree.ElementTree as ET
import zipfile


ROOT = Path(__file__).resolve().parents[4]
NS = {"m": "http://maven.apache.org/POM/4.0.0"}
ET.register_namespace("", NS["m"])


def element(parent, name, text=None):
    child = ET.SubElement(parent, f"{{{NS['m']}}}{name}")
    child.text = text
    return child


class BinaryLicensesTest(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        for name in ("LICENSE-binary", "NOTICE-binary"):
            shutil.copyfile(ROOT / name, self.root / name)
        shutil.copytree(ROOT / "licenses-binary", self.root / "licenses-binary")

    def assert_license_references(self, license_text, read):
        references = re.findall(r"\((licenses-binary/[^)]+)\)", license_text)
        self.assertTrue(references, "Binary LICENSE must point to its license texts")
        for name in references:
            self.assertEqual(read(name), (ROOT / name).read_bytes())
        checker = read("licenses-binary/LICENSE-checker-qual.txt").decode()
        self.assertIn("Copyright 2004-present by the Checker Framework developers", checker)
        self.assertIn("this permission notice shall be included", checker)
        slf4j = read("licenses-binary/LICENSE-slf4j.txt").decode()
        self.assertIn("Copyright (c) 2004-2022 QOS.ch", slf4j)
        self.assertIn("this permission notice shall be", slf4j)

    def test_shaded_jar_retains_binary_licenses(self):
        production = ET.parse(ROOT / "package/pom.xml").getroot()
        parent = ET.parse(ROOT / "pom.xml").getroot()
        shade = production.find(
            "m:build/m:plugins/m:plugin[m:artifactId='maven-shade-plugin']", NS
        )
        shade_version = parent.find(
            "m:build/m:pluginManagement/m:plugins/"
            "m:plugin[m:artifactId='maven-shade-plugin']/m:version", NS
        ).text
        for bundle_slf4j in (True, False):
            with self.subTest(bundle_slf4j=bundle_slf4j):
                directory = self.root / ("with-slf4j" if bundle_slf4j else "without-slf4j")
                directory.mkdir()
                project = ET.Element(f"{{{NS['m']}}}project")
                for key, value in (
                    ("modelVersion", "4.0.0"),
                    ("groupId", "org.apache.gluten"),
                    ("artifactId", "license-packaging-test"),
                    ("version", "1.0"),
                ):
                    element(project, key, value)
                properties = element(project, "properties")
                element(properties, "gluten.shade.packageName", "org.apache.gluten.shaded")
                dependencies = element(project, "dependencies")
                for group, artifact, version, scope in (
                    ("org.checkerframework", "checker-qual", "3.33.0", "compile"),
                    ("org.slf4j", "slf4j-api", "2.0.6",
                     "compile" if bundle_slf4j else "provided"),
                ):
                    dependency = element(dependencies, "dependency")
                    for key, value in (
                        ("groupId", group), ("artifactId", artifact),
                        ("version", version), ("scope", scope),
                    ):
                        element(dependency, key, value)
                build = element(project, "build")
                resources = copy.deepcopy(production.find("m:build/m:resources", NS))
                build.append(resources)
                conflicting = directory / "input-resources/META-INF"
                conflicting.mkdir(parents=True)
                for name in ("LICENSE", "NOTICE"):
                    (conflicting / name).write_text("upstream Apache-only metadata\n")
                resource = element(resources, "resource")
                element(resource, "directory", str(conflicting.parent))
                plugins = element(build, "plugins")
                plugin = copy.deepcopy(shade)
                element(plugin, "version", shade_version)
                plugins.append(plugin)
                ET.ElementTree(project).write(directory / "pom.xml", encoding="utf-8")
                command = [str(ROOT / "build/mvn"), "-B", "-q"]
                settings = os.environ.get("MAVEN_SETTINGS")
                if settings:
                    command.extend(["-s", settings])
                command.extend(["-f", str(directory / "pom.xml"), "package"])
                result = subprocess.run(
                    command, cwd=ROOT, text=True, stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT, timeout=300,
                )
                self.assertEqual(result.returncode, 0, result.stdout)
                with zipfile.ZipFile(directory / "target/gluten-package-1.0.jar") as jar:
                    self.assertIn(
                        "org/checkerframework/checker/builder/qual/CalledMethods.class",
                        jar.namelist(),
                    )
                    self.assertEqual(
                        "org/slf4j/Logger.class" in jar.namelist(), bundle_slf4j
                    )
                    self.assertEqual(
                        len(jar.namelist()), len(set(jar.namelist())),
                        "Shading must not create duplicate license entries",
                    )
                    self.assertEqual(
                        jar.read("META-INF/LICENSE"), (ROOT / "LICENSE-binary").read_bytes()
                    )
                    self.assertEqual(
                        jar.read("META-INF/NOTICE"), (ROOT / "NOTICE-binary").read_bytes()
                    )
                    self.assert_license_references(
                        jar.read("META-INF/LICENSE").decode(),
                        lambda name: jar.read("META-INF/" + name),
                    )

    def test_release_fails_when_license_directory_is_missing(self):
        script_directory = self.root / "dev/release"
        script_directory.mkdir(parents=True)
        script = script_directory / "package-release.sh"
        shutil.copyfile(ROOT / "dev/release/package-release.sh", script)
        (self.root / "release").mkdir()
        (self.root / "licenses-binary").rename(self.root / "missing-licenses")
        result = subprocess.run(
            ["bash", str(script), "v1.0-rc0"], text=True,
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=30,
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("licenses-binary, required for the binary distribution", result.stdout)

    def test_release_tarballs_include_referenced_licenses(self):
        script_directory = self.root / "dev/release"
        script_directory.mkdir(parents=True)
        script = script_directory / "package-release.sh"
        shutil.copyfile(ROOT / "dev/release/package-release.sh", script)
        release = self.root / "release"
        release.mkdir()
        script_text = script.read_text()
        versions = re.search(r'SPARK_VERSIONS="([^"]+)"', script_text).group(1).split()
        for version in versions:
            scala = "2.13" if version.startswith("4.") else "2.12"
            jar = release / f"gluten-velox-bundle-spark{version}_{scala}-linux_amd64-1.0.jar"
            with zipfile.ZipFile(jar, "w"):
                pass
        source_zip = self.root / "source.zip"
        with zipfile.ZipFile(source_zip, "w") as archive:
            archive.writestr("gluten-1.0-rc0/README", "release packaging fixture\n")
        commands = self.root / "bin"
        commands.mkdir()
        wget = commands / "wget"
        wget.write_text('#!/bin/sh\ncp "$SOURCE_ZIP" "$(basename "$1")"\n')
        wget.chmod(0o755)
        environment = dict(
            os.environ, PATH=str(commands) + os.pathsep + os.environ["PATH"],
            SOURCE_ZIP=str(source_zip),
        )
        result = subprocess.run(
            ["bash", str(script), "v1.0-rc0"], env=environment,
            text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=60,
        )
        self.assertEqual(result.returncode, 0, result.stdout)
        for version in versions:
            with self.subTest(spark=version):
                prefix = f"apache-gluten-1.0-bin-spark-{version}/"
                with tarfile.open(release / f"{prefix[:-1]}.tar.gz") as archive:
                    self.assert_license_references(
                        archive.extractfile(prefix + "LICENSE").read().decode(),
                        lambda name: archive.extractfile(prefix + name).read(),
                    )


if __name__ == "__main__":
    unittest.main()

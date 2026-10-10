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

"""Verify non-vcpkg Kafka package setup without installing system packages."""

import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile
import unittest

SETUP_DIR = Path(__file__).resolve().parents[2] / "ep/build-velox/src"


class KafkaSetupTest(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)
        self.work_dir = Path(self.temp_dir.name)
        (self.work_dir / "scripts").mkdir()
        self.package_log = self.work_dir / "packages.log"
        self.package_log.touch()

    def run_shell(self, commands):
        # Run the real sed with native in-place syntax, so both Linux and macOS
        # setup transformations can be exercised on either development host.
        inplace_args = "-i ''" if sys.platform == "darwin" else "-i"
        prelude = """
set -eu
SUDO=""
sed() {
  if [ "$1" = "-i" ]; then
    shift
    if [ "$1" = "" ]; then shift; fi
    command sed INPLACE_ARGS "$@"
  else
    command sed "$@"
  fi
}
apt() { printf '%s\\n' "$@" >> "$PACKAGE_LOG"; }
dnf_install() { apt "$@"; }
brew() { apt "$@"; }
""".replace("INPLACE_ARGS", inplace_args)
        result = subprocess.run(
            ["bash", "-c", prelude + commands],
            cwd=self.work_dir,
            env={**os.environ, "PACKAGE_LOG": str(self.package_log)},
            capture_output=True,
            text=True,
        )
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

    def check_upstream_setup(self, platform, fixture, package, existing_package):
        setup_file = self.work_dir / "scripts" / f"setup-{platform}.sh"
        setup_file.write_text(fixture)
        (self.work_dir / "scripts/setup-common.sh").write_text(
            "function install_folly {\n  local FOLLY_FLAGS=()\n}\n"
        )
        function_name = f"process_setup_{platform}"
        # Extract only the setup function; sourcing get-velox.sh itself would
        # download and reset the Velox checkout.
        function = re.search(
            rf"(?ms)^function {function_name} \{{\n.*?^\}}",
            (SETUP_DIR / "get-velox.sh").read_text(),
        )
        self.assertIsNotNone(function)
        command = function.group() + "\n" + function_name
        self.run_shell(command)
        first_setup = setup_file.read_text()
        self.run_shell(command)
        self.assertEqual(setup_file.read_text(), first_setup)

        self.run_shell(f'source "scripts/setup-{platform}.sh"')
        installed = self.package_log.read_text().splitlines()
        self.assertEqual(installed.count(package), 1)
        self.assertIn(existing_package, installed)

    def test_ubuntu(self):
        self.check_upstream_setup(
            "ubuntu",
            "${SUDO} apt install -y \\\n  libevent-dev \\\n  libsodium-dev\n",
            "librdkafka-dev",
            "libevent-dev",
        )

    def test_centos9(self):
        self.check_upstream_setup(
            "centos9",
            "dnf_install libevent-devel \\\n  libsodium-devel\n",
            "librdkafka-devel",
            "libevent-devel",
        )

    def test_macos(self):
        self.check_upstream_setup(
            "macos",
            'MACOS_VELOX_DEPS="libevent libsodium"\n'
            'for pkg in ${MACOS_VELOX_DEPS}; do brew install "$pkg"; done\n',
            "librdkafka",
            "libevent",
        )

    def test_local_rpm_setup_scripts(self):
        for platform in ("centos7", "centos8", "openeuler24", "rhel"):
            with self.subTest(platform=platform):
                source = (SETUP_DIR / f"setup-{platform}.sh").read_text()
                # Execute the actual package command, including continuation
                # lines, without invoking compiler setup or source builds.
                first_package = "ccache" if platform == "centos7" else "libevent-devel"
                command = re.search(
                    rf"(?m)^[ \t]*dnf_install {first_package}\b(?:[^\n]*\\\n)*[^\n]*",
                    source,
                )
                self.assertIsNotNone(command)
                self.package_log.write_text("")
                self.run_shell(command.group())
                installed = self.package_log.read_text().splitlines()
                self.assertEqual(installed.count("librdkafka-devel"), 1)
                self.assertIn("libevent-devel", installed)


if __name__ == "__main__":
    unittest.main()

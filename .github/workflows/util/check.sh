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

export BASE_COMMIT=$1

RESULT=0

./.github/workflows/util/check.py header branch
if [ $? -ne 0 ]; then
  ./.github/workflows/util/check.py header branch --fix
  echo -e "\n==== Apply using:"
  echo "patch -p1 \<<EOF"
  git --no-pager diff
  echo "EOF"
  RESULT=1
fi

# Check that shell scripts use #!/usr/bin/env bash instead of #!/bin/bash
BAD_SHEBANGS=$(git diff --relative --name-only --diff-filter='ACM' "$BASE_COMMIT" -- '*.sh' | while read -r f; do
  [ -f "$f" ] && head -1 "$f" | grep -q '^#!/bin/bash' && echo "$f"
done)

if [ -n "$BAD_SHEBANGS" ]; then
  echo -e "\n==== The following scripts use #!/bin/bash instead of #!/usr/bin/env bash:"
  echo "$BAD_SHEBANGS"
  echo "Please replace '#!/bin/bash' with '#!/usr/bin/env bash' for portability."
  RESULT=1
fi

exit $RESULT


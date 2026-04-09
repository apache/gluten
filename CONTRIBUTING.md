## Contributing

Welcome to make your contribution to Gluten project!

The contribution is not limited to contributing code. It also includes reviewing code, improving documentation, proposing ideas, etc.

### GitHub Pull Request

If necessary, please file a GitHub issue beforehand and reference that issue in your PR title.

By convention, `CH` & `VL` represent ClickHouse & Velox backend respectively. Please add `[CH]` or `[VL]` in your PR title
accordingly if your proposed code change is just for one specific backend. Please do NOT add such tag if common code is changed,
but add tags like `[CORE]`, `[MINOR]`, `[DOC]` instead.

Examples:
* PR just for CH backend:

  `[GLUTEN-<issue ID>][CH] xxxx`

* PR just for VL backend:

  `[GLUTEN-<issue ID>][VL] xxxx`

* PR with common code changed:

  `[GLUTEN-<issue ID>][CORE] xxxx`

Please add a description for your PR, if necessary, to help reviewer understand it.

### GitHub Issue

To avoid redundancy, please firstly do a search before going to file a new GitHub issue.

If the new issue is just relevant to a specific backend, please add `[CH]` or `[VL]` in your issue title accordingly.

We encourage developer to firstly file a GitHub issue before drafting a patch, which can avoid possible repeated work.

### Unit Test

A lot of Spark UTs have been imported into Gluten project. If your proposed fix or feature is not covered by Spark UTs,
please add at least one UT to ensure code quality and reduce regression issues for the project.

### Document

Please update document for your proposed code change if necessary.

If a new config property is being introduced, please update [Configuration.md](https://github.com/apache/gluten/blob/main/docs/Configuration.md).

### Code Style

##### Java/Scala code style
Developer can import the code style setting to IDE and format Java/Scala code with spotless maven plugin. See [Java/Scala code style](https://github.com/apache/gluten/blob/main/docs/developers/NewToGluten.md#javascala-code-style).

##### C/C++ code style
There are some code style conventions need to comply. See [CppCodingStyle.md](https://github.com/apache/gluten/blob/main/docs/developers/CppCodingStyle.md).

For Velox backend, developer can just execute `dev/formatcppcode.sh` to format C/C++ code. It requires `clang-format-15`
installed in your development env.

### License Header

An identical license header is used for all Gluten source code. If lacked or wrongly used, a check failure will be reported by Gluten CI.

You can execute a script to fix license header issue, as the following shows.

`dev/check.py header main --fix`

### Gluten CI

##### ClickHouse Backend CI
To check CI failure for CH backend, please log in with the public account/password provided [here](https://github.com/apache/gluten/blob/main/docs/get-started/ClickHouse.md#new-ci-system).
To re-trigger CH CI, please post the below comment on PR page:

`Run Gluten Clickhouse CI`

##### Velox Backend CI
To check CI failure for Velox backend, please go into the GitHub action page from your PR page.

To see the perf. impact on Velox backend, you can comment `/Benchmark Velox` on PR page to trigger a pretest. The benchmark
(currently TPC-H) result will be posted after completed.

If some new dependency is required to be installed, you may need to do some change for CI docker at [this folder](https://github.com/apache/gluten/tree/main/tools/gluten-te).

### Code Review

Please ensure no CI failure is reported for your PR.

A PR may need several iterations in the community review process. Please @reviewer once your PR is ready for next round of review.

For critical code change, merging the PR requires at least two committers' approval.

### Code Merging

**Squash and merge** should be selected to merge PR. If commit message title doesn't align with its PR title, please revise it.

Example for commit message:

  `[GLUTEN-<issue ID>][CORE] xxxx (#<PR ID>)`

Please add some brief description in extended message section, if helpful, when merging PR. Please do NOT leave any meaningless
historical commit message, such as "Initial commit", "Fix", etc.

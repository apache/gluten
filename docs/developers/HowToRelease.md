---
layout: page
title: How to Release
nav_order: 17
parent: Developer Overview
---

# Create Release Source and Binaries

The document provides a standard process for creating the sources and binaries that are required
for a release of Apache Gluten project with the Velox backend.

## Prerequisites

1. x86-64
2. Linux
3. Docker

## Pre-release Checks

Complete all of these **before cutting the release candidate tag**. Each one corresponds to an issue
previously raised during a release vote, so skipping them tends to cost an extra release candidate.

### Tag the Velox revision this release builds against

`ep/build-velox/src/get-velox.sh` pins the Velox fork and branch used by the native build:

```bash
VELOX_REPO=https://github.com/IBM/velox.git
VELOX_BRANCH=branch-1.7-dft
```

The build resolves that branch to whatever commit it happens to point at, via `git ls-remote`. The
branch keeps moving, so rebuilding the same Gluten tag later would pick up different Velox code.

Create a tag in the [IBM/velox](https://github.com/IBM/velox) repository for the commit this release
is built against, so the native side stays reproducible for as long as the Gluten release exists.

### Set the release version and remove leftover `-SNAPSHOT` strings

```bash
bash dev/release/bump-version.sh 1.7.0
```

`bump-version.sh` runs `versions:set` on the POMs under the repository root, `tools/gluten-it`,
`tools/qualification-tool` and `gluten-flink`. It does **not** touch versions hardcoded elsewhere,
for example `dev/info.sh` and the docs under `gluten-flink/docs/`. Check for anything it missed:

```bash
git grep -nIE '[0-9]+\.[0-9]+\.[0-9]+-SNAPSHOT'
```

Gluten's own version must not appear as `-SNAPSHOT` in the result. References to third-party
snapshots, such as `velox4j` and `spark-sql-perf`, are expected to remain.

A previous RC was voted `+0` because the source distribution still carried `-SNAPSHOT` versions,
including stale ones left over from an earlier release. Clean up all of them, not just the current
version.

The version bump must be committed **before** the release candidate tag is created, since the source
archive is generated from that tag. A tag that still carries `-SNAPSHOT` cannot be released and
requires cutting a new candidate.

### Update `LICENSE-binary` and `NOTICE-binary`

The bundle JAR statically links third-party code, and `package-release.sh` ships `LICENSE-binary`
and `NOTICE-binary` inside each binary tarball as `LICENSE` and `NOTICE`. Whenever a bundled
component is added or enabled, its copyright and license must be recorded there.

A previous RC was voted `-1` because Hudi and Paimon had been enabled in the binaries without
being added to `NOTICE-binary`.

### Check license headers

```bash
# Requires the `regex` Python package: pip install regex
python3 dev/check.py header main
```

Reviewers also run [Apache RAT](https://creadur.apache.org/rat/) against the source archive. Note
that RAT flags `.ipynb` notebooks, which cannot carry a comment header; the convention is to place
the license text in the notebook's first markdown cell.

## Steps to Create a Release

A standard release distribution can be created following the below steps.

### Pull and run the dev docker image

Pull and download the build environment docker image. The docker image is periodically
built and uploaded to DockerHub by scheduled GitHub Actions jobs.

```bash
docker pull apache/gluten:vcpkg-centos-7-gcc13
docker run -it apache/gluten:vcpkg-centos-7-gcc13 bash
```

### Clone the repository

In the docker container created by the last step, execute the following command to
clone the repository of Gluten with a specific git tag that you want to build on.

We are taking `v1.7.0-rc0` as an example git tag in this guide.

```bash
git clone --branch v1.7.0-rc0 https://github.com/apache/gluten.git /workspace
```

### Build

Build the project for all supported Spark versions.

```bash
cd /workspace
bash dev/release/build-release.sh
```

Alternatively, let CI do it. `.github/workflows/build_release.yml` runs the same
`dev/release/build-release.sh` in the same container, and is triggered automatically when a tag
matching `v*` is pushed — including the release candidate tag, for example `v1.7.0-rc0`. Download the
`build-velox-backend-release-packages` artifact from that workflow run instead of building locally.

### Collect the bundle JARs into the release directory

`package-release.sh` reads from `$GLUTEN_HOME/release/`, which is not created by the build. Create it
yourself and place one bundle JAR per supported Spark version inside, taken either from a local build
or from the workflow artifact above.

```bash
cd /workspace
mkdir -p release
cp -R package/target/* release/
```

The directory should end up holding these five JARs, matching the Spark and Scala combinations
`package-release.sh` expects:

```
gluten-velox-bundle-spark3.3_2.12-linux_amd64-1.7.0.jar
gluten-velox-bundle-spark3.4_2.12-linux_amd64-1.7.0.jar
gluten-velox-bundle-spark3.5_2.12-linux_amd64-1.7.0.jar
gluten-velox-bundle-spark4.0_2.13-linux_amd64-1.7.0.jar
gluten-velox-bundle-spark4.1_2.13-linux_amd64-1.7.0.jar
```

A missing or misnamed JAR makes `package-release.sh` fail with the Spark version it could not find.

### Package the release sources and binaries

By following this step you will create the release distribution that comply with the common name
convention of ASF project release process.

Note, the current tag should be specified when running this script.

```bash
cd /workspace
bash dev/release/package-release.sh v1.7.0-rc0
```

### Check the created release distribution

Confirm that all the needed sources and binaries are successfully created at the release directory
`$GLUTEN_HOME/release/`.

```bash
ls -1 release/*.tar.gz
```

```
release/apache-gluten-1.7.0-src.tar.gz
release/apache-gluten-1.7.0-bin-spark-3.3.tar.gz
release/apache-gluten-1.7.0-bin-spark-3.4.tar.gz
release/apache-gluten-1.7.0-bin-spark-3.5.tar.gz
release/apache-gluten-1.7.0-bin-spark-4.0.tar.gz
release/apache-gluten-1.7.0-bin-spark-4.1.tar.gz
```

Each binary tarball contains a versioned top-level directory holding the bundle JAR along with
the `LICENSE` and `NOTICE` that cover the third-party code statically linked into it.

```bash
tar -tzf release/apache-gluten-1.7.0-bin-spark-3.5.tar.gz
```

```
apache-gluten-1.7.0-bin-spark-3.5/
apache-gluten-1.7.0-bin-spark-3.5/LICENSE
apache-gluten-1.7.0-bin-spark-3.5/NOTICE
apache-gluten-1.7.0-bin-spark-3.5/gluten-velox-bundle-spark3.5_2.12-linux_amd64-1.7.0.jar
```

<!--- Moved from https://github.com/apache/gluten-site/blob/main/_docs/v1.3.0/developers/HowToRelease.md --->
# Publish the Release

This section outlines the steps for releasing Apache Gluten according to the Apache release guidelines.
All projects under the Apache umbrella must adhere to the [Apache Release Policy](https://www.apache.org/legal/release-policy.html).
This guide is designed to assist you in comprehending the policy and navigating the process of releasing projects at Apache.

## Release Process

Work through the steps in order. They assume the tarballs from the previous section exist under
`$GLUTEN_HOME/release/`.

Throughout, replace `<asf-id>` with your ASF id, `<asf-id>@apache.org` with your ASF email, and
`1.7.0` / `1.7.0-rc0` with the version being released.

Note the artifact **file names carry no `rc` suffix** — `package-release.sh` strips it. A release
candidate is identified only by the SVN directory it is staged in, `1.7.0-rc0`. That is why a
passing vote needs no re-signing: the same files simply move to `release/gluten/1.7.0/`.

### 1. Confirm the tag and record the commit id

The tag must already exist on GitHub with draft release notes, since the source tarball is built
from it. Record the commit id for the vote email:

```bash
git rev-parse v1.7.0-rc0
```

### 2. Preflight the artifacts

Once the artifacts are signed and uploaded, any change requires a new release candidate, so check
the structure first:

```bash
cd $GLUTEN_HOME/release/

# Each binary tarball: a versioned top-level directory holding the JAR, LICENSE and NOTICE.
for f in apache-gluten-1.7.0-bin-spark-*.tar.gz; do echo "== $f"; tar -tzf "$f"; done

# Source tarball: check the top-level directory name and that dot directories are gone.
tar -tzf apache-gluten-1.7.0-src.tar.gz | head -20
tar -tzf apache-gluten-1.7.0-src.tar.gz | grep -E '/\.(git|github|idea)' || echo "  clean"
```

### 3. Prepare the signing key

Reuse your existing key if you have it; a new key means every reviewer must re-import `KEYS`. To
move a key to another machine, export it with `gpg --export-secret-keys --armor <KEYID>`, transfer
it over a secure channel, and `gpg --import` it there.

Create one only if the previous key is unrecoverable:

```bash
gpg --full-generate-key
```

Choose **RSA and RSA**, **4096** bits, and no expiry. Use your ASF email in the UID: reviewers check
that the signing key maps to a committer. Set a long passphrase and record it in a password
manager — a key you cannot unlock is as lost as one you no longer have.

```bash
gpg --list-keys --keyid-format SHORT <asf-id>@apache.org
gpg --keyserver keyserver.ubuntu.com --send-key <asf-id>@apache.org

# Confirm the key can sign.
echo test > /tmp/t \
  && gpg --local-user <asf-id>@apache.org --armor --detach-sig /tmp/t \
  && gpg --verify /tmp/t.asc /tmp/t
```

On distributions that ship a cut-down GnuPG, `gpg --full-generate-key` fails with
`can't connect to the gpg-agent`. On Amazon Linux 2023, install the full package:

```bash
sudo dnf swap gnupg2-minimal gnupg2 -y
```

### 4. Back up the key

```bash
gpg --export-secret-keys --armor <asf-id>@apache.org > ~/gluten-signing-key.asc
```

Move that file to offline storage and delete it from the machine. Together with the passphrase in
your password manager, this is what lets the next release reuse the key.

### 5. Append the key to KEYS, before the vote

```bash
svn co --depth files --username <asf-id> \
  https://dist.apache.org/repos/dist/release/gluten/ ~/svn-gluten-release
cd ~/svn-gluten-release
(gpg --list-sigs <asf-id>@apache.org && gpg --export --armor <asf-id>@apache.org) >> KEYS
svn ci --username <asf-id> -m "Add GPG key for <asf-id>@apache.org"
```

The SVN password is your ASF LDAP password from <https://id.apache.org>, not the GPG passphrase.

Append; never replace. Older key blocks must stay, because they still verify previously released
artifacts.

`KEYS` lives in the **release** directory, not `dev`, so that it is served from
<https://downloads.apache.org/gluten/KEYS>. Confirm it appears there before starting the vote and
reference that URL in the vote email: a previous vote had to be corrected mid-thread because it
pointed reviewers at a `dist.apache.org/repos/dist/dev/` link instead. Propagation takes a few
minutes.

### 6. Sign the artifacts

```bash
cd $GLUTEN_HOME/release/
for i in *.tar.gz; do
  echo "$i"
  gpg --local-user <asf-id>@apache.org --armor --output "$i.asc" --detach-sig "$i"
done
```

`gpg-agent` caches the passphrase, so it is entered once rather than per file.

### 7. Generate checksums

```bash
for i in *.tar.gz; do echo "$i"; sha512sum "$i" > "$i.sha512"; done
```

### 8. Verify your own artifacts

```bash
for i in *.tar.gz; do gpg --verify "$i.asc" "$i"; done
for i in *.tar.gz; do sha512sum --check "$i.sha512"; done
ls -1 | wc -l
```

Expect three files per archive — the tarball, its `.asc` and its `.sha512`.

### 9. Upload to the dev staging area

`--depth immediates` avoids downloading every previous release, which matters at roughly 100 MB per
tarball:

```bash
svn co --depth immediates --username <asf-id> \
  https://dist.apache.org/repos/dist/dev/gluten/ ~/svn-gluten-dev
cd ~/svn-gluten-dev
mkdir 1.7.0-rc0
cp $GLUTEN_HOME/release/*.tar.gz* 1.7.0-rc0/
svn add 1.7.0-rc0
svn ci --username <asf-id> -m "Add Apache Gluten 1.7.0-rc0 release artifacts"
```

The project directory `https://dist.apache.org/repos/dist/dev/gluten/` only needs creating once,
for the first ever release.

### 10. Confirm the upload

Visit <https://dist.apache.org/repos/dist/dev/gluten/1.7.0-rc0/> and confirm the source archive plus
one binary archive per supported Spark version, each with its `.asc` and `.sha512`:

```
apache-gluten-1.7.0-src.tar.gz{,.asc,.sha512}
apache-gluten-1.7.0-bin-spark-3.3.tar.gz{,.asc,.sha512}
apache-gluten-1.7.0-bin-spark-3.4.tar.gz{,.asc,.sha512}
apache-gluten-1.7.0-bin-spark-3.5.tar.gz{,.asc,.sha512}
apache-gluten-1.7.0-bin-spark-4.0.tar.gz{,.asc,.sha512}
apache-gluten-1.7.0-bin-spark-4.1.tar.gz{,.asc,.sha512}
```

## Verifying a Release Candidate

This section is for anyone voting on a candidate, including the release manager before calling the
vote.

1. Check if the Download links are valid.

2. Check if the checksums and GPG signatures are valid.

3. Check if the release artifacts name is qualified and match with the current release.

4. Check if LICENSE and NOTICE files are correct.

5. Check if the License Headers are included in all files if necessary.

6. No unlicensed compiled archives bundled in source archive.


### How to Verify the Signatures

Please follow below steps to verify the signatures.

```bash
# download KEYS
$ curl https://downloads.apache.org/gluten/KEYS > KEYS

# import KEYS and trust the key, please replace the email address with the one you want to trust.
$ gpg --import KEYS
$ gpg --edit-key xxx@apache.org
gpg> trust
gpg> 5
gpg> y
gpg> quit

# enter the directory where the release artifacts are located
$ cd /path/to/release/artifacts

# verify the signature
$ for i in *.tar.gz; do echo $i; gpg --verify $i.asc $i ; done

# if you see 'Good signature' in the output, it means the signature is valid.
```


### How to Verify the checksums

Please follow below steps to verify the checksums
```bash
# verify the checksums
$ for i in *.tar.gz; do echo $i; sha512sum --check  $i.sha512; done
```

## Voting and Publishing

### 11. Initiate the release vote

Email a vote request to dev@gluten.apache.org, requiring at least 3 PMC +1s. Keep it open for at
least 72 hours or until enough votes are collected. Gluten is a Top-Level Project, so this is the
only vote required; there is no `general@incubator.apache.org` stage.

If the candidate is signed with a key that was not used for previous releases, say so in the email,
otherwise reviewers with the old key cached will hit a verification failure.

Vote Email Template
```
[VOTE] Release Apache Gluten 1.7.0 (RC0)

Hello everyone,

This is a call for a vote to release Apache Gluten version 1.7.0 (RC0).

The release candidates:
https://dist.apache.org/repos/dist/dev/gluten/1.7.0-rc0/

Release notes:
https://github.com/apache/gluten/releases/tag/v1.7.0-rc0

Git commit id for the release:
https://github.com/apache/gluten/commit/{commit-id}

Keys to verify the Release Candidate:
https://downloads.apache.org/gluten/KEYS

The vote will be open for at least 72 hours or until the necessary number
of votes are reached.

Please vote accordingly:

[ ] +1 approve
[ ] +0 no opinion
[ ] -1 disapprove (please provide reason)

Checklist for reference:

[ ] Download links are valid
[ ] Checksums and PGP signatures are valid
[ ] Source code distributions have correct names, matching the current release
[ ] LICENSE and NOTICE files are correct
[ ] All files have license headers if necessary
[ ] No unlicensed compiled archives are bundled in the source archive
[ ] Other (please specify):

To compile from the source, please refer to:
https://github.com/apache/gluten/tree/v1.7.0-rc0#build-from-source

Thanks,
{YOUR NAME}
```

Add this line under the KEYS link when the candidate is signed with a new key:

```
Note: this release is signed with a new GPG key. Please re-import KEYS
before verifying the signatures.
```

The build reference is pinned to the candidate's tag rather than `main`, so the instructions match
the code being voted on.

### 12. Publish the vote result

Send a `[RESULT][VOTE]` email to dev@gluten.apache.org summarising the binding and non-binding
votes, and linking the vote thread.

If the vote did not pass, address the findings, tag the next candidate, and repeat from step 6 into
a new `1.7.0-rc1` staging directory. The signing key and its `KEYS` entry carry over.

### 13. Announce the results and the release


Announce Email Template
```
[ANNOUNCE] Apache Gluten 1.7.0 released

Hello everyone,

The Apache Gluten 1.7.0 has been released!

A Middle Layer for Offloading JVM-Based SQL Execution to Native Engines

Download Links: https://downloads.apache.org/gluten/

Release Notes: https://github.com/apache/gluten/releases/tag/v1.7.0

Website: https://gluten.apache.org/

Resources:
- Issue: https://github.com/apache/gluten/issues
- Mailing list: dev@gluten.apache.org

Thanks,
<YOUR NAME>
```

### 14. Migrate candidate to the release Apache SVN

After the vote has passed, promote the candidate by moving the artifacts from Apache SVN's `dev`
directory to the `release` directory. Note the `rc` suffix is dropped from the directory name; the
file names and signatures are unchanged, so nothing is re-signed.

```bash
svn mv --username <asf-id> \
  https://dist.apache.org/repos/dist/dev/gluten/1.7.0-rc0 \
  https://dist.apache.org/repos/dist/release/gluten/1.7.0 \
  -m "Transfer packages for Apache Gluten 1.7.0"
```

### 15. Finish up

- Publish the GitHub release for the tag.
- Update the download page on the website.
- Bump the development version on the release branch with
  `bash dev/release/bump-version.sh <next-version>-SNAPSHOT`.

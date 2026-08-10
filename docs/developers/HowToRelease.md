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

Complete these before tagging an RC. Each one corresponds to an issue previously raised during a
release vote, so skipping them tends to cost an extra release candidate.

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

We are taking `v1.6.0-example-rc3` as an example git tag in this guide.

```bash
git clone --branch v1.6.0-example-rc3 https://github.com/apache/gluten.git /workspace
```

### Build

Build the project for all supported Spark versions.

```bash
cd /workspace
bash dev/release/build-release.sh
```

### Copy the target binaries to release directory

```bash
cd /workspace
mkdir -p release
cp -R package/target/* release/
```

### Package the release sources and binaries

By following this step you will create the release distribution that comply with the common name
convention of ASF project release process.

Note, the current tag should be specified when running this script.

```bash
cd /workspace
bash dev/release/package-release.sh v1.6.0-example-rc3
```

### Check the created release distribution

Confirm that all the needed sources and binaries are successfully created at the release directory
`$GLUTEN_HOME/release/`.

```bash
[root@8de83f716f0f workspace]# ls -1 release/*.tar.gz
release/apache-gluten-1.6.0-example-src.tar.gz
release/apache-gluten-1.6.0-example-bin-spark-3.3.tar.gz
release/apache-gluten-1.6.0-example-bin-spark-3.4.tar.gz
release/apache-gluten-1.6.0-example-bin-spark-3.5.tar.gz
release/apache-gluten-1.6.0-example-bin-spark-4.0.tar.gz
release/apache-gluten-1.6.0-example-bin-spark-4.1.tar.gz
```

Each binary tarball contains a versioned top-level directory holding the bundle JAR along with
the `LICENSE` and `NOTICE` that cover the third-party code statically linked into it.

```bash
[root@8de83f716f0f workspace]# tar -tzf release/apache-gluten-1.6.0-example-bin-spark-3.5.tar.gz
apache-gluten-1.6.0-example-bin-spark-3.5/
apache-gluten-1.6.0-example-bin-spark-3.5/LICENSE
apache-gluten-1.6.0-example-bin-spark-3.5/NOTICE
apache-gluten-1.6.0-example-bin-spark-3.5/gluten-velox-bundle-spark3.5_2.12-linux_amd64-1.6.0-example.jar
```

<!--- Moved from https://github.com/apache/gluten-site/blob/main/_docs/v1.3.0/developers/HowToRelease.md --->
# Publish the Release

This section outlines the steps for releasing Apache Gluten according to the Apache release guidelines.
All projects under the Apache umbrella must adhere to the [Apache Release Policy](https://www.apache.org/legal/release-policy.html). This guide is designed to assist you in comprehending the policy and navigating the process of releasing projects at Apache.

## Release Process

1. Prepare the release artifacts.

2. Upload the release artifacts to the SVN repository.

3. Verify the release artifacts.

4. Initiate a release vote.

5. Announce the results and the release.


### Prepare the release artifacts.

1. Create a branch from the target git repository.

2. Tag a RC and draft the release notes.

3. Build and Sign the release artifacts (including source archives, binaries, ...etc).

4. Generate checksums for the artifacts.


#### How to Sign the release artifacts.

1. Create a GPG key

2. Add the GPG key to the KEYS file

3. Sign the release artifacts with the GPG key.

```bash
# create a GPG key, after executing this command, select "RSA and RSA"
$ gpg --full-generate-key

# list the GPG keys
$ gpg  --keyid-format SHORT --list-keys

# upload the GPG key to the key server, xxx is the GPG key id
# eg: pub rsa4096/4C21E346 2024-05-06 [SC], 4C21E346 is the GPG key id;
$ gpg --keyserver keyserver.ubuntu.com --send-key xxx

# append the GPG key to the KEYS file the svn repository
# [IMPORTANT] Don't replace the KEYS file, just append the GPG key to the KEYS file. 
$ svn co https://dist.apache.org/repos/dist/release/gluten/
$ (gpg --list-sigs xxx@apache.org && gpg --export --armor xxx@apache.org) >> KEYS 
$ svn ci -m "add gpg key" 

# sign the release artifacts, xxxx is xxx@apache.org
$ for i in *.tar.gz; do echo $i; gpg --local-user xxxx --armor --output $i.asc --detach-sig $i ; done
```

Note the KEYS file is appended in the **release** SVN directory, not the `dev` one, so that it is
served from <https://downloads.apache.org/gluten/KEYS>. Do this **before** starting the vote and
reference that URL in the vote email: a previous vote had to be corrected mid-thread because it
pointed reviewers at a `dist.apache.org/repos/dist/dev/` KEYS link instead.

#### How to Generate checksums for the release artifacts.

```bash
# create the checksums
$ for i in *.tar.gz; do echo $i; sha512sum  $i > $i.sha512 ; done
```


### Upload the release artifacts to the SVN repository.

1. Create a project directory in the SVN repository (1st time only).
   `https://dist.apache.org/repos/dist/dev/gluten/`

2. Create a directory for the release artifacts in the SVN repository.
   `https://dist.apache.org/repos/dist/dev/gluten/{release-version}`
   release-version format: #.#.#-rc# (e.g. `1.7.0-rc0`)

3. Upload the release artifacts to the SVN repository.
```bash
$ svn co https://dist.apache.org/repos/dist/dev/gluten/
$ cp /path/to/release/artifacts/* ./{release-version}/
$ svn add ./{release-version}/*
$ svn commit -m "add Apache Gluten release artifacts for {release-version}"
```

4. After the upload, please visit the link `https://dist.apache.org/repos/dist/dev/gluten/{release-version}` to verify if the file upload is successful or not.
   The uploaded artifacts should include the source archive and one binary archive per supported
   Spark version, each with its `.asc` signature and `.sha512` checksum:
```bash
* apache-gluten-#.#.#-src.tar.gz{,.asc,.sha512}
* apache-gluten-#.#.#-bin-spark-3.3.tar.gz{,.asc,.sha512}
* apache-gluten-#.#.#-bin-spark-3.4.tar.gz{,.asc,.sha512}
* apache-gluten-#.#.#-bin-spark-3.5.tar.gz{,.asc,.sha512}
* apache-gluten-#.#.#-bin-spark-4.0.tar.gz{,.asc,.sha512}
* apache-gluten-#.#.#-bin-spark-4.1.tar.gz{,.asc,.sha512}
```


### Verify the release artifacts.

Please follow below steps to verify the release artifacts.

1. Check if the Download links are valid.

2. Check if the checksums and GPG signatures are valid.

3. Check if the release artifacts name is qualified and match with the current release.

4. Check if LICENSE and NOTICE files are correct.

5. Check if the License Headers are included in all files if necessary.

6. No unlicensed compiled archives bundled in source archive.


#### How to Verify the Signatures

Please follow below steps to verify the signatures.

```bash
# download KEYS
$ curl https://dist.apache.org/repos/dist/release/gluten/KEYS > KEYS

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


#### How to Verify the checksums

Please follow below steps to verify the checksums
```bash
# verify the checksums
$ for i in *.tar.gz; do echo $i; sha512sum --check  $i.sha512; done
```

### Initiate a release vote.

1. Email a vote request to dev@gluten.apache.org, requiring at least 3 PMC +1s.

2. Allow 72 hours or until enough votes are collected.

3. Share the vote outcome on the dev list.

Vote Email Template
```
[VOTE] Release Apache Gluten {release-version}

Hello,

    This is a call for vote to release Apache Gluten version {release-version}.

    The vote thread:
        https://lists.apache.org/thread/{id}

    Vote Result:
        https://lists.apache.org/thread/{id}

    The release candidates:
        https://dist.apache.org/repos/dist/dev/gluten/{release-version}/
    
    Release notes:
        https://github.com/apache/gluten/releases/tag/{release-version}

    Git tag for the release:
        https://github.com/apache/gluten/releases/tag/{release-version}
    
    Git commit id for the release:
        https://github.com/apache/gluten/commit/{id}

    Keys to verify the Release Candidate:
        https://downloads.apache.org/gluten/KEYS
        
    The vote will be open for at least 72 hours or until the necessary number of votes are reached.

    Please vote accordingly:

    [ ] +1 approve
    [ ] +0 no opinion
    [ ] -1 disapprove with the reason

    Checklist for reference:

    [ ] Download links are valid.
    [ ] Checksums and PGP signatures are valid.
    [ ] Source code distributions have correct names matching the current release.
    [ ] LICENSE and NOTICE files are correct for each Apache Gluten repo.
    [ ] All files have license headers if necessary.
    [ ] No unlicensed compiled archives bundled in source archive.

    To compile from the source, please refer to:
    
    https://github.com/apache/gluten#building-from-source

Thanks,
<YOUR NAME>
```

### Announce the results and the release.


Announce Email Template
```
Hello everyone,

The Apache Gluten {release-version} has been released!

Apache Gluten is a middle layer responsible for offloading JVM-based SQL engines' execution to native engines.

Download Links: https://downloads.apache.org/gluten/

Release Notes: https://github.com/apache/gluten/releases/tag/{release-version}

Website: https://gluten.apache.org/

Resources:
- Issue: https://github.com/apache/gluten/issues
- Mailing list: dev@gluten.apache.org

Thanks,
<YOUR NAME>

```

### Migrate candidate to the release Apache SVN

After the vote has passed, you need to migrate the RC build release to an official release by moving the artifacts from Apache SVN's dev directory to the release directory. Please follow the steps below to upload the artifacts:
```
$ svn mv https://dist.apache.org/repos/dist/dev/gluten/{release-version} https://dist.apache.org/repos/dist/release/gluten/{release-version} -m "transfer packages for gluten {release-version}"
```

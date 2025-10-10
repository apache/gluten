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

ROOT_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))
BUILD_DIR := ${ROOT_DIR}/cpp/build
CONAN_FILE_DIR := ${ROOT_DIR}/cpp/
BUILD_TYPE=Debug
ENABLE_ASAN ?= False
LDB_BUILD ?= False
BUILD_BENCHMARKS ?= False
BUILD_TESTS ?= False
BUILD_EXAMPLES ?= False
BUILD_ORC ?= False
ENABLE_PROTON ?= False

# conan package info
GLUTEN_BUILD_VERSION ?= main
BOLT_BUILD_VERSION ?= main
BUILD_USER ?=
BUILD_CHANNEL ?=

ENABLE_HDFS ?= True
ENABLE_S3 ?= False
RSS_PROFILE ?= ''

ifeq ($(BUILD_BENCHMARKS),True)
BUILD_ORC = True
endif

ARCH := $(shell arch)
ifeq ($(ARCH), x86_64)
	ARCH := amd64
endif

SHARED_LIBRARY ?= True

# Gluten package version, read from package/pom.xml so jar paths stay in sync
# when the project version bumps.
GLUTEN_PACKAGE_VERSION := $(shell sed -n '0,/<version>\(.*\)<\/version>/s//\1/p' \
    ${ROOT_DIR}/package/pom.xml | head -1 | xargs)

# Manually specify the number of bolt compilation threads by setting the BOLT_NUM_THREADS environment variable.
# e.g. export BOLT_NUM_THREADS=50
ifndef CI_NUM_THREADS
	ifdef BOLT_NUM_THREADS
		NUM_THREADS ?= $(BOLT_NUM_THREADS)
	else
		NUM_THREADS ?= $$((  $(shell grep -c ^processor /proc/cpuinfo) / 2 ))
	endif
else
    NUM_THREADS ?= $(CI_NUM_THREADS)
endif

ALLOWED_VERSIONS := 11 17
ifeq ($(JAVA_HOME),)
    $(error ERROR: JAVA_HOME is not set)
endif
ifneq ($(wildcard $(JAVA_HOME)/bin/java),)
    ifneq ($(wildcard $(JAVA_HOME)/bin/javac),)
        JDK_VERSION := $(shell $(JAVA_HOME)/bin/java -version 2>&1 | sed -n 's/.*version "\(1\.\)\{0,1\}\([0-9]\+\).*/\2/p')
        ifneq ($(filter $(JDK_VERSION),$(ALLOWED_VERSIONS)),$(JDK_VERSION))
            $(error ERROR: JDK version $(JDK_VERSION) is not supported, only 11 and 17 are allowed now)
        endif
    endif
endif

.PHONY: build clean clean_cpp debug release RelWithDebInfo \
	install_debug install_release release-with-tests debug-with-tests \
	release-with-benchmarks debug-with-benchmarks \
	release-with-tests-and-benchmarks debug-with-tests-and-benchmarks \
	java jar jar-skip-check jar_spark33 jar_spark34 jar_spark35 \
	fast-jar zip fast-zip test test_spark35 cpp-test-release cpp-test-debug

bolt-recipe:
	@echo "Install Bolt recipe into local cache"
	rm -rf ep/bolt
	git clone --depth=1 --branch ${BOLT_BUILD_VERSION} https://github.com/bytedance/bolt.git ep/bolt &&\
    bash ep/bolt/scripts/install-bolt-deps.sh && \
    conan export ep/bolt/conanfile.py --name=bolt --version=${BOLT_BUILD_VERSION} --user=${BUILD_USER} --channel=${BUILD_CHANNEL}
	@echo "Bolt recipe has been installed"

build:
	mkdir -p ${BUILD_DIR} && mkdir -p ${BUILD_DIR}/releases &&\
	cd  ${CONAN_FILE_DIR} && export BOLT_BUILD_VERSION=${BOLT_BUILD_VERSION} &&\
	ALL_CONAN_OPTIONS=" -o gluten/*:shared=${SHARED_LIBRARY} \
			-o gluten/*:enable_hdfs=${ENABLE_HDFS} \
			-o gluten/*:enable_s3=${ENABLE_S3} \
			-o gluten/*:enable_asan=${ENABLE_ASAN} \
			-o gluten/*:build_benchmarks=${BUILD_BENCHMARKS} \
			-o gluten/*:build_tests=${BUILD_TESTS} \
			-o gluten/*:build_examples=${BUILD_EXAMPLES} " && \
	conan graph info . --name=gluten --version=${GLUTEN_BUILD_VERSION} --user=${BUILD_USER} --channel=${BUILD_CHANNEL} -c "arrow/*:tools.build:download_source=True" $${ALL_CONAN_OPTIONS} --format=html > gluten.conan.graph.html  && \
	NUM_THREADS=$(NUM_THREADS) conan install . --name=gluten --version=${GLUTEN_BUILD_VERSION} --user=${BUILD_USER} --channel=${BUILD_CHANNEL}  \
			-s llvm-core/*:build_type=Release -s build_type=${BUILD_TYPE} --build=missing $${ALL_CONAN_OPTIONS} && \
	cmake --preset `echo conan-${BUILD_TYPE} | tr A-Z a-z` && \
	cmake --build build/${BUILD_TYPE} -j $(NUM_THREADS) && \
	if [ "${SHARED_LIBRARY}" = "True" ]; then  cmake --build ${BUILD_DIR}/${BUILD_TYPE} --target install ; fi && \
	if [ "${SHARED_LIBRARY}" = "False" ]; then \
	    conan export-pkg . --name=gluten --version=${GLUTEN_BUILD_VERSION} --user=${BUILD_USER} --channel=${BUILD_CHANNEL} -s build_type=${BUILD_TYPE} \
		$${ALL_CONAN_OPTIONS} ; \
	fi && cd -

release :
	$(MAKE) build BUILD_TYPE=Release GLUTEN_BUILD_VERSION=${GLUTEN_BUILD_VERSION} BOLT_BUILD_VERSION=${BOLT_BUILD_VERSION} BUILD_USER=${BUILD_USER} BUILD_CHANNEL=${BUILD_CHANNEL}

debug:
	$(MAKE) build BUILD_TYPE=Debug GLUTEN_BUILD_VERSION=${GLUTEN_BUILD_VERSION} BOLT_BUILD_VERSION=${BOLT_BUILD_VERSION} BUILD_USER=${BUILD_USER} BUILD_CHANNEL=${BUILD_CHANNEL}

RelWithDebInfo:
	$(MAKE) build BUILD_TYPE=RelWithDebInfo GLUTEN_BUILD_VERSION=${GLUTEN_BUILD_VERSION} BUILD_USER=${BUILD_USER} BUILD_CHANNEL=${BUILD_CHANNEL}

clean_cpp:
	rm -rf ${ROOT_DIR}/cpp/build &&\
	rm -f cpp/conan.lock cpp/conaninfo.txt cpp/graph_info.json CMakeCache.txt

install_debug:
	$(MAKE) clean_cpp
	$(MAKE) debug SHARED_LIBRARY=False

install_release:
	$(MAKE) clean_cpp
	$(MAKE) release SHARED_LIBRARY=False

release-with-tests :
	$(MAKE) build BUILD_TYPE=Release GLUTEN_BUILD_VERSION=${GLUTEN_BUILD_VERSION} BOLT_BUILD_VERSION=${BOLT_BUILD_VERSION} BUILD_USER=${BUILD_USER} BUILD_CHANNEL=${BUILD_CHANNEL} BUILD_TESTS=True

debug-with-tests :
	$(MAKE) build BUILD_TYPE=Debug GLUTEN_BUILD_VERSION=${GLUTEN_BUILD_VERSION} BOLT_BUILD_VERSION=${BOLT_BUILD_VERSION} BUILD_USER=${BUILD_USER} BUILD_CHANNEL=${BUILD_CHANNEL} BUILD_TESTS=True

release-with-benchmarks :
	$(MAKE) build BUILD_TYPE=Release GLUTEN_BUILD_VERSION=${GLUTEN_BUILD_VERSION} BOLT_BUILD_VERSION=${BOLT_BUILD_VERSION} B	UILD_USER=${BUILD_USER} BUILD_CHANNEL=${BUILD_CHANNEL} BUILD_BENCHMARKS=True

debug-with-benchmarks :
	$(MAKE) build BUILD_TYPE=Debug  GLUTEN_BUILD_VERSION=${GLUTEN_BUILD_VERSION} BOLT_BUILD_VERSION=${BOLT_BUILD_VERSION} BUILD_USER=${BUILD_USER} BUILD_CHANNEL=${BUILD_CHANNEL} BUILD_BENCHMARKS=True

release-with-tests-and-benchmarks :
	$(MAKE) build BUILD_TYPE=Release GLUTEN_BUILD_VERSION=${GLUTEN_BUILD_VERSION} BOLT_BUILD_VERSION=${BOLT_BUILD_VERSION} BUILD_USER=${BUILD_USER} BUILD_CHANNEL=${BUILD_CHANNEL} BUILD_BENCHMARKS=True BUILD_TESTS=True

debug-with-tests-and-benchmarks :
	$(MAKE) build BUILD_TYPE=Debug GLUTEN_BUILD_VERSION=${GLUTEN_BUILD_VERSION} BOLT_BUILD_VERSION=${BOLT_BUILD_VERSION} BUILD_USER=${BUILD_USER} BUILD_CHANNEL=${BUILD_CHANNEL} BUILD_BENCHMARKS=True BUILD_TESTS=True

arrow:
	bash dev/build_bolt_arrow.sh

# build gluten jar
jar:
	java -version && mvn package -Pbackends-bolt -Pspark-3.3 -Pceleborn -DskipTests -Denforcer.skip=true -Pjava-8 -Ppaimon &&\
	mkdir -p output && \
	rm -rf output/gluten-spark*.jar
	mv package/target/gluten-package-$(GLUTEN_PACKAGE_VERSION).jar output/gluten-spark3.2_2.12-1.0.0-SNAPSHOT-jar-with-dependencies.jar

jar-skip-check:
	java -version && mvn package -Pbackends-bolt -Pspark-3.2 -Pceleborn -DskipTests -Denforcer.skip=true -Pjava-8 -Ppaimon -Dcheckstyle.skip=true -Dspotless.check.skip=true &&\
	mkdir -p output && \
	rm -rf output/gluten-spark*.jar
	mv package/target/gluten-package-$(GLUTEN_PACKAGE_VERSION).jar output/gluten-spark3.2_2.12-1.0.0-SNAPSHOT-jar-with-dependencies.jar

spark32-las:
	java -version && mvn package -Pbackends-bolt -Pspark-3.2-las -Pceleborn -DskipTests -Denforcer.skip=true -Pjava-8 -Ppaimon &&\
	mkdir -p output && \
	rm -rf output/gluten-spark*.jar
	mv package/target/gluten-package-$(GLUTEN_PACKAGE_VERSION).jar output/gluten-spark3.2_2.12-1.0.0-SNAPSHOT-jar-with-dependencies.jar

fast-jar:
	if [ ! -f "output/gluten-spark3.2_2.12-1.0.0-SNAPSHOT-jar-with-dependencies.jar" ] ; then \
		$(MAKE) jar; \
	else \
		jar uf output/gluten-spark3.2_2.12-1.0.0-SNAPSHOT-jar-with-dependencies.jar -C cpp/build/releases/ libbolt_backend.so; \
	fi

zip:
	$(MAKE) jar
	rm -rf output/gluten-spark*.zip
	zip -j output/gluten-spark3.2_2.12-1.0.0-SNAPSHOT-jar-with-dependencies.zip output/gluten-spark3.2_2.12-1.0.0-SNAPSHOT-jar-with-dependencies.jar

fast-zip:
	$(MAKE) fast-jar
	rm -rf output/gluten-spark*.zip
	zip -j output/gluten-spark3.2_2.12-1.0.0-SNAPSHOT-jar-with-dependencies.zip output/gluten-spark3.2_2.12-1.0.0-SNAPSHOT-jar-with-dependencies.jar

jar_spark33:
	java -version && mvn -T32 clean package -Pbackends-bolt -Pspark-3.3 -Pceleborn -Piceberg -DskipTests -Denforcer.skip=true -Ppaimon && \
	mkdir -p output && \
	rm -rf output/gluten-spark*.jar
	mv package/target/gluten-package-$(GLUTEN_PACKAGE_VERSION).jar output/gluten-spark3.3_2.12-1.0.0-SNAPSHOT-jar-with-dependencies.jar

jar_spark34:
	java -version && mvn clean package -Pbackends-bolt -Pspark-3.4 -Pceleborn -Piceberg -DskipTests -Denforcer.skip=true -Ppaimon && \
	mkdir -p output && \
	rm -rf output/gluten-spark*.jar
	mv package/target/gluten-package-$(GLUTEN_PACKAGE_VERSION).jar output/gluten-spark3.4_2.12-1.0.0-SNAPSHOT-jar-with-dependencies.jar

jar_spark35:
	java -version && mvn -T32 package -Pbackends-bolt -Pspark-3.5 -Phadoop-3.2 -Pceleborn -Piceberg -DskipTests -Denforcer.skip=true && \
	mkdir -p output && \
	rm -rf output/gluten-spark*.jar
	mv package/target/gluten-package-$(GLUTEN_PACKAGE_VERSION).jar output/gluten-spark3.5_2.12-1.0.0-SNAPSHOT-jar-with-dependencies.jar

test:
	mvn -Pbackends-bolt -Pspark-3.2 -Pceleborn -Ppaimon package -Denforcer.skip=true

test_spark35:
	mvn -Pbackends-bolt -Pspark-3.5 -Ppaimon -Phadoop-3.2 -Pceleborn -Piceberg package -Denforcer.skip=true -DfailIfNoTests=false -Dexec.skip -Dmaven.test.failure.ignore=true

cpp-test-release: release-with-tests
	cd $(BUILD_DIR)/Release && ctest --timeout 7200 -j $(NUM_THREADS) --output-on-failure -V

cpp-test-debug: debug-with-tests
	cd $(BUILD_DIR)/Debug && ctest --timeout 7200 -j $(NUM_THREADS) --output-on-failure -V

clean :
	$(MAKE) clean_cpp
	mvn clean -Pbackends-bolt -Pspark-3.2 -Pceleborn -Ppaimon -DskipTests -Denforcer.skip=true && \
	rm -rf ${ROOT_DIR}/output/gluten-*.jar

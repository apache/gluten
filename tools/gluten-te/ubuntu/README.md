# Portable Test Environment of Gluten (gluten-te)

Build and run [gluten](https://github.com/apache/gluten) and [gluten-it](https://github.com/apache/gluten/tree/main/tools/gluten-it) in a portable docker container, from scratch.

# Prerequisites

Only Linux and MacOS are currently supported. Before running the scripts, make sure you have `git` and `docker` installed in your host machine.

# Getting Started (Build Gluten code, Velox backend)

```sh
git clone -b main https://github.com/apache/gluten.git gluten # Gluten main code

export HTTP_PROXY_HOST=myproxy.example.com # in case you are behind http proxy
export HTTP_PROXY_PORT=55555 # in case you are behind http proxy

cd gluten/
tools/gluten-te/ubuntu/examples/buildhere-veloxbe/run.sh
```

# Getting Started (TPC, Velox backend)

```sh
git clone -b main https://github.com/apache/gluten.git gluten # Gluten main code

export HTTP_PROXY_HOST=myproxy.example.com # in case you are behind http proxy
export HTTP_PROXY_PORT=55555 # in case you are behind http proxy

cd gluten/gluten-te
./tpc.sh
```

# Configurations

See the [config file](https://github.com/apache/gluten/blob/main/tools/gluten-te/ubuntu/defaults.conf). You can modify the file to configure gluten-te, or pass env variables during running the scripts.

# Example Usages

## Example: Build local Gluten code (Velox backend)

```sh
cd gluten/
{PATH_TO_GLUTEN_TE}/examples/buildhere-veloxbe/run.sh
```

## Example: Build local Gluten code behind a http proxy (Velox backend)

```sh
export HTTP_PROXY_HOST=myproxy.example.com # in case you are behind http proxy
export HTTP_PROXY_PORT=55555 # in case you are behind http proxy

cd gluten/
{PATH_TO_GLUTEN_TE}/examples/buildhere-veloxbe/run.sh
```

## Example: Run specific maven commands

```sh
cd gluten/
{PATH_TO_GLUTEN_TE}/cbash-mount.sh mvn clean dependency:tree
```

## Example: Run GUI-based IDEs

```sh
export HTTP_PROXY_HOST=myproxy.example.com # in case you are behind http proxy
export HTTP_PROXY_PORT=55555 # in case you are behind http proxy

cd gluten/
{PATH_TO_GLUTEN_TE}/examples/buildhere-veloxbe-dev/run.sh

# In a new host shell, run below command to setup a X11-enabled ssh
# connection (default password: 123). 
#
# {DOCKER_CONTAINER_IP} can be found via "docker ps" and "docker inspect",
# or by checking "ifconfig" results inside the container's shell 
ssh -X root@{DOCKER_CONTAINER_IP}
```

## Example: Build and run TPC benchmark on non-default remote branches of Gluten (Velox backend)

```sh
export HTTP_PROXY_HOST=myproxy.example.com # in case you are behind http proxy
export HTTP_PROXY_PORT=55555 # in case you are behind http proxy

TARGET_GLUTEN_REPO=my_repo \
TARGET_GLUTEN_BRANCH=my_branch \
{PATH_TO_GLUTEN_TE}/tpc.sh
```

## Example: Build and run TPC benchmark on official latest code behind a http proxy (Velox backend)

```sh
export HTTP_PROXY_HOST=myproxy.example.com # in case you are behind http proxy
export HTTP_PROXY_PORT=55555 # in case you are behind http proxy

{PATH_TO_GLUTEN_TE}/tpc.sh
```

## Example: Create debug build for all codes, and open a GDB debugger interface during running gluten-it (Velox backend)

```sh
DEBUG_BUILD=ON \
RUN_GDB=ON \
{PATH_TO_GLUTEN_TE}/tpc.sh
```

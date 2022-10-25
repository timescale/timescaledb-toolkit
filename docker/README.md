# Docker images

To speed up builds, we are using a set of pre-build docker images and
the Docker files for that is present in this directory.

## Pre-requisites

You need to have Docker installed with support for DockerKit multi-platform
and activate it by setting environment variable `DOCKER_BUILDKIT=1`.

```bash
apt-get install docker.io
```

## Building multi-platform images

To build a new Docker image `rust-pgx` for multiple platforms and push
it to the development repository:

```bash
ARCH=amd64
OS_NAME=debian
OS_VERSION=11
OS_CODE_NAME=bullseye
DOCKER_BUILDKIT=1 docker build --platform $ARCH --build-arg ARCH=$ARCH --build-arg OS_NAME=$OS_NAME --build-arg OS_VERSION=$OS_VERSION --build-arg OS_CODE_NAME=$OS_CODE_NAME -f docker/ci/Dockerfile -t timescaledev/toolkit-builder-test:$OS_NAME-$OS_VERSION-$ARCH .
docker build --tag timescaledev/rust-pgx-test:latest --push .
```

We publish the images as `timescaledev/toolkit-builder` instead of
`timescaledev/toolkit-builder-test` after testing.

## Troubleshooting

If you get the following error when pushing:

```
$ docker buildx build --platform linux/arm64/v8,linux/amd64 --tag timescaledev/rust-pgx-test:latest --push .
[+] Building 487.0s (54/54) FINISHED                                                                                                                                                                          
 => [internal] load .dockerignore                                                                                                                                                                        0.0s
 => => transferring context: 2B                                                                                                                                                                          0.0s 
    .
    .
    .
=> [auth] timescaledev/rust-pgx-test:pull,push token for registry-1.docker.io                                                                                                                           0.0s 
------
 > exporting to image:
------
error: failed to solve: failed to fetch oauth token: Post "https://auth.docker.io/token": x509: certificate has expired or is not yet valid: current time 2022-07-28T07:19:52+01:00 is after 2018-04-29T13:06:19Z
```

You may have better luck with buildx instead of BuildKit.
Install from https://github.com/docker/buildx and then:

```bash
export DOCKER_BUILDKIT=0
docker buildx build --platform linux/arm64/v8,linux/amd64 --tag timescaledev/rust-pgx-test:latest --push .
```

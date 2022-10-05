# Docker images

To speed up builds, we are using a set of pre-build docker images and
the Docker files for that is present in this directory.

## Pre-requisites

You need to have Docker installed with support for multi-platform
build to use `buildx`. It is available in `docker.io` package on Ubuntu:

```bash
apt-get install docker.io
```

## Building multi-platform images

To build a new Docker image `rust-pgx` for multiple platforms and push
it to the development repository:

```bash
docker buildx build --platform linux/arm64/v8,linux/amd64 --tag timescaledev/rust-pgx:latest --push .
```

There is a test repository `timescaledev/rust-pgx-test` available as
well that you can use if you want to test changes to the docker image.

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

You probably have build toolkit enabled and it can cause issues. You
can disable it and try again:

```bash
export DOCKER_BUILDKIT=0
docker buildx build --platform linux/arm64/v8,linux/amd64 --tag timescaledev/rust-pgx-test:latest --push .
```

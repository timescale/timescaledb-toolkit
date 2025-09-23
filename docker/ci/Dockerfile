ARG ARCH
ARG OS_NAME
ARG OS_VERSION

# Without DockerKit, this doesn't work, even though documentation suggests it should.
# With DockerKit, TARGETARCH is supposed to come in for free, but that doesn't work either.
FROM --platform=${ARCH} ${OS_NAME}:${OS_VERSION} AS toolkit-base

ARG ARCH
ARG OS_CODE_NAME
# Docker requires we redeclare these after FROM ¯\_(ツ)_/¯
ARG OS_NAME
ARG OS_VERSION

ENV HOME /home/postgres

# Docker fails to set LOGNAME :(
ENV LOGNAME=root
ENV CARGO_HOME=/usr/local/cargo
ENV RUSTUP_HOME?/usr/local/rustup
ENV PATH?"${CARGO_HOME}/bin:/sbin:/usr/sbin:/bin:/usr/bin:/usr/local/bin"

COPY docker/ci/setup.sh /
COPY tools/dependencies.sh /
# TODO simple option processing a la build and testbin would make this less error-prone
RUN /setup.sh ${ARCH} ${OS_NAME} ${OS_VERSION} "${OS_CODE_NAME}" postgres ${HOME}

# TODO What does this 'AS' do?  It doesn't seem to name it.  We need -t for that.
FROM toolkit-base AS toolkit-builder

WORKDIR ${HOME}
# Leave USER root for Github Actions.

#!/bin/bash

# simple upgrade script generator
# this will generate an upgrade script that drops the experimental schema then
# reruns all the commands in the CREATE EXTENSION script. This only works while
# all our features are experimental. We'll need a better strategy by the time we
# have stable features.

set -eu -o pipefail
shopt -s failglob

PG_CONFIG=${1}
FROM=${2}
TO=${3}

EXT_DIR=$(${PG_CONFIG} --sharedir)/extension
EXTENSION_FILE="${EXT_DIR}/timescale_analytics--${3}.sql"
UPGRADE_FILE="${EXT_DIR}/timescale_analytics--${2}--${3}.sql"


echo "DROP SCHEMA timescale_analytics_experimental CASCADE;" > ${UPGRADE_FILE}
# also drop the EVENT TRIGGERs; there's no CREATE OR REPLACE for those
echo "DROP EVENT TRIGGER disallow_experimental_deps CASCADE;" >> ${UPGRADE_FILE}
echo "DROP EVENT TRIGGER disallow_experimental_dependencies_on_views CASCADE;" >> ${UPGRADE_FILE}

cat ${EXTENSION_FILE} >> ${UPGRADE_FILE}

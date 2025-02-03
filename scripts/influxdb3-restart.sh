#!/usr/bin/env bash

DEFAULT_INFLUXDB_V3_VERSION="latest"
INFLUXDB_V3_VERSION="${INFLUXDB_V3_VERSION:-$DEFAULT_INFLUXDB_V3_VERSION}"
DOCKER_REPOSITORY="quay.io/influxdb"
INFLUXDB_V3_IMAGE=${DOCKER_REPOSITORY}/influxdb3-core:${INFLUXDB_V3_VERSION}
CONTAINER_NAME="${INFLUX_CONTAINER_NAME:-influxdb_v3}"
NETWORK_NAME=influxdb3_network

SCRIPT_PATH="$( cd "$(dirname "$0")" || exit ; pwd -P )"
PROJECT_PATH="$(dirname "${SCRIPT_PATH}")"
DATA_DIR="${INFLUXDB3_DATA_DIR:-${PROJECT_PATH}/temp/data}"
HOST_NAME=$(uname -n)
WRITER_ID="${HOST_NAME:-darkstar}_$(date +%s)"

generate_tokens() {
  echo "(RE)Generating Influxdb3 tokens"
  BASE_RESULT=$(docker run --name influxdb3_gen_token quay.io/influxdb/influxdb3-core create token | head -n 2 | sed ':a;N;$!ba;s/\n/#/g')
  TOKEN="$(echo "$BASE_RESULT" | sed s/\#.*$//g | sed s/^Token:\ //)"
  HASHED_TOKEN="$(echo "$BASE_RESULT" | sed s/^.*\#//g | sed s/Hashed\ Token:\ //)"
  echo "export INFLUXDB_TOKEN=${TOKEN}" > "${SCRIPT_PATH}"/influxdb3_current.token
  echo "export INFLUXDB_TOKEN_HASH=${HASHED_TOKEN}" >> "${SCRIPT_PATH}"/influxdb3_current.token
  echo tokens can be found in "${SCRIPT_PATH}"/influxdb3_current.token
  docker rm influxdb3_gen_token
}

make_data_dir(){
   echo making data directory ${DATA_DIR}
   mkdir -p "${DATA_DIR}"
   chmod 777 "${DATA_DIR}"
   ls -al ${DATA_DIR}
   echo File based data will be written to "$DATA_DIR"
}

listening_check(){
  echo Waiting for OSS3 server response at port 8181
  NOW=$(date +%s)
  TTL=$((NOW+30))
  while (( NOW < TTL )) && ! echo test | nc localhost 8181 > /dev/null
  do
    printf "."
    sleep 5
    NOW=$((NOW+5))
  done
  if ((NOW >= TTL))
  then
    echo
    echo failed to get response from 8181 in 30 seconds
    echo OSS3 server may not be available
  else
    echo OSS3 server responded on port 8181
  fi
}

restart() {
  echo using image "${INFLUXDB_V3_IMAGE}"
  if [ ! -f "${SCRIPT_PATH}"/influxdb3_current.token ]
  then
    generate_tokens
  fi

  # shellcheck disable=SC1091
  source "${SCRIPT_PATH}/influxdb3_current.token"
  echo Token for accessing the api: "${INFLUXDB_TOKEN}"
  make_data_dir

  docker kill "${CONTAINER_NAME}" || true
  docker rm "${CONTAINER_NAME}" || true
  docker network rm "${NETWORK_NAME}" || true
  docker network create -d bridge "${NETWORK_NAME}" --subnet 192.168.0.0/24 --gateway 192.168.0.1 || true

  #
  # InfluxDB 3.0
  #
  docker pull "${INFLUXDB_V3_IMAGE}" || true

  echo running "${INFLUXDB_V3_IMAGE}" as "${CONTAINER_NAME}"

  docker run \
    --detach \
    --name "${CONTAINER_NAME}" \
    --network "${NETWORK_NAME}" \
    --volume "${DATA_DIR}":/var/lib/influxdb3 \
    --publish 8181:8181 \
    "${INFLUXDB_V3_IMAGE}" \
    serve \
    --node-id "${WRITER_ID}" \
    --object-store file \
    --data-dir /var/lib/influxdb3 \
    --bearer-token "${INFLUXDB_TOKEN_HASH}"

  listening_check
}

help(){
  echo This script restarts influxdb3 OSS docker
  echo ""
  echo To regenerate tokens use the command:
  echo $ "${0}" tokens
  echo ""
  echo The API token will be stored in "${SCRIPT_PATH}"/influxdb3_current.token
  echo ""
  echo Otherwise, this script automatically recreates an influxdb3 OSS container named "${CONTAINER_NAME}"
  echo ""
  echo To reset the data directory \("${DATA_DIR}"\) from scratch,
  echo 1\) kill the "${CONTAINER_NAME}" container: $ docker kill "${CONTAINER_NAME}"
  echo 2\) try: $ sudo rm -rdf "${DATA_DIR}"
  echo ""
}

case $1 in
   tokens) generate_tokens;;
   help|--help|-h|-?|?) help;;
   check) listening_check;;
   *) restart;;
esac

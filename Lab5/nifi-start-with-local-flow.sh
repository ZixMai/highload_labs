#!/bin/sh
set -eu

LOCAL_FLOW_DIR="/opt/nifi/flow-local"
LOCAL_FLOW_FILE="${LOCAL_FLOW_DIR}/flow.json.gz"
NIFI_FLOW_FILE="${NIFI_HOME}/conf/flow.json.gz"

copy_local_into_nifi() {
    if [ -f "${LOCAL_FLOW_FILE}" ]; then
        cp "${LOCAL_FLOW_FILE}" "${NIFI_FLOW_FILE}"
    fi
}

sync_nifi_back_to_local() {
    while true; do
        sleep 5
        if [ -f "${NIFI_FLOW_FILE}" ]; then
            tmp_file="${LOCAL_FLOW_FILE}.tmp"
            cp "${NIFI_FLOW_FILE}" "${tmp_file}"
            mv "${tmp_file}" "${LOCAL_FLOW_FILE}"
        fi
    done
}

copy_local_into_nifi
sync_nifi_back_to_local &
sync_pid=$!

/opt/nifi/scripts/start.sh
status=$?

kill "${sync_pid}" 2>/dev/null || true
wait "${sync_pid}" 2>/dev/null || true

exit "${status}"

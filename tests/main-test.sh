#!/bin/bash

#set -ex
set -o errexit
set -o pipefail

TEST_TEXT="HELLO WORLD"
TEST_TEXT_FILE=test-tgfs.txt
TEST_DIR=testdir
ALT_TEST_TEXT_FILE=test-tgfs-ALT.txt
TEST_TEXT_FILE_LENGTH=15
TEMP_DIR="${TMPDIR:-"/var/tmp"}"

export SED_BIN="sed"
export SED_BUFFER_FLAG="--unbuffered"

function get_size() {
    if [ "$(uname)" = "Darwin" ]; then
        stat -f "%z" "$1"
    else
        stat -c %s "$1"
    fi
}

function check_file_size() {
    local FILE_NAME="$1"
    local EXPECTED_SIZE="$2"

    # Verify file is zero length via metadata
    local size
    size=$(get_size "${FILE_NAME}")
    if [ "${size}" -ne "${EXPECTED_SIZE}" ]
    then
        echo "error: expected ${FILE_NAME} to be zero length"
        return 1
    fi

    # Verify file is zero length via data
    size=$(wc -c < "${FILE_NAME}")
    if [ "${size}" -ne "${EXPECTED_SIZE}" ]
    then
        echo "error: expected ${FILE_NAME} to be ${EXPECTED_SIZE} length, got ${size}"
        return 1
    fi
}

function mk_test_file {
    if [ $# = 0 ]; then
        local TEXT="${TEST_TEXT}"
    else
        local TEXT="$1"
    fi
    echo "${TEXT}" > "${TEST_TEXT_FILE}"
    if [ ! -e "${TEST_TEXT_FILE}" ]
    then
        echo "Could not create file ${TEST_TEXT_FILE}, it does not exist"
        exit 1
    fi

    # wait & check
    local BASE_TEXT_LENGTH; BASE_TEXT_LENGTH=$(echo "${TEXT}" | wc -c | awk '{print $1}')
    local TRY_COUNT=10
    while true; do
        local MK_TEXT_LENGTH
        MK_TEXT_LENGTH=$(wc -c "${TEST_TEXT_FILE}" | awk '{print $1}')
        if [ "${BASE_TEXT_LENGTH}" -eq "${MK_TEXT_LENGTH}" ]; then
            break
        fi
        local TRY_COUNT=$((TRY_COUNT - 1))
        if [ "${TRY_COUNT}" -le 0 ]; then
            echo "Could not create file ${TEST_TEXT_FILE}, that file size is something wrong"
        fi
        sleep 1
    done
}

function rm_test_file {
    if [ $# = 0 ]; then
        local FILE="${TEST_TEXT_FILE}"
    else
        local FILE="$1"
    fi
    rm -f "${FILE}"

    if [ -e "${FILE}" ]
    then
        echo "Could not cleanup file ${TEST_TEXT_FILE}"
        exit 1
    fi
}

function mk_test_dir {
    mkdir "${TEST_DIR}"

    if [ ! -d "${TEST_DIR}" ]; then
        echo "Directory ${TEST_DIR} was not created"
        exit 1
    fi
}

function rm_test_dir {
    rmdir "${TEST_DIR}"
    if [ -e "${TEST_DIR}" ]; then
        echo "Could not remove the test directory, it still exists: ${TEST_DIR}"
        exit 1
    fi
}

function cd_run_dir {
    if [ "${TEST_MOUNT_POINT}" = "" ]; then
        echo "TEST_MOUNT_POINT variable not set"
        exit 1
    fi
    RUN_DIR="${TEST_MOUNT_POINT}/${1}"
    mkdir -p "${RUN_DIR}"
    cd "${RUN_DIR}"
}

function clean_run_dir {
    echo "Remove run dir ${RUN_DIR}"
    if [ -d "${RUN_DIR}" ]; then
        rm -rf "${RUN_DIR}" || echo "Error removing ${RUN_DIR}"
    fi
}

function init_suite {
    TEST_LIST=()
    TEST_FAILED_LIST=()
    TEST_PASSED_LIST=()
}

function report_pass {
    echo "$1 passed"
    TEST_PASSED_LIST+=("$1")
}

function report_fail {
    echo "$1 failed"
    TEST_FAILED_LIST+=("$1")
}

function add_tests {
    TEST_LIST+=("$@")
}

function describe {
    echo "${FUNCNAME[1]}: \"$*\""
}

function run_suite {
   orig_dir="${PWD}"
   key_prefix="testrun-${RANDOM}"
   cd_run_dir "${key_prefix}"
   for t in "${TEST_LIST[@]}"; do
       # Ensure test input name differs every iteration
       TEST_TEXT_FILE="test-tgfs-${RANDOM}.txt"
       TEST_DIR="testdir-${RANDOM}"
       ALT_TEST_TEXT_FILE="test-tgfs-ALT-${RANDOM}.txt"
       set +o errexit
       (set -o errexit; $t $key_prefix)
       if [ $? == 0 ]; then
           report_pass "${t}"
       else
           report_fail "${t}"
       fi
       set -o errexit
   done
   cd "${orig_dir}"
   clean_run_dir

   for t in "${TEST_PASSED_LIST[@]}"; do
       echo "PASS: ${t}"
   done
   for t in "${TEST_FAILED_LIST[@]}"; do
       echo "FAIL: ${t}"
   done

   local passed=${#TEST_PASSED_LIST[@]}
   local failed=${#TEST_FAILED_LIST[@]}

   echo "SUMMARY for $0: ${passed} tests passed.  ${failed} tests failed."

   if [[ "${failed}" != 0 ]]; then
       return 1
   else
       return 0
   fi
}

function test_create_empty_file {
    describe "Testing creating an empty file ..."

    local OBJECT_NAME; OBJECT_NAME=$(basename "${PWD}")/"${TEST_TEXT_FILE}"

    touch "${TEST_TEXT_FILE}"

    check_file_size "${TEST_TEXT_FILE}" 0

    rm_test_file
}

function test_append_file {
    describe "Testing append to file ..."
    local TEST_INPUT="echo ${TEST_TEXT} to ${TEST_TEXT_FILE}"

    # Write a small test file
    for x in $(seq 1 "${TEST_TEXT_FILE_LENGTH}"); do
        echo "${TEST_INPUT}"
    done > "${TEST_TEXT_FILE}"

    check_file_size "${TEST_TEXT_FILE}" $((TEST_TEXT_FILE_LENGTH * $((${#TEST_INPUT} + 1)) ))

    rm_test_file
}

function test_truncate_file {
    describe "Testing truncate file ..."
    # Write a small test file
    echo "${TEST_TEXT}" > "${TEST_TEXT_FILE}"

    # Truncate file to 0 length.  This should trigger open(path, O_RDWR | O_TRUNC...)
    : > "${TEST_TEXT_FILE}"

    check_file_size "${TEST_TEXT_FILE}" 0

    rm_test_file
}

function test_mv_file {
    describe "Testing mv file function ..."
    # if the rename file exists, delete it
    if [ -e "${ALT_TEST_TEXT_FILE}" ]
    then
       rm "${ALT_TEST_TEXT_FILE}"
    fi

    if [ -e "${ALT_TEST_TEXT_FILE}" ]
    then
       echo "Could not delete file ${ALT_TEST_TEXT_FILE}, it still exists"
       return 1
    fi

    # create the test file again
    mk_test_file

    # save file length
    local ALT_TEXT_LENGTH; ALT_TEXT_LENGTH=$(wc -c "${TEST_TEXT_FILE}" | awk '{print $1}')

    #rename the test file
    mv "${TEST_TEXT_FILE}" "${ALT_TEST_TEXT_FILE}"
    if [ ! -e "${ALT_TEST_TEXT_FILE}" ]
    then
       echo "Could not move file"
       return 1
    fi
    
    #check the renamed file content-type
    if [ -f "/etc/mime.types" ]
    then
      check_content_type "$1/${ALT_TEST_TEXT_FILE}" "text/plain"
    fi

    # Check the contents of the alt file
    local ALT_FILE_LENGTH; ALT_FILE_LENGTH=$(wc -c "${ALT_TEST_TEXT_FILE}" | awk '{print $1}')
    if [ "$ALT_FILE_LENGTH" -ne "$ALT_TEXT_LENGTH" ]
    then
       echo "moved file length is not as expected expected: $ALT_TEXT_LENGTH  got: $ALT_FILE_LENGTH"
       return 1
    fi

    # clean up
    rm_test_file "${ALT_TEST_TEXT_FILE}"
}

function test_mv_empty_directory {
    describe "Testing mv directory function ..."
    if [ -e "${TEST_DIR}" ]; then
       echo "Unexpected, this file/directory exists: ${TEST_DIR}"
       return 1
    fi

    mk_test_dir

    mv "${TEST_DIR}" "${TEST_DIR}_rename"
    if [ ! -d "${TEST_DIR}_rename" ]; then
       echo "Directory ${TEST_DIR} was not renamed"
       return 1
    fi

    rmdir "${TEST_DIR}_rename"
    if [ -e "${TEST_DIR}_rename" ]; then
       echo "Could not remove the test directory, it still exists: ${TEST_DIR}_rename"
       return 1
    fi
}

function test_mv_nonempty_directory {
    describe "Testing mv directory function ..."
    if [ -e "${TEST_DIR}" ]; then
       echo "Unexpected, this file/directory exists: ${TEST_DIR}"
       return 1
    fi

    mk_test_dir

    touch "${TEST_DIR}"/file

    mv "${TEST_DIR}" "${TEST_DIR}_rename"
    if [ ! -d "${TEST_DIR}_rename" ]; then
       echo "Directory ${TEST_DIR} was not renamed"
       return 1
    fi

    rm -r "${TEST_DIR}_rename"
    if [ -e "${TEST_DIR}_rename" ]; then
       echo "Could not remove the test directory, it still exists: ${TEST_DIR}_rename"
       return 1
    fi
}

function test_redirects {
    describe "Testing redirects ..."

    mk_test_file "ABCDEF"

    local CONTENT; CONTENT=$(cat "${TEST_TEXT_FILE}")

    if [ "${CONTENT}" != "ABCDEF" ]; then
       echo "CONTENT read is unexpected, got ${CONTENT}, expected ABCDEF"
       return 1
    fi

    echo "XYZ" > "${TEST_TEXT_FILE}"

    CONTENT=$(cat "${TEST_TEXT_FILE}")

    if [ "${CONTENT}" != "XYZ" ]; then
       echo "CONTENT read is unexpected, got ${CONTENT}, expected XYZ"
       return 1
    fi

    echo "123456" >> "${TEST_TEXT_FILE}"

    local LINE1; LINE1=$("${SED_BIN}" -n '1,1p' "${TEST_TEXT_FILE}")
    local LINE2; LINE2=$("${SED_BIN}" -n '2,2p' "${TEST_TEXT_FILE}")

    if [ "${LINE1}" != "XYZ" ]; then
       echo "LINE1 was not as expected, got ${LINE1}, expected XYZ"
       return 1
    fi

    if [ "${LINE2}" != "123456" ]; then
       echo "LINE2 was not as expected, got ${LINE2}, expected 123456"
       return 1
    fi

    # clean up
    rm_test_file
}

function test_mkdir_rmdir {
    describe "Testing creation/removal of a directory ..."

    if [ -e "${TEST_DIR}" ]; then
       echo "Unexpected, this file/directory exists: ${TEST_DIR}"
       return 1
    fi

    mk_test_dir
    rm_test_dir
}

function test_list {
    describe "Testing list ..."
    mk_test_file
    mk_test_dir

    local file_list=(*)
    local file_cnt=${#file_list[@]}
    if [ "${file_cnt}" -ne 2 ]; then
        echo "Expected 2 file but got ${file_cnt}"
        return 1
    fi

    rm_test_file
    rm_test_dir
}

function test_remove_nonempty_directory {
    describe "Testing removing a non-empty directory ..."
    mk_test_dir
    touch "${TEST_DIR}/file"
    (
        set +o pipefail
        rmdir "${TEST_DIR}" 2>&1 | grep -q "Directory not empty"
    )
    rm "${TEST_DIR}/file"
    rm_test_dir
}

function test_rm_rf_dir {
   describe "Test that rm -rf will remove directory with contents ..."
   # Create a dir with some files and directories
   mkdir dir1
   mkdir dir1/dir2
   touch dir1/file1
   touch dir1/dir2/file2

   # Remove the dir with recursive rm
   rm -rf dir1

   if [ -e dir1 ]; then
       echo "rm -rf did not remove $PWD/dir1"
       return 1
   fi
}

function test_copy_file {
   describe "Test simple copy ..."

   dd if=/dev/urandom of=/tmp/simple_file bs=1024 count=1
   cp /tmp/simple_file copied_simple_file
   cmp /tmp/simple_file copied_simple_file

   rm_test_file /tmp/simple_file
   rm_test_file copied_simple_file
}

function test_write_after_seek_ahead {
   describe "Test writes succeed after a seek ahead ..."
   dd if=/dev/zero of=testfile seek=1 count=1 bs=1024
   rm_test_file testfile
}

function test_overwrite_existing_file_range {
    describe "Test overwrite range succeeds ..."
    dd if=<(seq 1000) of="${TEST_TEXT_FILE}"
    dd if=/dev/zero of="${TEST_TEXT_FILE}" seek=1 count=1 bs=1024 conv=notrunc
    cmp "${TEST_TEXT_FILE}" <(
        seq 1000 | head -c 1024
        dd if=/dev/zero count=1 bs=1024
        seq 1000 | tail -c +2049
    )
    rm_test_file
}

function test_concurrent_directory_updates {
    describe "Test concurrent updates to a directory ..."
    for i in $(seq 5); do
        echo foo > "${i}"
    done
    for _ in $(seq 10); do
        for i in $(seq 5); do
            local file
            file=$(ls $(seq 5) | "${SED_BIN}" -n "$((RANDOM % 5 + 1))p")
            cat "${file}" >/dev/null || true
            rm -f "${file}"
            echo "foo" > "${file}" || true
        done &
    done
    wait
    rm -f $(seq 5)
}

function test_open_second_fd {
    describe "read from an open fd ..."
    rm_test_file second_fd_file

    local RESULT
    RESULT=$( (echo foo ; wc -c < second_fd_file >&2) 2>& 1>second_fd_file)
    if [ "${RESULT}" -ne 4 ]; then
        echo "size mismatch, expected: 4, was: ${RESULT}"
        return 1
    fi
    rm_test_file second_fd_file
}

function add_all_tests {
    add_tests test_create_empty_file
    add_tests test_append_file
    add_tests test_truncate_file
    add_tests test_mv_file
    add_tests test_mv_empty_directory
    add_tests test_mv_nonempty_directory
    add_tests test_redirects
    add_tests test_mkdir_rmdir
    add_tests test_list
    add_tests test_remove_nonempty_directory
    add_tests test_rm_rf_dir
    add_tests test_copy_file
    add_tests test_write_after_seek_ahead
    add_tests test_overwrite_existing_file_range
    add_tests test_concurrent_directory_updates
    add_tests test_open_second_fd
}

init_suite
add_all_tests
run_suite
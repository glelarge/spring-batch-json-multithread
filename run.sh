#!/bin/bash

# Run a loop to execute the batch.
# If an error is detected in the produced JSON file, the script will exit with status 1.

loop_count=100

# Uncomment following line to get the error, or export from the shell
# export MYBATCH_TRANSACTIONAL=true

for i in $(seq 1 ${loop_count})
do
    echo ""
    echo "================================ loop ${i}"
    java -jar ./target/spring-batch-json-multithread-1.0.0.jar

    # Get the filename of the last produced JSON file
    json_file=$(ls mydata_*.json | tail -n 1)

    # If the command "grep -rn "^,$" mydata_YYYYMMDD_hhmmssSSS.json" returns any result, then the script will exit with status 1
    if grep -rn "^,$" ${json_file}
    then
        echo "Error: Found empty line in the json file ${json_file}"
        log_file="${json_file}____ko.log"
        echo "Renaming output.log to $log_file"
        # Rename output.log to match the produced JSON file with .log extension
        mv output.log "$log_file"
        exit 1
    else
        log_file="${json_file}.log"
        echo "Renaming output.log to $log_file"
        mv output.log "$log_file"
    fi

done

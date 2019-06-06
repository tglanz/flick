#!/bin/bash

env_file=
start_cluster=
stop_cluster=
remake=
example=

function error {
    exit_code=$2
    if [ -z "$exit_code" ]; then
        exit_code=-1
    fi
    echo "Error: $1" 1>&2 && exit $exit_code
}

function show_help {
    echo "Usage: $0 [OPTIONS]"
    echo ""
    echo "Options:"
    echo "    -h, --help            Display this message"
    echo "    --env=<path>          Load environment file from <path>"
    echo "    --remake              Remake maven project, located at PROJECT_HOME. Equivalent to ~ \"mvn clean package\""
    echo ""    
    echo "    --start-cluster       Start a flink cluster, whom binaries are at FLINK_HOME"
    echo "    --stop-cluster        Stop a flink cluster, whom binaries are at FLINK_HOME"
    echo "    --example=<example>   Run the jar, located, by maven convention under the project at PROJECT_HOME"
    echo "                          with hardcoded parameters for a specific example"
    echo ""
    echo "Notes"
    echo " - The order of arguments doesn't indicate the order of execution"
    echo " - Data items will be read from DATA_HOME"
    echo " - Output items will be stored at OUTPUT_HOME"
    echo ""
    echo "Examples"
    echo "    $0 --remake ~/flink/applications"
    echo "    $0 --env=.env --stop-cluster --start-cluster --run=./applications"
}

while [ $# -gt 0 ]; do
    key=$1
    shift

    case $key in
        --env)
            env_file=$1
            shift
            ;;
        --env=*)
            env_file="${key#*=}"
            ;;
        --remake)
            remake=true
            ;;
        --start-cluster)
            start_cluster=true
            ;;
        --stop-cluster)
            stop_cluster=true
            ;;
        --example)
            example=$1
            shift
            ;;
        --example=*)
            example="${key#*=}"
            ;;
        --help|-h)
            show_help
            exit 0
    esac
done

if [ ! -z "$env_file" ]; then
    echo "Sourcing env"
    echo " - env file: $env_file"
    source $env_file
fi

echo "Checking environment"
for arg in "FLINK_HOME" "PROJECT_HOME" "DATA_HOME" "OUTPUT_HOME"; do
    value=${!arg}
    if [ -z "$value" ]; then
        error "missing environment variable: $arg"
    fi
    echo " - $arg=$value"
done


if [ "$remake" = true ]; then
    echo "Remaking"
    echo " - directory: $PROJECT_HOME"
    cd $PROJECT_HOME
    mvn clean package
    cd -
fi

if [ "$stop_cluster"  = true ]; then
    echo "Stopping cluster"
    $FLINK_HOME/bin/stop-cluster.sh
fi

if [ "$start_cluster" = true ]; then
    echo "Starting cluster"
    $FLINK_HOME/bin/start-cluster.sh
fi

if [ ! -z "$example" ]; then
    case $example in
        word-count|wc)
            $FLINK_HOME/bin/flink run $PROJECT_HOME/target/tglanz.flink.applications-0.0.1.jar \
                --input $DATA_HOME/wordcount-example.txt
            ;;
        *)
            error "Unknown example: $example"
    esac
fi
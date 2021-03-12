#!/bin/bash

##################################################################################
#
# The goal of this script is to allow quick setup of a blank local multi node
# cluster for development testing without needing to erase or interfere with
# previous testing. It also enables redeployment of the code for such testing
# clusters without erasing the data previously indexed.
#
# It is for dev testing only NOT for production use.
#
# This is also NOT meant to be run from this directory within a lucene-solr
# working copy. Typical usage is to copy it out to a separate workspace
# such as (<GIT_CHECKOUT>/../testing) and edit then either use the -w option
# or edit the definition of DEFAULT_VCS_WORKSPACE variable below.
#
# Usage:
#    ./cloud.sh <command> [options] [name]
#
# Options:
#  -c                clean the data & zk collections erasing all indexed data
#  -r                recompile server with 'ant clean server create-package'
#  -m <mem>          memory per node
#  -a <args>         additional JVM options
#  -n <num>          number of nodes to create/start if this doesn't match error
#  -w <path>         path to the vcs checkout
#  -z <num>          port to look for zookeeper on (2181 default)
#  -d <url>          Download solr tarball from this URL
#
# Commands:
#   new              Create a new cluster named by the current date or [name]
#   start            Start an existing cluster specified by [name]
#   stop             stop the cluster specified by [name]
#   restart          stop and then start
#
# In all cases if [name] is unspecified ls -t will be used to determine the
# most recent cluster working directory, and that will be used. If it is
# specified it will be resolved as a path from the directory where cloud.sh
# has been run.
#
# By default the script sets up a local Solr cloud with 4 nodes, in a local
# directory with ISO date as the name. A local zookeeper at 2181 or the
# specified port is presumed to be available, a new zk chroot is used for each
# cluster based on the file system path to the cluster directory. the default
# solr.xml is added to this solr root dir in zookeeper.
#
# Debugging ports are automatically opened for each node starting with port 5001
#
# Specifying an explicit destination path will cause the script to
# use that path and a zk chroot that matches, so more than one install
# can be created in a day, or issue numbers etc can be used. Normally the
# directories containing clusters created by this tool are in the same
# directory as this script. Distant  paths with slashes or funny characters
# *might* work, but are not well tested, YMMV.
#
# PEREQ: 1. Zookeeper on localhost:2181 (or as specified by -z option) where
#           it is ok to create a lot of top level directories named for
#           the absolute path of the [name] directory (for example:
#           /solr_home_myuser_projects_solr_testing_2019-01-01) Note
#           that not using the embedded zookeeper is key to being able
#           switch between testing setups and to test vs alternate versions
#           of zookeeper if desired.
#
# SETUP: 1. Place this script in a directory intended to hold all your
#           testing installations of solr.
#        2. Edit DEFAULT_VCS_WORKSPACE if the present value does not suit
#           your purposes.
#        3. chmod +x cloud.sh
#
# EXAMPLES:
#
# Create a brand new 4 node cluster deployed in a directory named for today
#
#   ./cloud.sh new
#
# Create a brand new 4 node cluster deployed in a directory named SOLR-1234567
#
#   ./cloud.sh new SOLR-1234567
#
# Stop the cluster
#
#   ./cloud.sh stop
#
# Compile and push new code to a running cluster (incl bounce the cluster)
#
#   ./cloud.sh restart -r
#
# Dump your hoplessly fubar'd test collections and start fresh with current tarball
#
#   ./cloud.sh restart -c
#
##################################################################################

DEFAULT_VCS_WORKSPACE='../code/lucene-solr'

############## Normally  no need to edit below this line ##############

##############
# Parse Args #
##############

COMMAND=$1
shift

CLEAN=false      # default
MEMORY=1g        # default
JVM_ARGS=''      # default
RECOMPILE=false  # default
NUM_NODES=0      # need to detect if not specified
VCS_WORK=${DEFAULT_VCS_WORKSPACE}
ZK_PORT=2181

while getopts ":crm:a:n:w:z:d:" opt; do
  case ${opt} in
    c)
      CLEAN=true
      ;;
    r)
      RECOMPILE=true
      ;;
    m)
      MEMORY=$OPTARG
      ;;
    a)
      JVM_ARGS=$OPTARG
      ;;
    n)
      NUM_NODES=$OPTARG
      ;;
    w)
      VCS_WORK=$OPTARG
      ;;
    z)
      ZK_PORT=$OPTARG
      ;;
    d)
      SMOKE_RC_URL=$OPTARG
      ;;
   \?)
      echo "Invalid option: -$OPTARG" >&2
      exit 1
   esac
done
shift $((OPTIND -1))

CLUSTER_WD=$1

#################
# Validate Args #
#################
case ${COMMAND} in
   new);;
   stop);;
   start);;
   restart);;
   *) echo "Invalid command $COMMAND"; exit 2;
esac

case ${NUM_NODES} in
    ''|*[!0-9]*) echo "$NUM_NODES (-n) is not a positive integer"; exit 3 ;;
    *) ;;
esac

case ${ZK_PORT} in
    ''|*[!0-9]*) echo "$NUM_NODES (-z) is not a positive integer"; exit 3 ;;
    *) ;;
esac

if [[ "$COMMAND" = "new" ]]; then
  if [[ "$CLEAN" = true ]]; then
    echo "Command new and option -c (clean) do not make sense together since a newly created cluster has no data to clean."; exit 1;
  fi
fi

if [[ ! -d "$VCS_WORK" ]]; then
  echo "$VCS_WORK (vcs working directory) does not exist"; exit 4;
fi

if [[ ! "$COMMAND" = "new" ]]; then
  if [[ -z "$CLUSTER_WD" ]]; then
    # find the most recently touched directory in the local directory
    CLUSTER_WD=$(find . -maxdepth 1 -mindepth 1 -type d -print0 | xargs -0 ls -1 -td | sed -E 's/\.\/(.*)/\1/' | head -n1)
  fi
fi

if [[ ! -z "$CLUSTER_WD" ]]; then
  if [[ ! -d "$CLUSTER_WD" && ! "$COMMAND" = "new" ]]; then
    echo "$CLUSTER_WD (cluster working directory) does not exist or is not a directory"; exit 5;
  fi
fi

############################
# Print our initialization #
############################
echo "COMMAND    : $COMMAND"
echo "VCS WD     : $VCS_WORK"
echo "CLUSTER WD : $CLUSTER_WD"
echo "NUM NODES  : $NUM_NODES"
echo "ZK PORT    : $ZK_PORT"
echo "CLEAN      : $CLEAN"
echo "RECOMPILE  : $RECOMPILE"

###########################################################
# Create new cluster working dir if new command specified #
###########################################################
mkdirIfReq() {
  if [[ "$COMMAND" = "new" ]]; then
    if [[ -z "$CLUSTER_WD" ]]; then
      DATE=$(date "+%Y-%m-%d")
      CLUSTER_WD="${DATE}"
    fi
    mkdir "$CLUSTER_WD"
    if [[ "$?" -ne 0 ]]; then
      echo "Unable to create $CLUSTER_WD"; exit 6;
    fi
  fi
}

#################
# Find Solr etc #
#################

findSolr() {
  pushd ${CLUSTER_WD}
  CLUSTER_WD_FULL=$(pwd -P)
  SOLR=${CLUSTER_WD}/$(find . -maxdepth 1 -name 'solr*' -type d -print0 | xargs -0 ls -1 -td | sed -E 's/\.\/(solr.*)/\1/' | head -n1)
  popd

  #echo "Found solr at $SOLR"
  SAFE_DEST="${CLUSTER_WD_FULL//\//_}";
}

###############################################
# Clean node dir (and thus data) if requested #
###############################################
cleanIfReq() {
  if [[ "$CLEAN" = true ]]; then
    if [[ -d "$CLUSTER_WD" ]]; then
      echo "Cleaning out $CLUSTER_WD"
      pushd ${CLUSTER_WD}
      rm -rf n*      # remove node dirs which are are n1, n2, n3 etc
      popd
    fi
    findSolr
    echo COLLECTIONS FOUND IN ZK | egrep --color=always '.*'
    COLLECTIONS_TO_CLEAN=`${SOLR}/bin/solr zk ls /solr_${SAFE_DEST}/collections -z     localhost:${ZK_PORT}`; echo $COLLECTIONS_TO_CLEAN | egrep --color=always '.*'
    for collection in ${COLLECTIONS_TO_CLEAN}; do
      echo nuke $collection
      ${SOLR}/bin/solr zk rm -r /solr_${SAFE_DEST}/collections/${collection} -z     localhost:${ZK_PORT}
      echo $?
    done
  fi
}

#################################
# Recompile server if requested #
#################################
recompileIfReq() {
  if [[ "$RECOMPILE" = true ]]; then
    pushd "$VCS_WORK"/lucene
    ant clean package-all-binary
    if [[ "$?" -ne 0 ]]; then
      echo "BUILD FAIL - cloud.sh stopping, see above output for details"; popd; exit 7;
    fi
    popd
    pushd "$VCS_WORK"/solr
    ant clean server create-package
    if [[ "$?" -ne 0 ]]; then
      echo "BUILD FAIL - cloud.sh stopping, see above output for details"; popd; exit 7;
    fi
    popd
    copyTarball
  fi
}

################
# Copy tarball #
################
copyTarball() {
    pushd ${CLUSTER_WD}
    rm -rf solr-*  # remove tarball and dir to which it extracts
    pushd # back to original dir to properly resolve vcs working dir
    if [ ! -z "$SMOKE_RC_URL" ]; then
      pushd ${CLUSTER_WD}
      RC_FILE=$(echo "${SMOKE_RC_URL}" | rev | cut -d '/' -f 1 | rev)
      curl -o "$RC_FILE" "$SMOKE_RC_URL"
      pushd
    else
      if [[ ! -f $(ls "$VCS_WORK"/solr/package/solr-*.tgz) ]]; then
        echo "No solr tarball found try again with -r"; popd; exit 10;
      fi
      cp "$VCS_WORK"/solr/package/solr-*.tgz ${CLUSTER_WD}
    fi
    pushd # back into cluster wd to unpack
    tar xzvf solr-*.tgz
    popd
}

#############################################
# Test to see if port for zookeeper is open #
# Assume that zookeeper holds it if it is   #
#############################################
testZookeeper() {
  PORT_FOUND=$( netstat -an | grep '\b'${ZK_PORT}'\s' | grep LISTEN | awk '{print $4}' | sed -E 's/.*\b('${ZK_PORT}')\s*/\1/');
  if [[ -z  "$PORT_FOUND" ]]; then
    echo "No process listening on port ${ZK_PORT}. Please start zookeeper and try again"; exit 8;
  fi
}

##########################
# Start server instances #
##########################
start(){
  testZookeeper
  echo "Starting servers"
  findSolr

  echo "SOLR=$SOLR"
  SOLR_ROOT=$("${SOLR}/server/scripts/cloud-scripts/zkcli.sh" -zkhost localhost:${ZK_PORT} -cmd getfile "/solr_${SAFE_DEST}" /dev/stdout);
  if [[ -z ${SOLR_ROOT} ]]; then
    # Need a fresh root in zookeeper...
    "${SOLR}/server/scripts/cloud-scripts/zkcli.sh" -zkhost localhost:${ZK_PORT} -cmd makepath "/solr_${SAFE_DEST}";
    "${SOLR}/server/scripts/cloud-scripts/zkcli.sh" -zkhost localhost:${ZK_PORT} -cmd put "/solr_${SAFE_DEST}" "created by cloud.sh"; # so we can test for existence next time
    "${SOLR}/server/scripts/cloud-scripts/zkcli.sh" -zkhost localhost:${ZK_PORT} -cmd putfile "/solr_${SAFE_DEST}/solr.xml" "${SOLR}/server/solr/solr.xml";
  fi

  ACTUAL_NUM_NODES=$(ls -1 -d ${CLUSTER_WD}/n* | wc -l )
  if [[ "$NUM_NODES" -eq 0 ]]; then
    NUM_NODES=${ACTUAL_NUM_NODES}
  else
    if [[ "$NUM_NODES" -ne "$ACTUAL_NUM_NODES" ]]; then
      #check that this isn't first time startup..
      if [[ "$ACTUAL_NUM_NODES" -ne 0 ]]; then
        echo "Requested $NUM_NODES for a cluster that already has $ACTUAL_NUM_NODES. Refusing to start!"; exit 9;
      fi
    fi
  fi

  if [[ "$NUM_NODES" -eq 0 ]]; then
    NUM_NODES=4  # nothing pre-existing found, default to 4
  fi
  echo "Final NUM_NODES is $NUM_NODES"
  for i in `seq 1 $NUM_NODES`; do
    mkdir -p "${CLUSTER_WD}/n${i}"
    argsArray=(-c -s $CLUSTER_WD_FULL/n${i} -z localhost:${ZK_PORT}/solr_${SAFE_DEST} -p 898${i} -m $MEMORY \
    -a "-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=500${i} \
    -Dsolr.solrxml.location=zookeeper -Dsolr.log.dir=$CLUSTER_WD_FULL/n${i} $JVM_ARGS")
    FINAL_COMMAND="${SOLR}/bin/solr ${argsArray[@]}"
    echo ${FINAL_COMMAND}
    ${SOLR}/bin/solr "${argsArray[@]}"
  done

  touch ${CLUSTER_WD}  # make this the most recently updated dir for ls -t

}

stop() {
  echo "Stopping servers"
  pushd ${CLUSTER_WD}
  SOLR=${CLUSTER_WD}/$(find . -maxdepth 1 -name 'solr*' -type d -print0 | xargs -0 ls -1 -td | sed -E 's/\.\/(solr.*)/\1/' | head -n1)
  popd

  "${SOLR}/bin/solr" stop -all
}

########################
# process the commands #
########################
case ${COMMAND} in
  new)
    testZookeeper
    mkdirIfReq
    recompileIfReq
    if [[ "$RECOMPILE" = false ]]; then
      copyTarball
    fi
    start
  ;;
  stop)
    stop
  ;;
  start)
    testZookeeper
    cleanIfReq
    recompileIfReq
    start
  ;;
  restart)
    testZookeeper
    stop
    cleanIfReq
    recompileIfReq
    start
  ;;
  *) echo "Invalid command $COMMAND"; exit 2;
esac
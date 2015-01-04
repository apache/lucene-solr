#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

if [[ $EUID -ne 0 ]]; then
  echo -e "\nERROR: This script must be run as root\n" 1>&2
  exit 1
fi

print_usage() {
  ERROR_MSG="$1"

  if [ "$ERROR_MSG" != "" ]; then
    echo -e "\nERROR: $ERROR_MSG\n" 1>&2
  fi

  echo ""
  echo "Usage: install_solr_service.sh path_to_solr_distribution_archive OPTIONS"
  echo ""
  echo "  The first argument to the script must be a path to a Solr distribution archive, such as solr-5.0.0.tgz"
  echo "    (only .tgz or .zip are supported formats for the archive)"
  echo ""
  echo "  Supported OPTIONS include:"
  echo ""
  echo "    -d     Directory for live / writable Solr files, such as logs, pid files, and index data; defaults to /var/solr"
  echo ""
  echo "    -i     Directory to extract the Solr installation archive; defaults to /opt/"
  echo "             The specified path must exist prior to using this script."
  echo ""
  echo "    -p     Port Solr should bind to; default is 8983"
  echo ""
  echo "    -s     Service name; defaults to solr"
  echo ""
  echo "    -u     User to own the Solr files and run the Solr process as; defaults to solr"
  echo "             This script will create the specified user account if it does not exist."
  echo ""
  echo " NOTE: Must be run as the root user"
  echo ""
} # end print_usage

if [ -f "/proc/version" ]; then
  proc_version=`cat /proc/version`
else
  proc_version=`uname -a`
fi

if [[ $proc_version == *"Debian"* ]]; then
  distro=Debian
elif [[ $proc_version == *"Red Hat"* ]]; then
  distro=RedHat
elif [[ $proc_version == *"Ubuntu"* ]]; then
  distro=Ubuntu
elif [[ $proc_version == *"SUSE"* ]]; then
  distro=SUSE
else
  echo -e "\nERROR: Your Linux distribution ($proc_version) not supported by this script!\nYou'll need to setup Solr as a service manually using the documentation provided in the Solr Reference Guide.\n" 1>&2
  exit 1
fi

if [ -z "$1" ]; then
  print_usage "Must specify the path to the Solr installation archive, such as solr-5.0.0.tgz"
  exit 1
fi

SOLR_ARCHIVE=$1
if [ ! -f "$SOLR_ARCHIVE" ]; then
  print_usage "Specified Solr installation archive $SOLR_ARCHIVE not found!"
  exit 1
fi

# strip off path info
SOLR_INSTALL_FILE=${SOLR_ARCHIVE##*/}
is_tar=true
if [ ${SOLR_INSTALL_FILE: -4} == ".tgz" ]; then
  SOLR_DIR=${SOLR_INSTALL_FILE:0:-4}
elif [ ${SOLR_INSTALL_FILE: -4} == ".zip" ]; then
  SOLR_DIR=${SOLR_INSTALL_FILE:0:-4}
  is_tar=false
else
  print_usage "Solr installation archive $SOLR_ARCHIVE is invalid, expected a .tgz or .zip file!"
  exit 1
fi

if [ $# -gt 1 ]; then
  shift
  while true; do
    case $1 in
        -i)
            if [[ -z "$2" || "${2:0:1}" == "-" ]]; then
              print_usage "Directory path is required when using the $1 option!"
              exit 1
            fi
            SOLR_EXTRACT_DIR=$2
            shift 2
        ;;
        -d)
            if [[ -z "$2" || "${2:0:1}" == "-" ]]; then
              print_usage "Directory path is required when using the $1 option!"
              exit 1
            fi
            SOLR_VAR_DIR="$2"
            shift 2
        ;;
        -u)
            if [[ -z "$2" || "${2:0:1}" == "-" ]]; then
              print_usage "Username is required when using the $1 option!"
              exit 1
            fi
            SOLR_USER="$2"
            shift 2
        ;;
        -s)
            if [[ -z "$2" || "${2:0:1}" == "-" ]]; then
              print_usage "Service name is required when using the $1 option!"
              exit 1
            fi
            SOLR_SERVICE="$2"
            shift 2
        ;;
        -p)
            if [[ -z "$2" || "${2:0:1}" == "-" ]]; then
              print_usage "Port is required when using the $1 option!"
              exit 1
            fi
            SOLR_PORT="$2"
            shift 2
        ;;
        -help|-usage)
            print_usage ""
            exit 0
        ;;
        --)
            shift
            break
        ;;
        *)
            if [ "$1" != "" ]; then
              print_usage "Unrecognized or misplaced argument: $1!"
              exit 1
            else
              break # out-of-args, stop looping
            fi
        ;;
    esac
  done
fi

if [ -z "$SOLR_EXTRACT_DIR" ]; then
  SOLR_EXTRACT_DIR=/opt
fi

if [ ! -d "$SOLR_EXTRACT_DIR" ]; then
  print_usage "Installation directory $SOLR_EXTRACT_DIR not found! Please create it before running this script."
  exit 1
fi

if [ -z "$SOLR_VAR_DIR" ]; then
  SOLR_VAR_DIR=/var/solr
fi

if [ -z "$SOLR_USER" ]; then
  SOLR_USER=solr
fi

if [ -z "$SOLR_SERVICE" ]; then
  SOLR_SERVICE=solr
fi

if [ -z "$SOLR_PORT" ]; then
  SOLR_PORT=8983
fi

if [ -f "/etc/init.d/$SOLR_SERVICE" ]; then
  echo -e "\nERROR: /etc/init.d/$SOLR_SERVICE already exists! Perhaps solr is already setup as a service on this host?\n" 1>&2
  exit 1
fi

if [ -e "$SOLR_EXTRACT_DIR/$SOLR_SERVICE" ]; then
  print_usage "$SOLR_EXTRACT_DIR/$SOLR_SERVICE already exists! Please move this directory / link or choose a different service name using the -s option."
  exit 1
fi

solr_uid=`id -u $SOLR_USER`
if [ $? -ne 0 ]; then
  echo "Creating new user: $SOLR_USER"
  if [ "$distro" == "RedHat" ]; then
    adduser $SOLR_USER
  elif [ "$distro" == "SUSE" ]; then
    useradd -m $SOLR_USER
  else
    adduser --system --shell /bin/bash --group --disabled-password --home /home/$SOLR_USER $SOLR_USER
  fi
fi

SOLR_INSTALL_DIR=$SOLR_EXTRACT_DIR/$SOLR_DIR
if [ ! -d "$SOLR_INSTALL_DIR" ]; then

  echo "Extracting $SOLR_ARCHIVE to $SOLR_EXTRACT_DIR"

  if $is_tar ; then
    tar zxf $SOLR_ARCHIVE -C $SOLR_EXTRACT_DIR
  else
    unzip -q $SOLR_ARCHIVE -d $SOLR_EXTRACT_DIR
  fi

  if [ ! -d "$SOLR_INSTALL_DIR" ]; then
    echo -e "\nERROR: Expected directory $SOLR_INSTALL_DIR not found after extracting $SOLR_ARCHIVE ... script fails.\n" 1>&2
    exit 1
  fi

  chown -R $SOLR_USER: $SOLR_INSTALL_DIR
else
  echo -e "\nWARNING: $SOLR_INSTALL_DIR already exists! Skipping extract ...\n"
fi

# create a symlink for easier scripting
ln -s $SOLR_INSTALL_DIR $SOLR_EXTRACT_DIR/$SOLR_SERVICE
chown -h $SOLR_USER: $SOLR_EXTRACT_DIR/$SOLR_SERVICE

mkdir -p $SOLR_VAR_DIR/data
mkdir -p $SOLR_VAR_DIR/logs
cp $SOLR_INSTALL_DIR/server/solr/solr.xml $SOLR_VAR_DIR/data/
cp $SOLR_INSTALL_DIR/bin/solr.in.sh $SOLR_VAR_DIR/
cp $SOLR_INSTALL_DIR/server/resources/log4j.properties $SOLR_VAR_DIR/log4j.properties
sed_expr="s#solr.log=.*#solr.log=\${solr.solr.home}/../logs#"
sed -i -e "$sed_expr" $SOLR_VAR_DIR/log4j.properties
chown -R $SOLR_USER: $SOLR_VAR_DIR

echo "SOLR_PID_DIR=$SOLR_VAR_DIR
SOLR_HOME=$SOLR_VAR_DIR/data
LOG4J_PROPS=$SOLR_VAR_DIR/log4j.properties
SOLR_LOGS_DIR=$SOLR_VAR_DIR/logs
SOLR_PORT=$SOLR_PORT
" >> $SOLR_VAR_DIR/solr.in.sh

echo "Creating /etc/init.d/$SOLR_SERVICE script ..."
cp $SOLR_INSTALL_DIR/bin/init.d/solr /etc/init.d/$SOLR_SERVICE
chmod 744 /etc/init.d/$SOLR_SERVICE
chown root:root /etc/init.d/$SOLR_SERVICE

# do some basic variable substitution on the init.d script
sed_expr1="s#SOLR_INSTALL_DIR=.*#SOLR_INSTALL_DIR=$SOLR_EXTRACT_DIR/$SOLR_SERVICE#"
sed_expr2="s#SOLR_ENV=.*#SOLR_ENV=$SOLR_VAR_DIR/solr.in.sh#"
sed_expr3="s#RUNAS=.*#RUNAS=$SOLR_USER#"
sed_expr4="s#Provides:.*#Provides: $SOLR_SERVICE#"
sed -i -e "$sed_expr1" -e "$sed_expr2" -e "$sed_expr3" -e "$sed_expr4" /etc/init.d/$SOLR_SERVICE

if [[ "$distro" == "RedHat" || "$distro" == "SUSE" ]]; then
  chkconfig $SOLR_SERVICE on
else
  update-rc.d $SOLR_SERVICE defaults
fi

service $SOLR_SERVICE start
sleep 5
service $SOLR_SERVICE status

echo "Service $SOLR_SERVICE installed."

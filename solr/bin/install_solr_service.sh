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
  echo "Usage: install_solr_service.sh <path_to_solr_distribution_archive> [OPTIONS]"
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
  echo "    -f     Upgrade Solr. Overwrite symlink and init script of previous installation."
  echo ""
  echo "    -n     Do not start Solr service after install, and do not abort on missing Java"
  echo ""
  echo " NOTE: Must be run as the root user"
  echo ""
} # end print_usage

print_error() {
  echo $1
  exit 1
}

# Locate *NIX distribution by looking for match from various detection strategies
# We start with /etc/os-release, as this will also work for Docker containers
for command in "grep -E \"^NAME=\" /etc/os-release" \
               "lsb_release -i" \
               "cat /proc/version" \
               "uname -a" ; do
    distro_string=$(eval $command 2>/dev/null)
    unset distro
    if [[ ${distro_string,,} == *"debian"* ]]; then
      distro=Debian
    elif [[ ${distro_string,,} == *"red hat"* ]]; then
      distro=RedHat
    elif [[ ${distro_string,,} == *"centos"* ]]; then
      distro=CentOS
    elif [[ ${distro_string,,} == *"ubuntu"* ]]; then
      distro=Ubuntu
    elif [[ ${distro_string,,} == *"suse"* ]]; then
      distro=SUSE
    elif [[ ${distro_string,,} == *"darwin"* ]]; then
      echo "Sorry, this script does not support macOS. You'll need to setup Solr as a service manually using the documentation provided in the Solr Reference Guide."
      echo "You could also try installing via Homebrew (http://brew.sh/), e.g. brew install solr"
      exit 1
    fi
    if [[ $distro ]] ; then break ; fi
done
if [[ ! $distro ]] ; then
  echo -e "\nERROR: Unable to auto-detect your *NIX distribution!\nYou'll need to setup Solr as a service manually using the documentation provided in the Solr Reference Guide.\n" 1>&2
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
  SOLR_DIR=${SOLR_INSTALL_FILE%.tgz}
elif [ ${SOLR_INSTALL_FILE: -4} == ".zip" ]; then
  SOLR_DIR=${SOLR_INSTALL_FILE%.zip}
  is_tar=false
else
  print_usage "Solr installation archive $SOLR_ARCHIVE is invalid, expected a .tgz or .zip file!"
  exit 1
fi

SOLR_START=true
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
        -f)
            SOLR_UPGRADE="YES"
            shift 1
        ;;
        -n)
            SOLR_START=false
            shift 1
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

# Test for availability of needed tools
if [[ $is_tar ]] ; then
  tar --version &>/dev/null     || print_error "Script requires the 'tar' command"
else
  unzip -hh &>/dev/null         || print_error "Script requires the 'unzip' command"
fi
if [[ $SOLR_START == "true" ]] ; then
  service --version &>/dev/null || service --help &>/dev/null || print_error "Script requires the 'service' command"
  java -version &>/dev/null     || print_error "Solr requires java, please install or set JAVA_HOME properly"
fi
lsof -h &>/dev/null             || echo "We recommend installing the 'lsof' command for more stable start/stop of Solr"


if [ -z "$SOLR_EXTRACT_DIR" ]; then
  SOLR_EXTRACT_DIR=/opt
fi

if [ ! -d "$SOLR_EXTRACT_DIR" ]; then
  print_usage "Installation directory $SOLR_EXTRACT_DIR not found! Please create it before running this script."
  exit 1
fi

if [ -z "$SOLR_SERVICE" ]; then
  SOLR_SERVICE=solr
fi

if [ -z "$SOLR_VAR_DIR" ]; then
  SOLR_VAR_DIR="/var/$SOLR_SERVICE"
fi

if [ -z "$SOLR_USER" ]; then
  SOLR_USER=solr
fi

if [ -z "$SOLR_PORT" ]; then
  SOLR_PORT=8983
fi

if [ -z "$SOLR_UPGRADE" ]; then
  SOLR_UPGRADE=NO
fi

if [ ! "$SOLR_UPGRADE" = "YES" ]; then
  if [ -f "/etc/init.d/$SOLR_SERVICE" ]; then
    print_usage "/etc/init.d/$SOLR_SERVICE already exists! Perhaps Solr is already setup as a service on this host? To upgrade Solr use the -f option."
    exit 1
  fi

  if [ -e "$SOLR_EXTRACT_DIR/$SOLR_SERVICE" ]; then
    print_usage "$SOLR_EXTRACT_DIR/$SOLR_SERVICE already exists! Please move this directory / link or choose a different service name using the -s option."
    exit 1
  fi
fi

# stop running instance
if [ -f "/etc/init.d/$SOLR_SERVICE" ]; then
  echo -e "\nStopping Solr instance if exists ...\n"
  service "$SOLR_SERVICE" stop
fi

# create user if not exists
solr_uid="`id -u "$SOLR_USER"`"
if [ $? -ne 0 ]; then
  echo "Creating new user: $SOLR_USER"
  if [ "$distro" == "RedHat" ] || [ "$distro" == "CentOS" ] ; then
    adduser --system -U -m --home-dir "$SOLR_VAR_DIR" "$SOLR_USER"
  elif [ "$distro" == "SUSE" ]; then
    useradd --system -U -m --home-dir "$SOLR_VAR_DIR" "$SOLR_USER"
  else
    adduser --system --shell /bin/bash --group --disabled-password --home "$SOLR_VAR_DIR" "$SOLR_USER"
  fi
fi

# extract
SOLR_INSTALL_DIR="$SOLR_EXTRACT_DIR/$SOLR_DIR"
if [ ! -d "$SOLR_INSTALL_DIR" ]; then

  echo -e "\nExtracting $SOLR_ARCHIVE to $SOLR_EXTRACT_DIR\n"

  if $is_tar ; then
    tar zxf "$SOLR_ARCHIVE" -C "$SOLR_EXTRACT_DIR"
  else
    unzip -q "$SOLR_ARCHIVE" -d "$SOLR_EXTRACT_DIR"
  fi

  if [ ! -d "$SOLR_INSTALL_DIR" ]; then
    echo -e "\nERROR: Expected directory $SOLR_INSTALL_DIR not found after extracting $SOLR_ARCHIVE ... script fails.\n" 1>&2
    exit 1
  fi

  chown -R root: "$SOLR_INSTALL_DIR"
  find "$SOLR_INSTALL_DIR" -type d -print0 | xargs -0 chmod 0755
  find "$SOLR_INSTALL_DIR" -type f -print0 | xargs -0 chmod 0644
  chmod -R 0755 "$SOLR_INSTALL_DIR/bin"
else
  echo -e "\nWARNING: $SOLR_INSTALL_DIR already exists! Skipping extract ...\n"
fi

# create a symlink for easier scripting
if [ -h "$SOLR_EXTRACT_DIR/$SOLR_SERVICE" ]; then
  echo -e "\nRemoving old symlink $SOLR_EXTRACT_DIR/$SOLR_SERVICE ...\n"
  rm "$SOLR_EXTRACT_DIR/$SOLR_SERVICE"
fi
if [ -e "$SOLR_EXTRACT_DIR/$SOLR_SERVICE" ]; then
  echo -e "\nWARNING: $SOLR_EXTRACT_DIR/$SOLR_SERVICE is not symlink! Skipping symlink update ...\n"
else
  echo -e "\nInstalling symlink $SOLR_EXTRACT_DIR/$SOLR_SERVICE -> $SOLR_INSTALL_DIR ...\n"
  ln -s "$SOLR_INSTALL_DIR" "$SOLR_EXTRACT_DIR/$SOLR_SERVICE"
fi

# install init.d script
echo -e "\nInstalling /etc/init.d/$SOLR_SERVICE script ...\n"
cp "$SOLR_INSTALL_DIR/bin/init.d/solr" "/etc/init.d/$SOLR_SERVICE"
chmod 0744 "/etc/init.d/$SOLR_SERVICE"
chown root: "/etc/init.d/$SOLR_SERVICE"
# do some basic variable substitution on the init.d script
sed_expr1="s#SOLR_INSTALL_DIR=.*#SOLR_INSTALL_DIR=\"$SOLR_EXTRACT_DIR/$SOLR_SERVICE\"#"
sed_expr2="s#SOLR_ENV=.*#SOLR_ENV=\"/etc/default/$SOLR_SERVICE.in.sh\"#"
sed_expr3="s#RUNAS=.*#RUNAS=\"$SOLR_USER\"#"
sed_expr4="s#Provides:.*#Provides: $SOLR_SERVICE#"
sed -i -e "$sed_expr1" -e "$sed_expr2" -e "$sed_expr3" -e "$sed_expr4" "/etc/init.d/$SOLR_SERVICE"

# install/move configuration
if [ ! -d /etc/default ]; then
  mkdir /etc/default
  chown root: /etc/default
  chmod 0755 /etc/default
fi
if [ -f "$SOLR_VAR_DIR/solr.in.sh" ]; then
  echo -e "\nMoving existing $SOLR_VAR_DIR/solr.in.sh to /etc/default/$SOLR_SERVICE.in.sh ...\n"
  mv "$SOLR_VAR_DIR/solr.in.sh" "/etc/default/$SOLR_SERVICE.in.sh"
elif [ -f "/etc/default/$SOLR_SERVICE.in.sh" ]; then
  echo -e "\n/etc/default/$SOLR_SERVICE.in.sh already exist. Skipping install ...\n"
else
  echo -e "\nInstalling /etc/default/$SOLR_SERVICE.in.sh ...\n"
  cp "$SOLR_INSTALL_DIR/bin/solr.in.sh" "/etc/default/$SOLR_SERVICE.in.sh"
  mv "$SOLR_INSTALL_DIR/bin/solr.in.sh" "$SOLR_INSTALL_DIR/bin/solr.in.sh.orig"  
  mv "$SOLR_INSTALL_DIR/bin/solr.in.cmd" "$SOLR_INSTALL_DIR/bin/solr.in.cmd.orig"  
  echo "SOLR_PID_DIR=\"$SOLR_VAR_DIR\"
SOLR_HOME=\"$SOLR_VAR_DIR/data\"
LOG4J_PROPS=\"$SOLR_VAR_DIR/log4j2.xml\"
SOLR_LOGS_DIR=\"$SOLR_VAR_DIR/logs\"
SOLR_PORT=\"$SOLR_PORT\"
" >> "/etc/default/$SOLR_SERVICE.in.sh"
fi
chown root:${SOLR_USER} "/etc/default/$SOLR_SERVICE.in.sh"
chmod 0640 "/etc/default/$SOLR_SERVICE.in.sh"

# install data directories and files
mkdir -p "$SOLR_VAR_DIR/data"
mkdir -p "$SOLR_VAR_DIR/logs"
if [ -f "$SOLR_VAR_DIR/data/solr.xml" ]; then
  echo -e "\n$SOLR_VAR_DIR/data/solr.xml already exists. Skipping install ...\n"
else
  cp "$SOLR_INSTALL_DIR/server/solr/"{solr.xml,zoo.cfg} "$SOLR_VAR_DIR/data/"
fi
if [ -f "$SOLR_VAR_DIR/log4j2.xml" ]; then
  echo -e "\n$SOLR_VAR_DIR/log4j2.xml already exists. Skipping install ...\n"
else
  cp "$SOLR_INSTALL_DIR/server/resources/log4j2.xml" "$SOLR_VAR_DIR/log4j2.xml"
fi
chown -R "$SOLR_USER:" "$SOLR_VAR_DIR"
find "$SOLR_VAR_DIR" -type d -print0 | xargs -0 chmod 0750
find "$SOLR_VAR_DIR" -type f -print0 | xargs -0 chmod 0640

# configure autostart of service
if [[ "$distro" == "RedHat" || "$distro" == "CentOS" || "$distro" == "SUSE" ]]; then
  chkconfig "$SOLR_SERVICE" on
else
  update-rc.d "$SOLR_SERVICE" defaults
fi
echo "Service $SOLR_SERVICE installed."
echo "Customize Solr startup configuration in /etc/default/$SOLR_SERVICE.in.sh"

# start service
if [[ $SOLR_START == "true" ]] ; then
  service "$SOLR_SERVICE" start
  sleep 5
  service "$SOLR_SERVICE" status
else
  echo "Not starting Solr service (option -n given). Start manually with 'service $SOLR_SERVICE start'"
fi

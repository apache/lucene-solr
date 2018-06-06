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
#
###
### Shell script functions for Solr
###
#
##### Setup
DEFAULT_AGENT_HEAP=16M
DEFAULT_AGENT_MAIN_ETC_DIR=/etc/solr
DEFAULT_AGENT_PORT=58983
DEFAULT_CLOUD_BUILD_ADDRESS=239.89.83.0
DEFAULT_CLOUD_BUILD_PORT=8983
DEFAULT_SOLR_HEAP=512M
DEFAULT_SOLR_PORT=8983
DEFAULT_X_WHICH=/usr/bin/which
DEFAULT_ZK_CLIENT_PORT=2181
DEFAULT_ZK_ELECTION_PORT=3888
DEFAULT_ZK_LEADER_PORT=2888
##### End Setup
#
#----
# DO NOT EDIT BELOW THIS LINE WITHOUT GUIDANCE
#----

if [ -z "${AGENT_HEAP}" ]; then
  AGENT_HEAP="${DEFAULT_AGENT_HEAP}"
fi

if [ -z "${AGENT_MAIN_ETC_DIR}" ]; then
  AGENT_MAIN_ETC_DIR="${DEFAULT_AGENT_MAIN_ETC_DIR}"
fi

if [ -z "${AGENT_PORT}" ]; then
  AGENT_PORT="${DEFAULT_AGENT_PORT}"
fi

if [ -z "${CLOUD_BUILD_ADDRESS}" ]; then
  CLOUD_BUILD_ADDRESS="${DEFAULT_CLOUD_BUILD_ADDRESS}"
fi

if [ -z "${CLOUD_BUILD_PORT}" ]; then
  CLOUD_BUILD_PORT="${DEFAULT_CLOUD_BUILD_PORT}"
fi

if [ -z "${SOLR_HEAP}" ]; then
  SOLR_HEAP="${DEFAULT_SOLR_HEAP}"
fi

if [ -z "${SOLR_PORT}" ]; then
  SOLR_PORT="${DEFAULT_SOLR_PORT}"
fi

if [ -z "${X_WHICH}" ]; then
  X_WHICH="${DEFAULT_X_WHICH}"
fi

if [ -z "${ZK_CLIENT_PORT}" ]; then
  ZK_CLIENT_PORT="${DEFAULT_ZK_CLIENT_PORT}"
fi

if [ -z "${ZK_ELECTION_PORT}" ]; then
  ZK_ELECTION_PORT="${DEFAULT_ZK_ELECTION_PORT}"
fi

if [ -z "${ZK_LEADER_PORT}" ]; then
  ZK_LEADER_PORT="${DEFAULT_ZK_LEADER_PORT}"
fi

# Write a message to stderr indicating that a command is required.
# If no command is provided to this function, use a generic message.
# Exit the script after writing the message.
exit_err_msg_required_command() {
  REQ_CMD="$1"
  if [ -z "${REQ_CMD}" ]; then
    echo >&2 "A command required to run this script is missing."
  else
    echo >&2 "The ${REQ_CMD} command is required to run this script."
  fi
  err_msg_contact_community
  exit 1
}

# Write a message asking the user to contact the Solr community if they
# feel that there's a problem.
err_msg_contact_community() {
  echo >&2 "---"
  echo >&2 "If you feel this is in error, please mention the problem"
  echo >&2 "to the Solr community on the mailing list or IRC channel."
  echo >&2 "http://lucene.apache.org/solr/community.html#mailing-lists-irc"
}

# Locate directories based based on a script directory passed in as the first
# argument and set environment variables.  The current working directory will
# be set to the parent of that directory and assigned to SOLR_TIP.
find_dirs_and_change_cwd() {
  MYDIR="$1"
  cd ${MYDIR}
  cd ..
  SOLR_TIP=`pwd`
  LOCAL_ETC_DIR=${SOLR_TIP}/etc
  SOLR_LIB_DIR=${SOLR_TIP}/lib
  SOLR_TMP_DIR=${SOLR_TIP}/tmp
  SOLR_RUN_DIR=${SOLR_TIP}/run
}

# Find required system executables or shell builtin commands and set
# variables like X_TEMPFILE so the script has a better chance of running
# on systems where things are installed outside of LSB locations.
find_system_programs_or_exit() {
  # Make sure the which command is available.
  if [ ! -x "${X_WHICH}" ]; then
    echo >&2 "The command ${X_WHICH} is not found or not executable."
    exit_err_msg_required_command "${X_WHICH}"
  fi

  # Check for the existence of other commands we need.
  locate_command_or_exit tempfile X_TEMPFILE 1
  locate_command_or_exit find X_FIND 1
  locate_command_or_exit xargs X_XARGS 1
  # We look for lsof and netstat, but do not exit
  # if they are not found.
  locate_command_or_exit lsof X_LSOF 0
  locate_command_or_exit netstat X_NETSTAT 0
  # If NEITHER lsof or netstat is found, log a warning message.
  if [-z "${X_LSOF}" ] && [ -z "${X_NETSTAT}" ]; then
    echo >&2 "Neither lsof or netstat found.  Will not be able to check for"
    echo >&2 "programs already listening on relevant ports."
  fi
}

# Check whether a program exists as a builtin or an executable.  Will not
# validate a command that is a function or an alias.  Assign a dynamic
# variable to the bare program name or an executable file path as required
# if found, exit the script if not.  The first argument is the bare command,
# the second argument is the variable that will be assigned with the correct
# command or executable.  The third argument is an exit flag -- a numeric
# boolean value (usually 0 or 1) indicating whether or not a missing program
# will cause a script exit.  A missing program will result in an error message
# whether an exit is forced or not.  The first two arguments are required.
# The exit flag will default to 1 if not present.  If the variable mentioned
# as the second argument is already defined, then the value of that variable
# will override the first argument to this script and tested to make sure it
# is a valid command.  This allows somebody to define variables like X_TEMPFILE
# in advance to override the automatic detection, while making sure that the
# explicitly set value is actually a good command.
locate_command_or_exit() {
  PRGNAME=$1
  shift
  PRGVAR=$1
  shift
  EXITFLAG=$1

  if [ -z "${PRGNAME}" ] || [ -z "${PRGVAR}" ]; then
    echo >&2 "locate_command_or_exit routine missing required arguments."
    echo >&2 "Aborting script."
    err_msg_contact_community
    exit 1
  fi

  if [ -z "${EXITFLAG}" ]; then
    EXITFLAG=1;
  fi

  BADPROG=0
  if [ -n "${!PRGVAR}" ]; then
    echo >&2 "The ${PRGNAME} command has been predefined as ${!PRGVAR}."
    PRGNAME="${!PRGVAR}"
  fi

  CMD_TYPE=`type -t ${PRGNAME}`
  if [ "${CMD_TYPE}" == "builtin" ]; then
    eval ${PRGVAR}="\${PRGNAME}"
  elif [ "${CMD_TYPE}" == "file" ]; then
    XPATH=`"${X_WHICH}" "${PRGNAME}"`
    RET=$?
    if [ $RET -eq 0 ]; then
      if [ -x "${XPATH}" ]; then
        eval ${PRGVAR}="\${XPATH}"
      else
        BADPROG=1
      fi
    else
      BADPROG=1
    fi
  else
    BADPROG=1
  fi

  if [ ${BADPROG} -ne 0 ]; then
    echo >&2 "The ${PRGNAME} command is not found or not executable."
    echo >&2 "The ${PRGVAR} env variable can explicitly set its location."
    if [ ${EXITFLAG} -ne 0 ]; then
      exit_err_msg_required_command "${PRGNAME}"
    fi
  fi
  return 0
}

# Find Java and set the JAVA variable.
# Exit script if unable to locate a java executable.
find_java_or_exit() {
  JAVA=
  if [ -n "$SOLR_JAVA_HOME" ]; then
    echo >&2 "SOLR_JAVA_HOME found, using it as JAVA_HOME."
    JAVA_HOME="$SOLR_JAVA_HOME"
  fi

  if [ -n "$JAVA_HOME" ]; then
    for java in "${JAVA_HOME}/bin/amd64/java" "${JAVA_HOME}/bin/java"; do
      if [ -x "$java" ]; then
        JAVA="$java"
        break
      fi
    done
    if [ -z "$JAVA" ]; then
      echo >&2 "The currently defined JAVA_HOME (${JAVA_HOME}) refers"
      echo >&2 "to a location where Java could not be found.  Aborting."
      echo >&2 "Either fix the variable or remove it from the environment"
      echo >&2 "so that the system PATH will be searched."
      err_msg_contact_community
      exit 1
    fi
  else
    X_JAVA=`"${X_WHICH}" java`
    if [ -x "${X_JAVA}" ]; then
      JAVA=${X_JAVA}
    else
      echo >&2 "Unable to locate a java executable."
      err_msg_contact_community
      exit 1
    fi
  fi
  return 0
}

# Start the agent.  Will pass all parameters to the startup.
# The first parameter is the properties file to use.
agent_start() {
  start_agent_program $@ start
}

# Start the agent program with a commandline parameter that tells it
# to examine its config and the system, and use what it finds to create
# a bunch of environment variables.  The agent will output the commands
# for the variables to stdout.
agent_get_env() {
  start_agent_program get_env
  return $?
}

# Start the agent with arguments sent to this function.
# Removes all options (things like -D), leaving only commands for the agent.
# Will set a SOLR_AGENT_PROPERTIES sysprop with that variable value.
start_agent_program() {
  CP_JARS=`get_jars_for_classpath ${SOLR_TIP}/lib/log ${SOLR_TIP}/lib/solrj`

  OPTIND=1
  while getopts ":D:" opt; do
    case ${opt} in
      D)
        # Do nothing.  This will remove the option.  May not need this case.
        ;;
      *)
        # Do nothing.  This will remove the option.
        ;;
    esac
  done
  shift $((OPTIND -1))

  $JAVA -Xmx${AGENT_HEAP} -cp ${CP_JARS} -DSOLR_AGENT_PROPERTIES="${SOLR_AGENT_PROPERTIES}" -jar lib/agent.jar $@
  return $?
}

# Given a list of path locations, recursively find all the jars and write a
# classpath string to stdout.
get_jars_for_classpath() {
  get_files_for_classpath jar $@
}

# Given an extension and a list of of path locations, recursively find all
# the files with that extension and write a classpath string to stdout.  To
# include all files, pass an equal sign in as the extension.
get_files_for_classpath() {
  CP_EXTENSION=$1
  shift

  if [ "${CP_EXTENSION}" == "=" ]; then
    CP_FILESPEC="*"
  else
    CP_FILESPEC="*.${CP_EXTENSION}"
  fi

  DETECTED_CLASSPATH=
  ADDED_FIRST_CP_ENTRY=
  KEEP_RUNNING=true
  while [ -n "${KEEP_RUNNING}" ]; do
    CHECKDIR=$1
    shift
    if [ -n "${CHECKDIR}" ]; then
      if [ -d "${CHECKDIR}" ]; then
        while IFS= read -r -d '' line; do
          add_to_detected_classpath "$line"
        done < <(${X_FIND} ${CHECKDIR} -type f -name "${CP_FILESPEC}" -print0)
      fi
    else
      KEEP_RUNNING=
    fi
  done
  echo "$DETECTED_CLASSPATH"
}

add_to_detected_classpath() {
  CP_ENTRY="$1"
  if [ -n "${ADDED_FIRST_CP_ENTRY}" ]; then
    # If the first entry in the classpath has been added
    # we add a colon before the entry as a separator.
    CP_ENTRY=":${CP_ENTRY}"
  fi
  # Set the value to indicate the first entry has been added.
  ADDED_FIRST_CP_ENTRY=true
  DETECTED_CLASSPATH="${DETECTED_CLASSPATH}${CP_ENTRY}"
}

# Locate the agent properties file based on available information.
# The SOLR_AGENT_PROPERTIES variable may be empty after this runs,
# but only if this is a non-service install.
find_agent_properties_or_exit() {
  # First, if service name is not set, try to figure out a service name.
  # When no service name set and only one service is installed, we assume
  # that service.  If multiple services installed and no name is set, the
  # script will abort.
  if [ -z "${SOLR_SERVICE_NAME}" ]; then
    SOLR_SERVICES_CONFIG=${LOCAL_ETC_DIR}/services.conf
    declare -a SERVICES
    if [ -r "${SOLR_SERVICES_CONFIG}" ]; then
      readarray -t SERVICES < ${SOLR_SERVICES_CONFIG}
    fi
    if [ ${#SERVICES[@]} -gt 0 ]; then
      if [ ${#SERVICES[@]} -eq 1 ]; then
        SOLR_SERVICE_NAME=${SERVICES[0]}
      else
        echo >&2 "Multiple services detected in ${SOLR_SERVICES_CONFIG}"
        echo >&2 "SOLR_SERVICE_NAME must be defined to proceed."
        err_msg_contact_community
        exit 1
      fi
    fi
  fi

  # If we reach here, then we have either settled on one service name or
  # there are no services.
  if [ -n "${SOLR_SERVICE_NAME}" ]; then
    echo >&2 "Solr service name ${SOLR_SERVICE_NAME} detected."
    SOLR_AGENT_PROPERTIES=${MAIN_ETC_DIR}/${SOLR_SERVICE_NAME}/solr-agent.properties
  else
    SOLR_AGENT_PROPERTIES=${LOCAL_ETC_DIR}/solr-agent.properties
  fi

  # When running as a service, the agent properties file must exist. When the
  # properties file does not exist, decide whether to exit or use defaults.
  if [ ! -r "${SOLR_AGENT_PROPERTIES}" ]; then
    if [ -n "${SOLR_SERVICE_NAME}" ]; then
      echo >&2 "The ${SOLR_AGENT_PROPERTIES} file is not found or not readable."
      echo >&2 "Service installs must have an agent properties file."
      err_msg_contact_community
      exit 1
    else
      echo >&2 "The ${SOLR_AGENT_PROPERTIES} file is not found or not readable."
      echo >&2 "Proceeding with defaults for a non-service install."
      SOLR_AGENT_PROPERTIES=
    fi
    # TODO: Service recommendation might need to move.
    if [ -z "${SOLR_SERVICE_NAME}" ]; then
      recommend_service
    fi
  fi
}

recommend_service() {
  echo >&2 "Non-service install detected."
  echo >&2 "Installing Solr as a service is strongly recommended."
  echo >&2 "https://lucene.apache.org/solr/guide/taking-solr-to-production.html"
}

# Determine whether or not the configured version of Java supports the
# ExitOnOutOfMemoryError flag.  Sets JAVA_OOM_EXIT_SUPPORTED with a numeric
# boolean value and returns the exit code from the attempt to run Java with
# that option.
check_oom_exit_supported() {
  $JAVA -Xmx1M -XX:+ExitOnOutOfMemoryError -version > /dev/null 2> /dev/null
  RET=$?
  if [ ${RET} -eq 0 ]; then
    JAVA_OOM_EXIT_SUPPORTED=1
  else
    JAVA_OOM_EXIT_SUPPORTED=0
  fi
  return ${RET}
}

# If the BASH_VERSION variable is not set, it is a reasonable indication
# that the shell we are running in is not bash.  If that situation is
# detected, exit.
exit_if_no_bash() {
  if [ -z "${BASH_VERSION}" ]; then
    echo >&2 "This script must be run by the bash shell."
    err_msg_contact_community
    exit 1
  fi
}

# Exit script if the bash version is too old.  Require version 4.
# The first release of bash 4 was in 2009.
exit_if_bash_too_old() {
  if [ -n "${BASH_VERSINFO[*]}" ]; then
    if [ "${BASH_VERSINFO[0]}" -lt 4 ]; then
      echo >&2 "Detected bash version is ${BASH_VERSION}."
      echo >&2 "Script needs at least version 4."
      err_msg_contact_community
      exit 1
    fi
  else
    echo >&2 "There was a problem detecting bash version."
    echo >&2 "Aborting because compatibility cannot be detected."
    err_msg_contact_community
    exit 1
  fi
}

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

# We count on this working - it hooks in our proper error handling. Test well on changing.

export ERR_LN="-- ERROR! --> "

PS4='+ ${HOSTNAME}:$( basename "${BASH_SOURCE[0]}" ):${LINENO} '

if [ -z "${EXP_EXIT}" ]; then
  export EXP_EXIT="n"
fi

function errexit() {
  local err=$?
  local code="${1:-1}"
  local last_cmd="${BASH_COMMAND}"
  set +x

  if [ ! "${EXP_EXIT}" = "q" ]; then
    echo -w "\n\n\n${ERR_LN} Error in ${BASH_SOURCE[1]}:${BASH_LINENO[0]}. '${last_cmd}' exited with code ${err}"
  fi
  # print stack trace
  if [ ${#FUNCNAME[@]} -gt 2 ]; then
    echo "Stack Trace:"
    for ((i=1;i<${#FUNCNAME[@]}-1;i++))
    do
      echo " $i: ${BASH_SOURCE[$i+1]}:${BASH_LINENO[$i]} ${FUNCNAME[$i]}(...)"
    done
  fi

  if [ "${EXP_EXIT}" = "y" ]; then
    echo "${ERR_LN} Saw an exit with code ${code}"
  elif [ "${EXP_EXIT}" = "q" ]; then
    exit 0;
  else
    echo -e"${ERR_LN} Exiting with code ${code}\n\n\n"
    exit "${code}"
  fi
  
}

# trap ERR to provide an error handler whenever a command exits nonzero
#  this is a more verbose version of set -o errexit
trap 'errexit' ERR
# setting errtrace allows our ERR trap handler to be propagated to functions,
#  expansions and subshells
set -o errtrace -o pipefail


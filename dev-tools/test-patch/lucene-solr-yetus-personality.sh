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

# This is a Yetus precommit "personality" (aka customized configuration) for Lucene/Solr.
#
# See the Yetus precommit documentation at https://yetus.apache.org/documentation/0.7.0/
# and especially https://yetus.apache.org/documentation/0.7.0/precommit-advanced/.
# See also the Yetus source code for other projects' personality examples at
# https://git-wip-us.apache.org/repos/asf?p=yetus.git;f=precommit/personality;a=tree;hb=HEAD
#
# To add a new validation method (aka "plugin"):
#   1) Add its name to the PLUGIN_LIST below
#   2) Invoke "add_test_type" with it below
#   3) Add a "<plugin>_filefilter" function to decide whether the plugin needs to be run based on changed files
#   4) Add a "<plugin>_rebuild" function to call out to ant to perform the validation method.
# See examples of the above-described function types ^^ below.
   
# Both compile+javac plugins are required, as well as unit+junit: in both cases, neither work individually 
PLUGIN_LIST="ant,jira,compile,javac,unit,junit,test4tests"
PLUGIN_LIST+=",testoutput,checkluceneversion,ratsources,checkforbiddenapis,checklicenses"
PLUGIN_LIST+=",validatesourcepatterns,validaterefguide"
personality_plugins "${PLUGIN_LIST}"

add_test_type "checkluceneversion"
add_test_type "ratsources"
add_test_type "checkforbiddenapis"
add_test_type "checklicenses"
add_test_type "validatesourcepatterns"
add_test_type "validaterefguide"

add_test_format "testoutput"

## @description  Globals specific to this personality
## @audience     private
## @stability    evolving
function personality_globals
{
  #shellcheck disable=SC2034
  PATCH_BRANCH_DEFAULT=master
  #shellcheck disable=SC2034
  JIRA_ISSUE_RE='^(LUCENE|SOLR)-[0-9]+$'
  #shellcheck disable=SC2034
  JIRA_STATUS_RE='Patch Available'
  #shellcheck disable=SC2034
  GITHUB_REPO="apache/lucene-solr"
  #shellcheck disable=SC2034
  BUILDTOOL=ant
}

## @description  Queue up modules for this personality
## @audience     private
## @stability    evolving
## @param        repostatus
## @param        testtype
function personality_modules
{
  local repostatus=$1
  local testtype=$2

  local module
  local extra

  local moduleType="submodules"

  yetus_debug "Personality (lucene-solr): ${repostatus} ${testtype}"

  clear_personality_queue

  case ${testtype} in
    clean|distclean|validatesourcepatterns)
      moduleType="top"
      ;;
    checkluceneversion)
      moduleType="solr"
      ;;
    ratsources)
      moduleType="submodules"
      ;;
    checkforbiddenapis)
      moduleType="both"
      ;;
    checklicenses)
      moduleType="mains"
      ;;
    validaterefguide)
      moduleType="solr-ref-guide"
      ;;
    compile)
      moduleType="submodules"
      extra="compile-test"
      ;;
    junit|unit)
      moduleType="submodules"
      extra="-Dtests.badapples=false test"
      ;;  
    *)
      ;;
  esac

  case ${moduleType} in
    submodules)
      for module in "${CHANGED_MODULES[@]}"; do
        if [[ ! "${module}" =~ ^lucene/(licenses|site) ]]; then # blacklist lucene/ dirs that aren't modules
          if [[ "${module}" =~ ^(lucene/(analysis/[^/]+|[^/]+)) ]]; then
            local lucene_module=${BASH_REMATCH[0]}
            personality_enqueue_module "${lucene_module}" "${extra}"
          elif [[ "${module}" =~ ^solr/(core|solrj|test-framework|solr-ref-guide|contrib/[^.]+) ]]; then # whitelist solr/ modules
            local solr_module=${BASH_REMATCH[0]}
            # In solr-ref-guide module, do not execute "compile" or "unit" plugins
            if [[ ! "${solr_module}" == solr/solr-ref-guide || ! ${testtype} =~ ^(compile|unit)$ ]]; then
              personality_enqueue_module "${solr_module}" "${extra}"
            fi
          fi
        fi
      done
      ;;
    lucene|solr)
      personality_enqueue_module "${moduleType}" "${extra}"
      ;;
    top)
      personality_enqueue_module . "${extra}"
      ;;
    mains)
      personality_enqueue_module "lucene" "${extra}"
      personality_enqueue_module "solr" "${extra}"
      ;;
    both) # solr, lucene, or both
      # personality_enqueue_module KEEPS duplicates, so de-dupe first
      local doSolr=0,doLucene=0 
      for module in "${CHANGED_MODULES[@]}"; do
        if [[ "${module}" =~ ^solr/ ]]; then doSolr=1; fi
        if [[ "${module}" =~ ^lucene/ ]]; then doLucene=1; fi
      done
      if [[ ${doLucene} == 1 ]]; then
        if [[ ${doSolr} == 1 ]]; then 
          personality_enqueue_module . "${extra}"
        else
          personality_enqueue_module "lucene" "${extra}"
        fi          
      elif [[ ${doSolr} == 1 ]]; then 
        personality_enqueue_module "solr" "${extra}"
      fi
      ;;
    solr-ref-guide)
      for module in "${CHANGED_MODULES[@]}"; do
        if [[ "${module}" =~ ^solr/solr-ref-guide ]]; then
          personality_enqueue_module "solr/solr-ref-guide" "${extra}"
        fi
      done 
      ;;     
    *)
      ;;
  esac
}

## @description  Add tests based upon personality needs
## @audience     private
## @stability    evolving
## @param        filename
function personality_file_tests
{
  declare filename=$1

  yetus_debug "Using Lucene/Solr-specific personality_file_tests"

  if [[ ! ${filename} =~ solr-ref-guide ]]; then
    if [[ ${filename} =~ build\.xml$ || ${filename} =~ /src/(java|resources|test|test-files|tools) ]]; then
      yetus_debug "tests/unit: ${filename}"
      add_test compile
      add_test javac
      add_test unit
    fi
  fi
}

## @description  hook to reroute junit folder to search test results based on the module
## @audience     private
## @stability    evolving
## @param        module
## @param        buildlogfile
function testoutput_process_tests
{
  # shellcheck disable=SC2034
  declare module=$1
  declare buildlogfile=$2
  if [[ "${module}" =~ ^lucene/analysis/ ]]; then
    JUNIT_TEST_OUTPUT_DIR="../../build/${module#*/}"
  elif [[ "${module}" =~ ^lucene/ ]]; then
    JUNIT_TEST_OUTPUT_DIR="../build/${module#*/}"
  elif [[ "${module}" =~ ^solr/contrib/extraction ]]; then
    JUNIT_TEST_OUTPUT_DIR="../../build/contrib/solr-extraction"
  elif [[ "${module}" =~ ^solr/contrib/(.*) ]]; then
    JUNIT_TEST_OUTPUT_DIR="../../build/contrib/solr-${BASH_REMATCH[1]}"
  elif [[ "${module}" =~ ^solr/(.*) ]]; then
    JUNIT_TEST_OUTPUT_DIR="../build/solr-${BASH_REMATCH[1]}"
  fi
  yetus_debug "Rerouting build dir for junit to ${JUNIT_TEST_OUTPUT_DIR}"
}

## @description  checkluceneversion file filter
## @audience     private
## @stability    evolving
## @param        filename
function checkluceneversion_filefilter
{
  local filename=$1
  if [[ ${filename} =~ ^solr/(example|server/solr/configsets) ]]; then
    yetus_debug "tests/checkluceneversion: ${filename}"
    add_test checkluceneversion
  fi
}

## @description  checkluceneversion test
## @audience     private
## @stability    evolving
## @param        repostatus
function checkluceneversion_rebuild
{
  local repostatus=$1
  lucene_ant_command ${repostatus} "checkluceneversion" "check-example-lucene-match-version" "Check configsets' lucene version"
}

## @description  ratsources file filter
## @audience     private
## @stability    evolving
## @param        filename
function ratsources_filefilter
{
  local filename=$1
  if [[ ${filename} =~ /src/|\.xml$ ]] ; then
    yetus_debug "tests/ratsources: ${filename}"
    add_test ratsources
  fi
}

## @description  ratsources test
## @audience     private
## @stability    evolving
## @param        repostatus
function ratsources_rebuild
{
  local repostatus=$1
  lucene_ant_command ${repostatus} "ratsources" "rat-sources" "Release audit (RAT)"
}

## @description  checkforbiddenapis file filter
## @audience     private
## @stability    evolving
## @param        filename
function checkforbiddenapis_filefilter
{
  local filename=$1
  if [[ ${filename} =~ \.java$ ]] ; then
    yetus_debug "tests/checkforbiddenapis: ${filename}"
    add_test checkforbiddenapis
  fi
}

## @description  checkforbiddenapis test
## @audience     private
## @stability    evolving
## @param        repostatus
function checkforbiddenapis_rebuild
{
  local repostatus=$1
  lucene_ant_command ${repostatus} "checkforbiddenapis" "check-forbidden-apis" "Check forbidden APIs"
}

## @description  checklicenses file filter
## @audience     private
## @stability    evolving
## @param        filename
function checklicenses_filefilter
{
  local filename=$1
  if [[ ${filename} =~ (lucene|solr)/licenses/|lucene/ivy-versions.properties$ ]]; then
    yetus_debug "tests/checklicenses: ${filename}"
    add_test checklicenses
  fi    
}

## @description  checklicenses test
## @audience     private
## @stability    evolving
## @param        repostatus
function checklicenses_rebuild
{
  local repostatus=$1
  lucene_ant_command ${repostatus} "checklicenses" "check-licenses" "Check licenses"
}

## @description  validaterefguide file filter
## @audience     private
## @stability    evolving
## @param        filename
function validaterefguide_filefilter
{
  local filename=$1
  if [[ ${filename} =~ solr/solr-ref-guide ]]; then
    yetus_debug "tests/validaterefguide: ${filename}"
    add_test validaterefguide
  fi
}

## @description  validaterefguide test
## @audience     private
## @stability    evolving
## @param        repostatus
function validaterefguide_rebuild
{
  local repostatus=$1
  lucene_ant_command ${repostatus} "validaterefguide" "bare-bones-html-validation" "Validate ref guide"
}

## @description  validatesourcepatterns file filter
## @audience     private
## @stability    evolving
## @param        filename
function validatesourcepatterns_filefilter
{
  local filename=$1
  if [[ ${filename} =~ \.(java|jflex|py|pl|g4|jj|html|js|css|xml|xsl|vm|sh|cmd|bat|policy|properties|mdtext|groovy|template|adoc|json)$ ]]; then
    yetus_debug "tests/validatesourcepatterns: ${filename}"
    add_test validatesourcepatterns
  fi
}

## @description  validatesourcepatterns test
## @audience     private
## @stability    evolving
## @param        repostatus
function validatesourcepatterns_rebuild
{
  local repostatus=$1
  lucene_ant_command ${repostatus} "validatesourcepatterns" "validate-source-patterns" "Validate source patterns"
}

function lucene_ant_command
{
  declare repostatus=$1
  declare testname=$2
  declare antcommand=$3
  declare title=$4

  declare result=0
  declare i=0
  declare module
  declare fn
  declare result

  if [[ "${repostatus}" = branch ]]; then
    return 0
  fi

  if ! verify_needed_test ${testname}; then
    echo "${BUILDMODEMSG} does not need ${testname} testing."
    return 0
  fi

  big_console_header "${title}"
  personality_modules ${repostatus} ${testname}
  until [[ ${i} -eq ${#MODULE[@]} ]]; do
    if [[ ${MODULE_STATUS[${i}]} == -1 ]]; then
      ((result=result+1))
      ((i=i+1))
      continue
    fi
    ANT_ARGS=${antcommand}

    start_clock
    module=${MODULE[${i}]}
    fn=$(module_file_fragment "${module}")
    logfilename="${repostatus}-${antcommand}-${fn}.txt";
    logfile="${PATCH_DIR}/${logfilename}"
    buildtool_cwd "${i}"
    echo_and_redirect "${logfile}" $(ant_executor)

    if [[ $? == 0 ]] ; then
      module_status ${i} +1 "${logfilename}" "${title}" "${antcommand} passed"
    else
      module_status ${i} -1 "${logfilename}" "${title}" "${antcommand} failed"
      ((result=result+1))
    fi
    ((i=i+1))
    savestop=$(stop_clock)
    MODULE_STATUS_TIMER[${i}]=${savestop}
  done
  ANT_ARGS=""
  if [[ ${result} -gt 0 ]]; then
    modules_messages ${repostatus} "${title}" false
    return 1
  fi
  modules_messages ${repostatus} "${title}" true
  return 0
}

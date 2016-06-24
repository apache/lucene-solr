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

import sys
import os
sys.path.append(os.path.dirname(__file__))
from scriptutil import *
import argparse
import re

# Pulls out all JIRAs mentioned in the given CHANGES.txt filename under the given version
# and outputs a regular expression that will match all of them
def print_changes_jira_regex(filename, version):
  release_section_re = re.compile(r'\s*====*\s+(.*)\s+===')
  version_re = re.compile(r'%s(?:$|[^-])' % version)
  bullet_re = re.compile(r'\s*[-*]\s*(.*)')
  issue_list_re = re.compile(r'[:,/()\s]*((?:LUCENE|SOLR)-\d+)')
  more_issues_on_next_line_re = re.compile(r'(?:[:,/()\s]*(?:LUCENE|SOLR)-\d+)+\s*,\s*$') # JIRA list with trailing comma
  under_requested_version = False
  requested_version_found = False
  more_issues_on_next_line = False
  lucene_issues = []
  solr_issues = []
  with open(filename, 'r') as changes:
    for line in changes:
      version_boundary = release_section_re.match(line)
      if version_boundary is not None:
        if under_requested_version:
          break # No longer under the requested version - stop looking for JIRAs
        else:
          if version_re.search(version_boundary.group(1)):
            under_requested_version = True # Start looking for JIRAs
            requested_version_found = True
      else:
        if under_requested_version:
          bullet_match = bullet_re.match(line)
          if more_issues_on_next_line or bullet_match is not None:
            content = bullet_match.group(1) if bullet_match is not None else line
            for issue in issue_list_re.findall(content):
              (lucene_issues if issue.startswith('LUCENE-') else solr_issues).append(issue.rsplit('-', 1)[-1])
            more_issues_on_next_line = more_issues_on_next_line_re.match(content)
  if not requested_version_found:
    raise Exception('Could not find %s in %s' % (version, filename))
  print('\nRegex to match JIRAs in the %s release section in %s:' % (version, filename))
  if len(lucene_issues) > 0:
    print(r'LUCENE-(?:%s)\b' % '|'.join(lucene_issues), end='')
    if len(solr_issues) > 0:
      print('|', end='')
  if len(solr_issues) > 0:
    print(r'SOLR-(?:%s)\b' % '|'.join(solr_issues), end='')
  print()

def read_config():
  parser = argparse.ArgumentParser(description='Tools to help manage a Lucene/Solr release')
  parser.add_argument('version', type=Version.parse, help='Version of the form X.Y.Z')
  c = parser.parse_args()

  c.branch_type = find_branch_type()
  c.matching_branch = c.version.is_bugfix_release() and c.branch_type == BranchType.release or \
                      c.version.is_minor_release() and c.branch_type == BranchType.stable or \
                      c.version.is_major_release() and c.branch_type == BranchType.unstable

  print ("branch_type is %s " % c.branch_type)

  return c

def main():
  c = read_config()
  # TODO: add other commands to perform, specifiable via cmdline param
  # Right now, only one operation is performed: generate regex matching JIRAs for the given version from CHANGES.txt
  print_changes_jira_regex('lucene/CHANGES.txt', c.version)
  print_changes_jira_regex('solr/CHANGES.txt', c.version)

if __name__ == '__main__':
  try:
    main()
  except KeyboardInterrupt:
    print('\nReceived Ctrl-C, exiting early')

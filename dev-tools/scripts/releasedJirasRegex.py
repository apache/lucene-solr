#!/usr/bin/env python3
# -*- coding: utf-8 -*-
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

# Pulls out all JIRAs mentioned at the beginning of bullet items
# under the given version in the given CHANGES.txt file
# and prints a regular expression that will match all of them
#
# Caveat: In ancient versions (Lucene v1.9 and older; Solr v1.1 and older),
# does not find Bugzilla bugs or JIRAs not mentioned at the beginning of
# bullets or numbered entries.
#
def print_released_jiras_regex(version, filename):
  release_boundary_re = re.compile(r'\s*====*\s+(.*)\s+===')
  version_re = re.compile(r'%s(?:$|[^-])' % version)
  bullet_re = re.compile(r'\s*(?:[-*]|\d+\.(?=(?:\s|(?:LUCENE|SOLR)-)))(.*)')
  jira_ptn = r'(?:LUCENE|SOLR)-\d+'
  jira_re = re.compile(jira_ptn)
  jira_list_ptn = r'(?:[:,/()\s]*(?:%s))+' % jira_ptn
  jira_list_re = re.compile(jira_list_ptn)
  more_jiras_on_next_line_re = re.compile(r'%s\s*,\s*$' % jira_list_ptn) # JIRA list with trailing comma
  under_requested_version = False
  requested_version_found = False
  more_jiras_on_next_line = False
  lucene_jiras = []
  solr_jiras = []
  with open(filename, 'r') as changes:
    for line in changes:
      version_boundary = release_boundary_re.match(line)
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
          if more_jiras_on_next_line or bullet_match is not None:
            content = line if bullet_match is None else bullet_match.group(1)
            jira_list_match = jira_list_re.match(content)
            if jira_list_match is not None:
              jira_match = jira_re.findall(jira_list_match.group(0))
              for jira in jira_match:
                (lucene_jiras if jira.startswith('LUCENE-') else solr_jiras).append(jira.rsplit('-', 1)[-1])
            more_jiras_on_next_line = more_jiras_on_next_line_re.match(content)
  if not requested_version_found:
    raise Exception('Could not find %s in %s' % (version, filename))
  print()
  if (len(lucene_jiras) == 0 and len(solr_jiras) == 0):
    print('(No JIRAs => no regex)', end='')
  else:
    if len(lucene_jiras) > 0:
      print(r'LUCENE-(?:%s)\b%s' % ('|'.join(lucene_jiras), '|' if len(solr_jiras) > 0 else ''), end='')
    if len(solr_jiras) > 0:
      print(r'SOLR-(?:%s)\b' % '|'.join(solr_jiras), end='')
  print()

def read_config():
  parser = argparse.ArgumentParser(
    description='Prints a regex matching JIRAs fixed in the given version by parsing the given CHANGES.txt file')
  parser.add_argument('version', type=Version.parse, help='Version of the form X.Y.Z')
  parser.add_argument('changes', help='CHANGES.txt file to parse')
  return parser.parse_args()

def main():
  config = read_config()
  print_released_jiras_regex(config.version, config.changes)

if __name__ == '__main__':
  try:
    main()
  except KeyboardInterrupt:
    print('\nReceived Ctrl-C, exiting early')

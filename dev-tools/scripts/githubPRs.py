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

"""
Simple script that queries Github for all open PRs, then finds the ones without
issue number in title, and the ones where the linked JIRA is already closed
"""

import os
import sys
sys.path.append(os.path.dirname(__file__))
import argparse
import json
import re
from github import Github
from jira import JIRA

def read_config():
  parser = argparse.ArgumentParser(description='Find open Pull Requests that need attention')
  parser.add_argument('--json', action='store_true', default=False, help='Output as json')
  parser.add_argument('--token', help='Github access token in case you query too often anonymously')
  newconf = parser.parse_args()
  return newconf


def out(text):
  global conf
  if not conf.json:
    print(text)


def main():
  global conf
  conf = read_config()
  token = conf.token if conf.token is not None else None
  if token:
    gh = Github(token)
  else:
    gh = Github()
  jira = JIRA('https://issues.apache.org/jira')
  result = {}
  repo = gh.get_repo('apache/lucene-solr')
  open_prs = repo.get_pulls(state='open')
  out("Lucene/Solr Github PR report")
  out("============================")
  out("Number of open Pull Requests: %s" % open_prs.totalCount)
  result['open_count'] = open_prs.totalCount

  lack_jira = list(filter(lambda x: not re.match(r'.*\b(LUCENE|SOLR)-\d{3,6}\b', x.title), open_prs))
  lack_jira_dict = {}
  for pr in lack_jira:
    lack_jira_dict[pr.number] = pr.title
  result['no_jira_count'] = len(lack_jira_dict)
  result['no_jira'] = lack_jira_dict
  out("\nPRs lacking JIRA reference in title")
  for pr in lack_jira:
    out("  #%s: %s" % (pr.number, pr.title))

  out("\nOpen PRs with a resolved JIRA")
  has_jira = list(filter(lambda x: re.match(r'.*\b(LUCENE|SOLR)-\d{3,6}\b', x.title), open_prs))

  issue_ids = []
  issue_to_pr = {}
  for pr in has_jira:
    jira_issue_str = re.match(r'.*\b((LUCENE|SOLR)-\d{3,6})\b', pr.title).group(1)
    issue_ids.append(jira_issue_str)
    issue_to_pr[jira_issue_str] = pr

  resolved_jiras = jira.search_issues(jql_str="key in (%s) AND status in ('Closed', 'Resolved')" % ", ".join(issue_ids))
  closed_jira_dict = {}
  for issue in resolved_jiras:
    pr_title = issue_to_pr[issue.key].title
    pr_number = issue_to_pr[issue.key].number
    closed_jira_dict[pr_number] = { 'key': issue.key,
                                                 'status': issue.fields.status.name,
                                                 'resolution': issue.fields.resolution.name,
                                                 'resolution_date': issue.fields.resolutiondate,
                                                 'pr_title': pr_title}
    out("  #%s: %s status=%s, resolution=%s, resolutiondate=%s (%s)" % (pr_number,
                                                                        issue.key,
                                                                        issue.fields.status.name,
                                                                        issue.fields.resolution.name,
                                                                        issue.fields.resolutiondate,
                                                                        pr_title)
        )
  result['closed_jira_count'] = len(resolved_jiras)
  result['closed_jira'] = closed_jira_dict

  if conf.json:
    print(json.dumps(result, indent=4))


if __name__ == '__main__':
  try:
    main()
  except KeyboardInterrupt:
    print('\nReceived Ctrl-C, exiting early')

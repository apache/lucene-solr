#!/usr/bin/python
#
# vim: softtabstop=2 shiftwidth=2 expandtab
#
# Python port of poll-mirrors.pl
#
# This script is designed to poll download sites after posting a release
# and print out notice as each becomes available.  The RM can use this
# script to delay the release announcement until the release can be
# downloaded.
#
#
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
import argparse
import datetime
import ftplib
import re
import sys
import time

try:
  from urllib.parse import urlparse
except:
  from urlparse import urlparse

try:
  import http.client as http
except ImportError:
  import httplib as http

def p(s):
  sys.stdout.write(s)
  sys.stdout.flush()

def mirror_contains_file(url):
  url = urlparse(url)
  
  if url.scheme == 'http':
    return http_file_exists(url)
  elif url.scheme == 'ftp':
    return ftp_file_exists(url)

def http_file_exists(url):
  exists = False

  try:
    conn = http.HTTPConnection(url.netloc)
    conn.request('HEAD', url.path)
    response = conn.getresponse()

    exists = response.status == 200
  except:
    pass

  return exists

def ftp_file_exists(url):
  listing = []
  try:
    conn = ftplib.FTP(url.netloc)
    conn.login()
    listing = conn.nlst(url.path)
    conn.quit()
  except Exception as e:
    pass

  return len(listing) > 0

def check_url_list(lst):
  ret = []
  for url in lst:
    if mirror_contains_file(url):
      p('.')
    else:
      p('X')
      ret.append(url)

  return ret

parser = argparse.ArgumentParser(description='Checks that all Lucene mirrors contain a copy of a release')
parser.add_argument('-version', '-v', help='Lucene version to check', required=True)
parser.add_argument('-interval', '-i', help='seconds to wait to query again pending mirrors', type=int, default=300)
args = parser.parse_args()

try:
  conn = http.HTTPConnection('www.apache.org')
  conn.request('GET', '/mirrors/')
  response = conn.getresponse()
  html = response.read()
except Exception as e:
  p('Unable to fetch the Apache mirrors list!\n')
  sys.exit(1)

apache_path = 'lucene/java/{}/changes/Changes.html'.format(args.version);
maven_url = 'http://repo1.maven.org/maven2/' \
            'org/apache/lucene/lucene-core/{0}/lucene-core-{0}.pom.asc'.format(args.version)
maven_available = False

pending_mirrors = []
for match in re.finditer('<TR>(.*?)</TR>', str(html), re.MULTILINE | re.IGNORECASE | re.DOTALL):
  row = match.group(1)
  if not '<TD>ok</TD>' in row:
    # skip bad mirrors
    continue

  match = re.search('<A\s+HREF\s*=\s*"([^"]+)"\s*>', row, re.MULTILINE | re.IGNORECASE)
  if match:
    pending_mirrors.append(match.group(1) + apache_path)

total_mirrors = len(pending_mirrors)

while True:
  p('\n' + str(datetime.datetime.now()))
  p('\nPolling {} Apache Mirrors'.format(len(pending_mirrors)))
  if not maven_available:
    p(' and Maven Central')
  p('...\n')

  if not maven_available:
    maven_available = mirror_contains_file(maven_url)

  start = time.time()
  pending_mirrors = check_url_list(pending_mirrors)
  stop = time.time()
  remaining = args.interval - (stop - start)

  available_mirrors = total_mirrors - len(pending_mirrors)

  p('\n\n{} is{}downloadable from Maven Central\n'.format(args.version, maven_available and ' ' or ' not '))
  p('{} is downloadable from {}/{} Apache Mirrors ({:.2f}%)\n'.format(args.version, available_mirrors, 
                                                                      total_mirrors,
                                                                      available_mirrors * 100 / total_mirrors))
  if len(pending_mirrors) == 0:
    break

  if remaining > 0:
    p('Sleeping for {} seconds...\n'.format(remaining))
    time.sleep(remaining)


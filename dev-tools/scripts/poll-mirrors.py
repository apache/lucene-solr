#!/usr/bin/env python3
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

from urllib.parse import urlparse
from multiprocessing import Pool
import http.client as http


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


def check_mirror(url):
  if mirror_contains_file(url):
    p('.')
    return None
  else:
    p('\nFAIL: ' + url + '\n' if args.details else 'X')
    return url


desc = 'Periodically checks that all Lucene/Solr mirrors contain either a copy of a release or a specified path'
parser = argparse.ArgumentParser(description=desc)
parser.add_argument('-version', '-v', help='Lucene/Solr version to check')
parser.add_argument('-path', '-p', help='instead of a versioned release, check for some/explicit/path')
parser.add_argument('-interval', '-i', help='seconds to wait before re-querying mirrors', type=int, default=300)
parser.add_argument('-details', '-d', help='print missing mirror URLs', action='store_true', default=False)
parser.add_argument('-once', '-o', help='run only once', action='store_true', default=False)
args = parser.parse_args()

if (args.version is None and args.path is None) \
    or (args.version is not None and args.path is not None):
  p('You must specify either -version or -path but not both!\n')
  sys.exit(1)

try:
  conn = http.HTTPConnection('www.apache.org')
  conn.request('GET', '/mirrors/')
  response = conn.getresponse()
  html = response.read()
except Exception as e:
  p('Unable to fetch the Apache mirrors list!\n')
  sys.exit(1)

mirror_path = args.path if args.path is not None else 'lucene/java/{}/changes/Changes.html'.format(args.version)
maven_url = None if args.version is None else 'http://repo1.maven.org/maven2/' \
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
    pending_mirrors.append(match.group(1) + mirror_path)

total_mirrors = len(pending_mirrors)

label = args.version if args.version is not None else args.path
while True:
  p('\n{:%Y-%m-%d %H:%M:%S}'.format(datetime.datetime.now()))
  p('\nPolling {} Apache Mirrors'.format(len(pending_mirrors)))
  if maven_url is not None and not maven_available:
    p(' and Maven Central')
  p('...\n')

  if maven_url is not None and not maven_available:
    maven_available = mirror_contains_file(maven_url)

  start = time.time()
  with Pool(processes=5) as pool:
    pending_mirrors = list(filter(lambda x: x is not None, pool.map(check_mirror, pending_mirrors)))
  stop = time.time()
  remaining = args.interval - (stop - start)

  available_mirrors = total_mirrors - len(pending_mirrors)

  if maven_url is not None:
    p('\n\n{} is{}downloadable from Maven Central'.format(label, ' ' if maven_available else ' not '))
  p('\n{} is downloadable from {}/{} Apache Mirrors ({:.2f}%)\n'
    .format(label, available_mirrors, total_mirrors, available_mirrors * 100 / total_mirrors))
  if len(pending_mirrors) == 0 or args.once == True:
    break

  if remaining > 0:
    p('Sleeping for {:d} seconds...\n'.format(int(remaining + 0.5)))
    time.sleep(remaining)


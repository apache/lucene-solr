<!--
    Licensed to the Apache Software Foundation (ASF) under one or more
    contributor license agreements.  See the NOTICE file distributed with
    this work for additional information regarding copyright ownership.
    The ASF licenses this file to You under the Apache License, Version 2.0
    the "License"); you may not use this file except in compliance with
    the License.  You may obtain a copy of the License at

        http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
 -->

# What to do with existing GitHub PRs?

A pull request is a bunch of commits forking for some other
commit. As such, the same commits should be applicable to the
forked repository (Solr or Lucene), at least initially (until
the folder structure changes, for example).

Here is a sample workflow to apply an existing pull request from
Lucene:

https://github.com/apache/lucene-solr/pull/2459

1. You can just apply this PR directly as a patch:

git clone https://github.com/apache/lucene.git
cd lucene
wget https://github.com/apache/lucene-solr/pull/2459.patch
git apply 2459.patch
git add -A .
git commit -m "Applying PR # ..."
git push

2. You can "rebase" the PR via a separate fork on github. This preserves commit
   history but is slightly longer. Example:

- create a github fork of the lucene repository (mine at https://github.com/dweiss/lucene)
- locate the source repository and branch of the PR. For our example:
  https://github.com/apache/lucene-solr/pull/2459
  the source is:
  https://github.com/donnerpeter/lucene-solr/tree/revTrie

- Now create a "cloned" PR:
# clone your own fork
git clone https://github.com/dweiss/lucene.git
cd lucene
# add pr's repository as the remote and fetch commits from there
git remote add donnerpeter https://github.com/donnerpeter/lucene-solr.git
git fetch donnerpeter
# get the PR's branch:
git checkout donnerpeter/revTrie -b revTrie
# push to your own fork
git push origin HEAD -u
# The above will display a PR-creating link but you can also do this manually:
https://github.com/apache/lucene/compare/main...dweiss:revTrie
Look at the PR and create it if it looks good.
# This example's PR was at:
https://github.com/apache/lucene/pull/2

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
```
git clone https://github.com/apache/lucene.git
cd lucene
wget https://github.com/apache/lucene-solr/pull/2459.patch
git apply 2459.patch
git add -A .
git commit -m "Applying PR # ..."
git push
```
2. You can "rebase" the PR via a separate fork on github. This preserves commit
   history but is slightly longer. Example:

- create a github fork of the lucene repository (mine at https://github.com/dweiss/lucene)
- locate the source repository and branch of the PR. For our example:
  https://github.com/apache/lucene-solr/pull/2459
  the source is:
  https://github.com/donnerpeter/lucene-solr/tree/revTrie

- Now create a "cloned" PR:
```
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
```
3. Instead of rebasing, you can `git merge` the upstream `main` branch from the new
project into your existing PR branch, push the result to your own fork of the new
project, and use that as the basis for creating a PR against the new upstream project.

This approach works cleanly because the `main` branch on each of the new projects is
a direct descendant of the merge-base (with `lucene-solr/master`) of all existing
PR branches against the legacy joint `lucene-solr` project.

A benefit of a `merge`-based approach (as opposed to rebasing or applying a patch) is
that commit history (including commit hashes) is preserved, and remains compatible
across projects (and in some multi-commit cases, a merge-based approach can also
avoid the need to "re-resolve" related conflicts in multiple rebased commits).

NOTE: PRs updated in this way, if merged back into the `main` branch, could result
in an undesirably convoluted (if "correct") commit history. In many cases it may be
preferable to "squash-merge" such PRs (already a common general practice for merging
feature branches in these projects).

```
# clone new upstream project (optionally supplying remote name "apache" instead
# of "origin")
git clone --origin apache https://github.com/apache/lucene.git
cd lucene
# in Github UI, create ${user}'s fork of new project (as in the "rebase" example)
# add ${user}'s fork of the new project
git remote add mynewfork https://github.com/${user}/lucene.git
git fetch mynewfork
# add ${user}'s fork of the legacy (joint) project
git remote add mylegacyfork https://github.com/${user}/lucene-solr.git
git fetch mylegacyfork
# get the legacy PR's branch:
git checkout --no-track mylegacyfork/LUCENE-XXXX -b LUCENE-XXXX
# merge upstream main branch (a higher `merge.renameLimit` allows `git merge` to
# complete without the warning that it would otherwise print due to the large
# number of files deleted across the TLP split)
git -c merge.renameLimit=7000 merge apache/main
# after resolving any conflicts and committing the merge,
# push to ${user}'s new fork
git push -u mynewfork LUCENE-XXXX
# in Github UI, create PR from mynewfork/LUCENE-XXXX (against apache/main)
# as in the "rebase" example
```

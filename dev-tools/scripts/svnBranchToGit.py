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

""" Workaround for slow updates from svn to git.

Situation:

Remove svn repo   ---> slow git-svn update process --->    Remote git-svn repo (upstream)
   |                                                        |
   |                                                        |
   v                                                        v
Local svn working copy --> this workaround         --->    Local git repo

Because of the slow remote git-svn update process the remote git repo is (far) behind
the remote svn repo.


For a branch branchname in a local git repository following an upstream git-svn git repository,
this maintains commits on a temporary git branch branchname.svn in the local git repository.
These commits contain a message ending like this:
  "RepoUrl patch of svn diff -r EarlierSvnRevisionNumber:LatestSvnRevisionNumber".

The earlier revision number is taken from the git-svn-id message of git svn,
or from the LatestSvnRevisionNumber in the commit message of branchname.svn,
whichever is later.

This allows branchname.svn to be used as a local git branch instead of branchname
to develop new features locally, usually by mering branchname.svn into a feature branch.
Once the normal git-svn branch is up to date, it can also be merged.

In more detail:
  - update the svn working copy of the branch to the latest revision,
  - in the git repo:
  - fetch the git repository from upstream.
  - merge branchname from upstream/branchname, this is the branch that can be (far) behind.
  - use the git-svn-id from the latest git commit on this branch to determine the corresponding svn revision.
  - if the branchname.svn exists determine the latest svn revision from there.
  - choose the latest svn revision number available.
  - compare the git-svn revision to the svn latest revision (delay deleting a too early branchname.svn to later below).
  - when the git repository has the same revision:
    - exit reporting that branchname is up to date.
  - when the git repository has an earlier revision:
    - in the svn working copy, create a patch from the earlier revision into file ~/patches/branchname.svn
    - in the git working tree:
      - if branchname.svn is not at the earlier svn revision number, delete branchname.svn
      - if necessary create branch branchname.svn from branchname.
      - check out branchname.svn
      - apply the patch ~/patches/branchname.svn, ignoring whitespace differences.
      - commit with a message with revision numbers as indicated above
"""

import os
import subprocess
import StringIO


def svnSeq():
  return ("svn",)

def callSvn(*args):
  subprocess.check_call(svnSeq() + args)

def callSvnStdout(*args):
  return subprocess.check_output(svnSeq() + args)

def callSvnStdoutToFile(f, *args):
  subprocess.check_call(svnSeq() + args, stdout=f)


def gitCommand():
  return "git"

def gitAndRepoList(gitRepo):
  return (gitCommand(), "-C", gitRepo)

def callGitRepo(gitRepo, *args):
  subprocess.check_call(gitAndRepoList(gitRepo) + args)

def callGitStdout(gitRepo, *args):
  return subprocess.check_output(gitAndRepoList(gitRepo) + args)

def getGitCommitMessage(gitRepo, commitRef):
  return callGitStdout(gitRepo, "log", "--format=%B", "-n", "1", commitRef)



def lastChangedSvnRevision(svnInfo):
  lastChangedMarker = "Last Changed Rev: "
  after = svnInfo.split(lastChangedMarker)[1]
  splitAfter = after.split()
  return int(splitAfter[0])



def getGitSvnRemoteAndRevision(gitSvnCommitMessage): # from a git-svn commit
  words = gitSvnCommitMessage.split();
  svnIdMarker = "git-svn-id:"
  assert words.index(svnIdMarker) >= 0
  svnId = words[words.index(svnIdMarker) + 1]
  splitSvnId = svnId.split("@")
  return (splitSvnId[0], int(splitSvnId[1]))

def lastTempGitSvnRevision(gitCommitMessage): # from a commit generated here on the temp branch.
  parts = gitCommitMessage.split(":")
  lastPart = parts[-1].split()[0] # remove appended newlines
  try:
    return int(lastPart)
  except: # not generated here, ignore.
    print "Warning: svn revision range not found at end of commit message:\n", gitCommitMessage
    return None


def errorExit(*messageParts):
  raise Exception(" ".join(messageParts))

def maintainTempGitSvnBranch(branchName, tempGitBranchName, svnWorkingCopyOfBranch, gitRepo, gitUpstream, patchFileName):
  callGitRepo(gitRepo, "checkout", branchName) # fail when git working tree is not clean

  # CHECKME: add svn switch to branch here?

  callSvn("update", svnWorkingCopyOfBranch)

  svnInfo = callSvnStdout("info", svnWorkingCopyOfBranch)
  # print "svnInfo:", svnInfo
  lastSvnRevision = lastChangedSvnRevision(svnInfo)
  print svnWorkingCopyOfBranch, "lastSvnRevision:", lastSvnRevision

  callGitRepo(gitRepo, "fetch", gitUpstream)
  callGitRepo(gitRepo, "merge", branchName, gitUpstream + "/" + branchName)
  lastGitCommitMessage = getGitCommitMessage(gitRepo, branchName)
  print "lastGitCommitMessage:\n", lastGitCommitMessage
  (svnRemote, lastSvnRevisionOnGitSvnBranch) = getGitSvnRemoteAndRevision(lastGitCommitMessage)
  print "svnRemote:", svnRemote
  print gitRepo, branchName, "lastSvnRevisionOnGitSvnBranch:", lastSvnRevisionOnGitSvnBranch

  # check whether tempGitBranchName exists:
  diffBaseRevision = lastSvnRevisionOnGitSvnBranch
  svnTempRevision = None
  doCommitOnExistingTempBranch = False
  listOut = callGitStdout(gitRepo, "branch", "--list", tempGitBranchName)
  if listOut: # tempGitBranchName exists
    print tempGitBranchName, "exists"
    lastGitCommitMessage = getGitCommitMessage(gitRepo, tempGitBranchName)
    # update lastSvnRevisionOnGitSvnBranch from there.
    svnTempRevision = lastTempGitSvnRevision(lastGitCommitMessage)
    if svnTempRevision is not None:
      if svnTempRevision > lastSvnRevisionOnGitSvnBranch:
        doCommitOnExistingTempBranch = True
        diffBaseRevision = svnTempRevision

  if doCommitOnExistingTempBranch:
    callGitRepo(gitRepo, "checkout", tempGitBranchName) # checkout the temp branch.
    currentGitBranch = tempGitBranchName
  else:
    currentGitBranch = branchName

  if lastSvnRevision == diffBaseRevision:
    print gitRepo, currentGitBranch, "up to date with", svnWorkingCopyOfBranch
    return

  if lastSvnRevision < diffBaseRevision: # unlikely, do nothing
    print gitRepo, currentGitBranch, "later than", svnWorkingCopyOfBranch, ", nothing to update."
    return

  print gitRepo, currentGitBranch, "earlier than", svnWorkingCopyOfBranch

  # assert that the git working tree is on branchName
  gitStatus = callGitStdout(gitRepo, "status")
  # print "gitStatus:\n", gitStatus

  statusParts = gitStatus.split("On branch")
  actualBranchName = statusParts[1].split()[0]
  if actualBranchName != currentGitBranch:
    errorExit(gitRepo, "on unexpected branch", actualBranchName, "but expected", currentGitBranch)

  expSubString = "nothing to commit, working directory clean"
  if gitStatus.find(expSubString) < 0:
    errorExit(gitRepo, "on branch", actualBranchName, "not clean")

  print gitRepo,"on branch", actualBranchName, "and clean"

  # create patch file from svn between the revisions:
  revisionsRange = str(diffBaseRevision) + ":" + str(lastSvnRevision)
  patchFile = open(patchFileName, 'w')
  print "Creating patch from", svnWorkingCopyOfBranch, "between revisions", revisionsRange
  callSvnStdoutToFile(patchFile,
                      "diff", "-r", revisionsRange,
                      svnWorkingCopyOfBranch)
  patchFile.close()
  print "Created patch", patchFileName

  if not doCommitOnExistingTempBranch:
    listOut = callGitStdout(gitRepo, "branch", "--list", tempGitBranchName)
    if listOut: # tempGitBranchName exists, delete it first.
      print tempGitBranchName, "exists, deleting"
      callGitRepo(gitRepo, "branch", "-D", tempGitBranchName)
      # verify deletion:
      listOut = callGitStdout(gitRepo, "branch", "--list", tempGitBranchName)
      if listOut:
        errorExit("Could not delete", tempGitBranchName, "(", listOut, ")")

      callGitRepo(gitRepo, "branch", tempGitBranchName) # create a new tempGitBranchName
      callGitRepo(gitRepo, "checkout", tempGitBranchName) # checkout tempGitBranchName

  # apply the patch
  subprocess.check_call((gitCommand(), "apply",
                                      "-p6",  # FIXME: use depth of svnRepo from root to determine the depth to strip from patch.
                                      "--whitespace=nowarn",
                                      ("--directory=" + gitRepo),
                                      patchFileName))

  # add all patch changes to the git index to be committed.
  callGitRepo(gitRepo, "add", "-A")

  # Applying the patch leaves files that have been actually deleted at zero size.
  # Therefore delete empty patched files from the git repo that do not exist in svn working copy:
  indexPrefix = "^Index: "
  patchedFileNames = subprocess.check_output(("grep", indexPrefix, patchFileName))
  for indexPatchFileName in patchedFileNames.split("\n"):
    patchFileName = indexPatchFileName[len(indexPrefix):]
    versionControlledFileName = patchFileName[len(svnWorkingCopyOfBranch):]
    # print "Patched versionControlledFileName:", versionControlledFileName

    fileNameInGitRepo = gitRepo + "/" + versionControlledFileName
    if not os.path.isfile(fileNameInGitRepo): # already deleted or a directory.
      continue

    fileSize = os.path.getsize(fileNameInGitRepo)
    if fileSize > 0:
      # print "Non empty file patched normally:", fileNameInGitRepo
      continue

    # fileNameInGitRepo exists is empty
    if os.path.isfile(patchFileName):
      print "Left empty file:", fileNameInGitRepo
      continue

    callGitRepo(gitRepo, "rm", "-f", # force, the file is not up to date
                          fileNameInGitRepo)
    # print "Deleted empty file", fileNameInGitRepo # not needed, git rm is verbose enough

  # commit
  message = svnRemote + " patch of svn diff -r " + revisionsRange
  callGitRepo(gitRepo,
              "commit",
              "-a",
              "--message=" + message)

  callGitRepo(gitRepo, "clean", "-fd") # delete untracked directories and files



if __name__ == "__main__":

  repo = "lucene-solr"
  branchName = "trunk"
  tempGitBranchName = branchName + ".svn"

  home = os.path.expanduser("~")

  svnWorkingCopyOfBranch = home + "/svnwork/" + repo + "/" + branchName
  # CHECKME: branchName is not really needed here, svn can switch between branches.

  gitRepo = home + "/gitrepos/" + repo
  gitUpstream = "upstream"

  patchFileName = home + "/patches/" + tempGitBranchName


  maintainTempGitSvnBranch(branchName, tempGitBranchName, svnWorkingCopyOfBranch, gitRepo, gitUpstream, patchFileName)
  

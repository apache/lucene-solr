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

""" Workaround for slow updates from an svn branch to git.
See also jira issue INFRA-9182

Situation:

Remote svn repo        ---> (slow) git-svn fetch  --->   Remote git repo (upstream)
   |                                                        |
   |                                                        |
   v                                                        v
Local svn working copy ---> this workaround       --->   Local git repo

When the remote git-svn fetch is slow, the remote git repo is behind
the remote svn repo.

When this script is run it will first check that the local working copy and repository are clean.
Then it switches the svn working copy to the branch, which updates from the remote.
Then it the fetches branch from the git upstream repo, and merges the branch locally.
Normally the local svn and git will then be at the same svn revision, and the script will exit.

Otherwise the remote git repo is out of date, and the following happens.

For the branch branchname in a local git repository following an upstream git-svn git repository,
this maintains commits on a temporary git branch branchname.svn in the local git repository.
These commits contain a message ending like this:
  "SvnRepoUrl diff -r EarlierSvnRevisionNumber:NextSvnRevisionNumber".
Otherwise the messages of the added commits are the same as their counterparts from git svn.

Normally the added git commits and their git-svn counterparts have no differences between their working trees.
However such differences can occur, see also the documentation of git-svn reset and the limitations below.
In order not to interfere with git-svn this script only adds commits to a temporary branch
branchname.svn, and the commit messages are chosen differently, they do not contain git-svn-id.

In case an earlier branchname.svn exists, it will first be deleted if necessary,
and restarted at the later branch.
Therefore branchname.svn is temporary and should only be used locally.

By default, no more than 20 commits will be added to branchname.svn in a single run.

The earlier revision number is taken from the git-svn-id message of git svn,
or from the latest revision number in the commit message on branchname.svn,
whichever is later.

This allows branchname.svn to be used as a local git branch instead of branchname
to develop new features locally, for example by merging branchname.svn into a feature branch.
"""

""" Limitations:

This currently works by patching text, and therefore this does not work on binary files.
An example commit in lucene-solr that adds a binary file, on which this currently does not work correctly:
svn revision 1707457
git commit 3c0390f71e1f08a17f32bc207b4003362f8b6ac2

When the local svn working copy contains file after updating to the latest available revision,
and there is an interim commit that deletes this file, this file is left as an empty file in the working directory
of the local git repository.

All svn properties are ignored here.
"""

""" To be done:
Take binary files from the patch, and check out binary files directly from the remote svn repo directly into the local git repo.

Going really far: checkout each interim svn revision, and use all (changed) files from there instead of the text diff.
Determining all files under version control with svn (svn ls) is far too slow for this (esp. when compared to git ls-tree),
so this is probably better done by using svnsync to setup a local mirror repository following the remote,
and then using svnlook on the local mirror repository.
Doing that only to avoid the limitations of this workaround does not appear to be worthwhile.
"""

""" This was developed on Linux using the following program versions:
python 2.7.6
git 1.9.1
svn 1.8.8
grep (GNU grep) 2.16

gitk (part of git) was used for manual testing:
- reset branch to an earlier commit to simulate a non working update from svn to git,
- delete branchname.svn, reset branchname.svn to earlier,
- diff a commit generated here to a commit from git svn,
- update, reload, show commits by commit date, ...
"""

import os
import subprocess

from xml import sax
from xml.sax.handler import ContentHandler

import types

class SvnInfoHandler(ContentHandler):
  revisionAttr = "revision"

  def __init__(self):
    self.lastChangeRev = None

  def startElement(self, name, attrs):
    if name == "commit":
      self.lastChangeRev = int(attrs.getValue(self.revisionAttr))

  def getLastChangeRevision(self):
    return self.lastChangeRev


class SvnLogEntry(object):
  pass # attributes set in SvnLogHandler: revision, author, date, msg


class SvnLogHandler(ContentHandler): # collect list of SvnLogEntry's
  logEntryTag = "logentry"
  revisionAttr = "revision" # also used as SvnLogEntry attribute
  authorTag = "author"
  dateTag = "date"
  msgTag = "msg"
  charCollectTags = (authorTag, dateTag, msgTag) # also used as SvnLogEntry attributes

  def __init__(self):
    self.logEntries = []
    self.chars = None

  def startElement(self, name, attrs):
    if name == self.logEntryTag:
      self.lastLogEntry = SvnLogEntry()
      setattr(self.lastLogEntry, self.revisionAttr, int(attrs.getValue(self.revisionAttr)))
      for tag in self.charCollectTags:
        setattr(self.lastLogEntry, tag, None)
      return

    if name in self.charCollectTags:
      self.chars = ""

  def characters(self, content):
    if self.chars is not None:
      self.chars += content

  def endElement(self, name):
    if name in self.charCollectTags:
      setattr(self.lastLogEntry, name, self.chars)
      self.chars = None
      return

    if name == self.logEntryTag:
      self.logEntries.append(self.lastLogEntry)
      self.lastLogEntry = None

  def getLogEntries(self):
    return self.logEntries


class SubProcessAtPath(object):
  def __init__(self, pathName, verbose=True):
    assert pathName != ""
    self.pathName = pathName
    self.verbose = verbose

  def getPathName(self):
    return self.pathName

  def chDirToPath(self):
    if self.pathName != os.getcwd():
      os.chdir(self.pathName)
      assert self.pathName == os.getcwd()

  def __str__(self):
    return self.__class__.__name__ + "(" + self.pathName + ")"

  def checkCall(self, *args, **kwArgs):
    assert type(*args) != types.StringType
    self.chDirToPath()
    if self.verbose:
      print "check_call args:", " ".join(*args)
    subprocess.check_call(*args, **kwArgs)

  def checkOutput(self, *args, **kwArgs):
    assert type(*args) != types.StringType
    self.chDirToPath()
    if self.verbose:
      print "check_output args:", " ".join(*args)
    result = subprocess.check_output(*args, **kwArgs)
    if self.verbose:
      print "check_output result:", result
    return result


class SvnWorkingCopy(SubProcessAtPath):
  def __init__(self, pathName):
    SubProcessAtPath.__init__(self, pathName, verbose=False)

  svnCmd = "svn"

  def ensureNoLocalModifications(self):
    localMods = self.checkOutput((self.svnCmd, "status"))
    if localMods:
      errorExit(self, "should not have local modifications:\n", localMods)

  def update(self):
    self.checkCall((self.svnCmd, "update"))

  def switch(self, repoBranchName):
    self.checkCall((self.svnCmd, "switch", ("^/" + repoBranchName)))

  def lastChangedRevision(self):
    infoXml = self.checkOutput((self.svnCmd, "info", "--xml"))
    infoHandler = SvnInfoHandler()
    sax.parseString(infoXml, infoHandler)
    return infoHandler.getLastChangeRevision()

  def getLogEntries(self, fromRevision, toRevision, maxNumLogEntries):
    revRange = self.revisionsRange(fromRevision, toRevision)
    logXml = self.checkOutput((self.svnCmd, "log", "-r", revRange, "--xml", "-l", str(maxNumLogEntries)))
    logHandler = SvnLogHandler()
    sax.parseString(logXml, logHandler)
    return logHandler.getLogEntries()

  def revisionsRange(self, fromRevision, toRevision):
    return str(fromRevision) + ":" + str(toRevision)

  def createPatchFile(self, fromRevision, toRevision, patchFileName):
    revRange = self.revisionsRange(fromRevision, toRevision)
    patchFile = open(patchFileName, 'w')
    try:
      print "Creating patch from", self.pathName, "between revisions", revRange
      self.checkCall((self.svnCmd, "diff", "-r", revRange,
                                    "--ignore-properties"), # git apply can fail on svn properties.
                     stdout=patchFile)
    finally:
      patchFile.close()
    print "Created patch file", patchFileName

  def patchedFileNames(self, patchFileName): # return a sequence of the patched file names
    if os.path.getsize(patchFileName) == 0: # changed only svn properties, no files changed.
      return []

    indexPrefix = "Index: "
    regExp = "^" + indexPrefix # at beginning of line
    patchedFileNamesLines = self.checkOutput(("grep", regExp, patchFileName)) # grep exits 1 whithout any match.
    indexPrefixLength = len(indexPrefix)
    return [line[indexPrefixLength:]
            for line in patchedFileNamesLines.split("\n")
            if len(line) > 0]


class GitRepository(SubProcessAtPath):
  def __init__(self, pathName):
    SubProcessAtPath.__init__(self, pathName, verbose=False)
    self.currentBranch = None

  gitCmd = "git"

  def checkOutBranch(self, branchName):
    self.checkCall((self.gitCmd, "checkout", branchName))
    self.currentBranch = branchName

  def getCurrentBranch(self):
    if self.currentBranch is None:
      gitStatusOut = self.checkOutput((self.gitCmd, "status"))
      if gitStatusOut.startswith("On branch "):
        self.currentBranch = gitStatusOut.split[2]
      else:
        errorExit(self, "not on a branch:", gitStatusOut)
    return self.currentBranch

  def workingDirectoryClean(self):
    gitStatusOut = self.checkOutput((self.gitCmd, "status"))
    expSubString = "nothing to commit, working directory clean"
    return gitStatusOut.find(expSubString) >= 0

  def listBranches(self, pattern):
    return self.checkOutput((self.gitCmd, "branch", "--list", pattern))

  def branchExists(self, branchName):
    listOut = self.listBranches(branchName) # CHECKME: using branchName as pattern may not always be ok.
    return len(listOut) > 0

  def deleteBranch(self, branchName):
    self.checkCall((self.gitCmd, "branch", "-D", branchName))
    if branchName == self.currentBranch:
      self.currentBranch = None

  def createBranch(self, branchName):
    self.checkCall((self.gitCmd, "branch", branchName))

  def fetch(self, upStream):
    self.checkCall((self.gitCmd, "fetch", upStream))

  def merge(self, branch, fromBranch):
    self.checkCall((self.gitCmd, "merge", branch, fromBranch))

  def getCommitMessage(self, commitRef):
    return self.checkOutput((self.gitCmd, "log", "--format=%B", "-n", "1", commitRef))

  def getCommitAuthorName(self, commitRef):
    return self.checkOutput((self.gitCmd, "log", "--format=%aN", "-n", "1", commitRef))

  def getCommitAuthorEmail(self, commitRef):
    return self.checkOutput((self.gitCmd, "log", "--format=%aE", "-n", "1", commitRef))

  def getLatestCommitForAuthor(self, svnAuthor):
    authorCommit = self.checkOutput(
                    " ".join((self.gitCmd,
                              "rev-list", "--all", "-i", ("--author=" + svnAuthor), # see git commit documentation on --author
                              "|",                               # pipe should have a buffer for at most a few commit ids.
                              "head", "-1")),
                    shell=True) # use shell pipe
    authorCommit = authorCommit.rstrip("\n")
    return authorCommit

  def getSvnRemoteAndRevision(self, gitSvnCommitRef):
    gitSvnCommitMessage = self.getCommitMessage(gitSvnCommitRef)
    words = gitSvnCommitMessage.split();
    svnIdMarker = "git-svn-id:"
    assert words.index(svnIdMarker) >= 0
    svnId = words[words.index(svnIdMarker) + 1]
    splitSvnId = svnId.split("@")
    svnRemote = splitSvnId[0]
    svnRevision = int(splitSvnId[1])
    return (svnRemote, svnRevision)

  def lastTempGitSvnRevision(self, branchName): # at a commit generated here on the temp branch.
    gitCommitMessage = self.getCommitMessage(branchName)
    parts = gitCommitMessage.split(":")
    lastPart = parts[-1].split()[0] # remove appended newlines
    try:
      return int(lastPart)
    except: # not generated here, ignore.
      print "Warning: svn revision range not found at end of commit message:\n", gitCommitMessage
      return None

  def applyPatch(self, patchFileName, stripDepth):
    self.checkCall((self.gitCmd, "apply",
                                  ("-p" + str(stripDepth)),
                                  "--whitespace=nowarn",
                                  patchFileName))

  def addAllToIndex(self):
    self.checkCall((self.gitCmd, "add", "-A"))

  def deleteForced(self, fileName):
    self.checkCall((self.gitCmd, "rm", "-f", fileName))

  def commit(self, message,
                  authorName, authorEmail, authorDate,
                  committerName, committerEmail, committerDate):
    author = ''.join((authorName, " <", authorEmail, ">"))
    os.environ["GIT_COMMITTER_NAME"] = committerName # no need to save/restore earlier environment state.
    os.environ["GIT_COMMITTER_EMAIL"] = committerEmail
    os.environ["GIT_COMMITTER_DATE"] = committerDate
    self.checkCall((self.gitCmd, "commit",
                                  "--allow-empty", # only svn poperties changed.
                                  ("--message=" + message),
                                  ("--author=" + author),
                                  ("--date=" + authorDate) ))

  def cleanDirsForced(self):
    self.checkCall((self.gitCmd, "clean", "-fd"))



def errorExit(*messageParts):
  raise RuntimeError(" ".join(map(str, messageParts)))


def allSuccessivePairs(lst):
  return [lst[i:i+2] for i in range(len(lst)-1)]


def maintainTempGitSvnBranch(branchName, tempGitBranchName,
                              svnWorkingCopyOfBranchPath, svnRepoBranchName,
                              gitRepoPath, gitUpstream,
                              patchFileName,
                              maxCommits=20, # generate at most this number of commits on tempGitBranchName, rerun to add more.
                              testMode=False):

  assert maxCommits >= 1

  gitRepo = GitRepository(gitRepoPath)
  gitRepo.checkOutBranch(branchName) # fails with git message when working directory is not clean

  svnWorkingCopy = SvnWorkingCopy(svnWorkingCopyOfBranchPath)
  svnWorkingCopy.ensureNoLocalModifications()
  svnWorkingCopy.switch(svnRepoBranchName) # switch to repo branch, update to latest revision

  lastSvnRevision = svnWorkingCopy.lastChangedRevision()
  # print svnWorkingCopy, "lastSvnRevision:", lastSvnRevision

  gitRepo.fetch(gitUpstream)
  if testMode:
    pass # leave branch where it is, as if the last commits from upstream did not arrive
  else:
    gitRepo.merge(branchName, gitUpstream + "/" + branchName)

  (svnRemote, lastSvnRevisionOnGitSvnBranch) = gitRepo.getSvnRemoteAndRevision(branchName)
  print "svnRemote:", svnRemote
  #print gitRepo, branchName, "lastSvnRevisionOnGitSvnBranch:", lastSvnRevisionOnGitSvnBranch

  # check whether tempGitBranchName exists:
  diffBaseRevision = lastSvnRevisionOnGitSvnBranch
  svnTempRevision = None
  doCommitOnExistingTempBranch = False

  if gitRepo.branchExists(tempGitBranchName):
    print tempGitBranchName, "exists"
    # update lastSvnRevisionOnGitSvnBranch from there.
    svnTempRevision = gitRepo.lastTempGitSvnRevision(tempGitBranchName)
    if svnTempRevision is None:
      print "Warning: no svn revision found on branch:", tempGitBranchName
    else:
      if svnTempRevision > lastSvnRevisionOnGitSvnBranch:
        diffBaseRevision = svnTempRevision
        doCommitOnExistingTempBranch = True
        gitRepo.checkOutBranch(tempGitBranchName)

  if lastSvnRevision == diffBaseRevision:
    print gitRepo, gitRepo.getCurrentBranch(), "up to date with", svnWorkingCopy, svnRepoBranchName
    return

  if lastSvnRevision < diffBaseRevision: # unlikely, do nothing
    print gitRepo, gitRepo.getCurrentBranch(), "later than", svnWorkingCopy, ", nothing to update."
    # CHECK: generate svn commits from the git commits?
    return

  print gitRepo, gitRepo.getCurrentBranch(), "earlier than", svnWorkingCopy

  if not gitRepo.workingDirectoryClean():
    errorExit(gitRepo, "on branch", gitRepo.getCurrentBranch(), "not clean")

  print gitRepo,"on branch", gitRepo.getCurrentBranch(), "and clean"

  if not doCommitOnExistingTempBranch: # restart temp branch from branch
    assert gitRepo.getCurrentBranch() == branchName
    if gitRepo.branchExists(tempGitBranchName): # tempGitBranchName exists, delete it first.
      print "Branch", tempGitBranchName, "exists, deleting"
      gitRepo.deleteBranch(tempGitBranchName)
      if gitRepo.branchExists(tempGitBranchName):
        errorExit("Could not delete branch", tempGitBranchName, "from", gitRepo)

    gitRepo.createBranch(tempGitBranchName)
    gitRepo.checkOutBranch(tempGitBranchName)
    print "Started branch", tempGitBranchName, "at", branchName

  assert gitRepo.getCurrentBranch() == tempGitBranchName

  patchStripDepth = 0 # patch generated at svn repo.

  maxNumLogEntries = maxCommits + 1
  svnLogEntries = svnWorkingCopy.getLogEntries(diffBaseRevision, lastSvnRevision, maxNumLogEntries)

  numCommits = 0

  for (logEntryFrom, logEntryTo) in allSuccessivePairs(svnLogEntries):
    # create patch file from svn between the revisions:
    svnWorkingCopy.createPatchFile(logEntryFrom.revision, logEntryTo.revision, patchFileName)

    patchedFileNames = svnWorkingCopy.patchedFileNames(patchFileName)

    if os.path.getsize(patchFileName) > 0:
      gitRepo.applyPatch(patchFileName, patchStripDepth)
      print "Applied patch", patchFileName
    else: # only svn properties changed, do git commit for commit info only.
      print "Empty patch", patchFileName

    gitRepo.addAllToIndex() # add all patch changes to the git index to be committed.

    # Applying the patch leaves files that have been actually deleted at zero size.
    # Therefore delete empty patched files from the git repo that do not exist in svn working copy:
    for patchedFileName in patchedFileNames:
      fileNameInGitRepo = os.path.join(gitRepo.getPathName(), patchedFileName)
      fileNameInSvnWorkingCopy = os.path.join(svnWorkingCopy.getPathName(), patchedFileName)

      if os.path.isdir(fileNameInGitRepo):
        # print "Directory:", fileNameInGitRepo
        continue

      if not os.path.isfile(fileNameInGitRepo):
        print "Possibly new binary file in svn, ignored here:", fileNameInGitRepo
        # FIXME: Take a new binary file out of the svn repository directly.
        continue

      fileSize = os.path.getsize(fileNameInGitRepo)
      if fileSize > 0:
        # print "Non empty file patched normally:", fileNameInGitRepo
        continue

      # fileNameInGitRepo exists and is empty
      if os.path.isfile(fileNameInSvnWorkingCopy):
        # FIXME: this only works correctly when the svn working copy is hecked out at the target revision.
        print "Left empty file:", fileNameInGitRepo
        continue

      gitRepo.deleteForced(fileNameInGitRepo) # force, the file is not up to date. This also stages the delete for commit.
      # print "Deleted empty file", fileNameInGitRepo # not needed, git rm is verbose enough

    # commit, put toRevision at end so it can be picked up later.
    revisionsRange = svnWorkingCopy.revisionsRange(logEntryFrom.revision, logEntryTo.revision)
    message = logEntryTo.msg + "\n\n" + svnRemote + " diff -r " + revisionsRange
    authorCommit = gitRepo.getLatestCommitForAuthor(logEntryTo.author)
    authorName = gitRepo.getCommitAuthorName(authorCommit)
    authorEmail = gitRepo.getCommitAuthorEmail(authorCommit)
    # print "Author name and email:", authorName, authorEmail
    gitRepo.commit(message,
                    authorName, authorEmail, logEntryTo.date,
                    authorName, authorEmail, logEntryTo.date) # author is also git committer, just like git-svn

    numCommits += 1

    # print "Commit  author:", logEntryTo.author
    # print "Commit    date:", logEntryTo.date
    print "Commit message:", logEntryTo.msg

    gitRepo.cleanDirsForced() # delete untracked directories and files

    if not gitRepo.workingDirectoryClean():
      errorExit(gitRepo, "on branch", gitRepo.getCurrentBranch(), "not clean, numCommits:", numCommits)

  print "Added", numCommits, "commit(s) to branch", tempGitBranchName


if __name__ == "__main__":

  testMode = False # when true, leave branch where it is, as if the last commits from upstream did not arrive
  defaultMaxCommits = 20
  maxCommits = defaultMaxCommits

  import sys
  argv = sys.argv[1:]
  while argv:
    if argv[0] == "test":
      testMode = True
    else:
      try:
        maxCommits = int(argv[0])
        assert maxCommits >= 1
      except:
        errorExit("Argument(s) should be test and/or a maximum number of commits, defaults are false and " + defaultMaxCommits)
    argv = argv[1:]

  repo = "lucene-solr"
  branchName = "trunk"
  tempGitBranchName = branchName + ".svn"

  home = os.path.expanduser("~")

  svnWorkingCopyOfBranchPath = os.path.join(home, "svnwork", repo, branchName)
  svnRepoBranchName = "lucene/dev/" + branchName # for svn switch to

  gitRepo = os.path.join(home, "gitrepos", repo)
  gitUpstream = "upstream"

  patchFileName = os.path.join(home, "patches", tempGitBranchName)

  maintainTempGitSvnBranch(branchName, tempGitBranchName,
                            svnWorkingCopyOfBranchPath, svnRepoBranchName,
                            gitRepo, gitUpstream,
                            patchFileName,
                            maxCommits=maxCommits,
                            testMode=testMode)

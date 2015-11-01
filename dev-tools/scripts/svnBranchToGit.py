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
See also jira issue INFRA-9182

Situation:

Remove svn repo   ---> slow git-svn update process --->    Remote git-svn repo (upstream)
   |                                                        |
   |                                                        |
   v                                                        v
Local svn working copy --> this workaround         --->    Local git repo

Because of the slow remote git-svn update process the remote git repo is (far) behind
the remote svn repo.

When this script is run it will first check that the local repositories are clean.
Then it switches the svn working copy to branch, which updates from the remote.
Then it fetches branch from the git upstream repo, and merges the branch locally.
Normally the local svn and git will then be at the same svn revision, and the script will exit.

Otherwise the remote git repo is out of date, and the following happens.

For the branch branchname in a local git repository following an upstream git-svn git repository,
this maintains commits on a temporary git branch branchname.svn in the local git repository.
These commits contain a message ending like this:
  "SvnRepoUrl diff -r EarlierSvnRevisionNumber:NextSvnRevisionNumber".
Otherwise the added commit messages look a lot like their counterparts from git svn,
only the committer is taken from the local git settings.

In case an earlier branchname.svn exists, it will first be deleted if necessary,
and restarted at the later branch.
Therefore branchname.svn is temporary and should only be used locally.

By default, no more than 20 commits will be added to branchname.svn in a single run.

The earlier revision number is taken from the git-svn-id message of git svn,
or from the LatestSvnRevisionNumber in the commit message of branchname.svn,
whichever is later.

This allows branchname.svn to be used as a local git branch instead of branchname
to develop new features locally, usually by mering branchname.svn into a feature branch.

In more detail:
  - switch the svn working copy to the branch, updating it to the latest revision,
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
  - in the git working tree:
    - if branchname.svn is not at the earlier svn revision number:
      - delete branchname.svn
      - recreate branch branchname.svn from branchname.
    - check out branchname.svn
  - get the svn commits from the latest available git svn commit (possible generated here), this uses the remote svn repo,
    to the latest one from the svn log (but no more than the maximum):
  - for all these commits:
    - from the svn working copy, create a patch for the svn commit into file ~/patches/branchname.svn,
      this takes most the the time as it uses the remote svn repo.
    - in the git working tree:
      - apply the patch ~/patches/branchname.svn, ignoring whitespace differences.
      - commit using author, date and message from the svn log, and append the message with revision numbers. 
"""

"""
This was developed on Linux using the following program versions:
python 2.7.6
git 1.9.1
svn 1.8.8
grep (GNU grep) 2.16

gitk (part of git) was used for manual testing:
- reset branch to an earlier commit to simulate a non working update from svn to git,
- delete branchname.svn, reset branchname.svn to earlier,
- diff a commit generated here to a commit from git svn, diffs between corresponding commits are normally empty,
- update, reload, sort commits by commit date, ...
"""

import os
import subprocess

from xml import sax
from xml.sax.handler import ContentHandler

import types

class SvnInfoHandler(ContentHandler):
  revisionAttr = "revision"

  def __init__(self):
    self.lastChangedRevision = None

  def startElement(self, name, attrs):
    if name == "commit":
      self.lastChangeRev = int(attrs.getValue(self.revisionAttr))

  def lastChangeRevision(self):
    return self.lastChangeRev


class SvnLogEntry:
  pass # attributes set in SvnLogHandler: revision, author, date, msg


class SvnLogHandler(ContentHandler): # collect list of SvnLogEntry's
  tagLogEntry = "logentry"
  revisionAttr = "revision"
  tagAuthor = "author"
  tagDate = "date"
  tagMsg = "msg"
  charCollectTags = (tagAuthor, tagDate, tagMsg) # also used as SvnLogEntry attributes

  def __init__(self):
    self.logEntries = []
    self.chars = None

  def startElement(self, name, attrs):
    if name == self.tagLogEntry:
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

    if name == self.tagLogEntry:
      self.logEntries.append(self.lastLogEntry)
      self.lastLogEntry = None

  def getLogEntries(self):
    return self.logEntries


class PathName:
  def __init__(self, pathName):
    self.pathName = pathName

  def getPathName(self):
    return self.pathName

  def __str__(self):
    return self.__class__.__name__ + "(" + self.pathName + ")"


class SubProcess:
  def __init__(self, verbose=True):
    self.verbose = verbose

  def check_call(self, *args, **kwArgs):
    assert type(*args) != types.StringType
    if self.verbose:
      print "check_call args:", " ".join(*args)
    subprocess.check_call(*args, **kwArgs)

  def check_call_silent(self, *args, **kwArgs): # ignore self.verbose
    assert type(*args) != types.StringType
    subprocess.check_call(*args, **kwArgs)

  def check_output(self, *args, **kwArgs):
    assert type(*args) != types.StringType
    if self.verbose:
      print "check_output args:", " ".join(*args)
    result = subprocess.check_output(*args, **kwArgs)
    if self.verbose:
      print "check_output result:", result
    return result

  def check_output_silent(self, *args, **kwArgs): # ignore self.verbose
    assert type(*args) != types.StringType
    return subprocess.check_output(*args, **kwArgs)


class SvnWorkingCopy(PathName, SubProcess):
  def __init__(self, pathName):
    PathName.__init__(self, pathName)
    SubProcess.__init__(self, verbose=False)

  svnCmd = "svn"

  def ensureNoLocalModifications(self):
    localMods = self.check_output((self.svnCmd, "status", self.pathName))
    if localMods:
      errorExit(self, "should not have local modifications:\n", localMods)

  def update(self):
    self.check_call((self.svnCmd, "update", self.pathName))

  def switch(self, repoBranchName):
    self.check_call((self.svnCmd, "switch", ("^/" + repoBranchName), self.pathName))

  def lastChangedRevision(self):
    infoXml = self.check_output_silent((self.svnCmd, "info", self.pathName, "--xml"))
    infoHandler = SvnInfoHandler()
    sax.parseString(infoXml, infoHandler)
    return infoHandler.lastChangeRevision()

  def getLogEntries(self, fromRevision, toRevision, maxNumLogEntries):
    revRange = self.revisionsRange(fromRevision, toRevision)
    logXml = self.check_output_silent((self.svnCmd, "log", self.pathName, "-r", revRange, "--xml", "-l", str(maxNumLogEntries)))
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
      self.check_call((self.svnCmd, "diff", "-r", revRange, self.pathName), stdout=patchFile)
    finally:
      patchFile.close()
    print "Created patch file", patchFileName

  def patchedFileNames(self, patchFileName): # return a sequence of the patched absolute file names
    indexPrefix = "Index: "
    regExp = "^" + indexPrefix # at beginning of line
    patchedFileNamesLines = self.check_output(("grep", regExp, patchFileName))
    indexPrefixLength = len(indexPrefix)
    return [line[indexPrefixLength:]
            for line in patchedFileNamesLines.split("\n")
            if len(line) > 0]


class GitRepository(PathName, SubProcess):
  def __init__(self, pathName):
    PathName.__init__(self, pathName)
    SubProcess.__init__(self, verbose=False)
    self.currentBranch = None

  gitCmd = "git"

  def _cmdForPath(self):
    return (self.gitCmd, "-C", self.pathName)

  def _statusCmd(self):
    return (self._cmdForPath() + ("status",))

  def checkOutBranch(self, branchName):
    self.check_call(self._cmdForPath() + ("checkout", branchName))
    self.currentBranch = branchName

  def getCurrentBranch(self):
    if self.currentBranch is None:
      gitStatusOut = self.check_output(self._statusCmd())
      if gitStatusOut.startswith("On branch "):
        self.currentBranch = gitStatusOut.split[2]
      else:
        errorExit(self, "not on a branch:", gitStatusOut)
    return self.currentBranch

  def workingDirectoryClean(self):
    gitStatusOut = self.check_output(self._statusCmd())
    expSubString = "nothing to commit, working directory clean"
    return gitStatusOut.find(expSubString) >= 0

  def listBranches(self, pattern):
    return self.check_output(self._cmdForPath() + ("branch", "--list", pattern))

  def branchExists(self, branchName):
    listOut = self.listBranches(branchName)
    return len(listOut) > 0

  def deleteBranch(self, branchName):
    self.check_call(self._cmdForPath() + ("branch", "-D", branchName))
    if branchName == self.currentBranch:
      self.currentBranch = None

  def createBranch(self, branchName):
    self.check_call(self._cmdForPath() + ("branch", branchName))

  def fetch(self, upStream):
    self.check_call(self._cmdForPath() + ("fetch", upStream))

  def merge(self, branch, fromBranch):
    self.check_call(self._cmdForPath() + ("merge", branch, fromBranch))

  def getCommitMessage(self, commitRef):
    return self.check_output(self._cmdForPath() + ("log", "--format=%B", "-n", "1", commitRef))

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
    self.check_call((self.gitCmd, "apply",
                                        ("-p" + str(stripDepth)),
                                        "--whitespace=nowarn",
                                        ("--directory=" + self.pathName),
                                        patchFileName))

  def addAllToIndex(self):
    self.check_call(self._cmdForPath() + ("add", "-A"))

  def deleteForced(self, fileName):
    self.check_call(self._cmdForPath() + ("rm", "-f", fileName))

  def commit(self, message, author, date):
    self.check_call(self._cmdForPath()
                          + ("commit",
                              ("--message=" + message),
                              ("--author=" + author),
                              ("--date=" + date) ))

  def cleanDirsForced(self):
    self.check_call(self._cmdForPath() + ("clean", "-fd"))



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

  lenSvnWorkingCopyPathName = len(svnWorkingCopy.getPathName())
  patchStripDepth = len(svnWorkingCopy.getPathName().split(os.sep))

  maxNumLogEntries = maxCommits + 1
  svnLogEntries = svnWorkingCopy.getLogEntries(diffBaseRevision, lastSvnRevision, maxNumLogEntries)

  for (logEntryFrom, logEntryTo) in allSuccessivePairs(svnLogEntries):
    print ""

    # create patch file from svn between the revisions:
    svnWorkingCopy.createPatchFile(logEntryFrom.revision, logEntryTo.revision, patchFileName)
    patchedFileNames = svnWorkingCopy.patchedFileNames(patchFileName)

    gitRepo.applyPatch(patchFileName, patchStripDepth)
    print "Applied patch", patchFileName

    gitRepo.addAllToIndex() # add all patch changes to the git index to be committed.

    # Applying the patch leaves files that have been actually deleted at zero size.
    # Therefore delete empty patched files from the git repo that do not exist in svn working copy:
    for patchedFileName in patchedFileNames:
      versionControlledFileName = patchedFileName[lenSvnWorkingCopyPathName:] # includes leading slash
      fileNameInGitRepo = gitRepo.getPathName() + versionControlledFileName

      if os.path.isdir(fileNameInGitRepo):
        # print "Directory:", fileNameInGitRepo
        continue

      if not os.path.isfile(fileNameInGitRepo):
        print "Already deleted:", fileNameInGitRepo
        continue

      fileSize = os.path.getsize(fileNameInGitRepo)
      if fileSize > 0:
        # print "Non empty file patched normally:", fileNameInGitRepo
        continue

      # fileNameInGitRepo exists is empty
      if os.path.isfile(patchedFileName):
        print "Left empty file:", fileNameInGitRepo
        continue

      gitRepo.deleteForced(fileNameInGitRepo) # force, the file is not up to date. This also stages the delete for commit.
      # print "Deleted empty file", fileNameInGitRepo # not needed, git rm is verbose enough

    # commit, put toRevision at end so it can be picked up later.
    revisionsRange = svnWorkingCopy.revisionsRange(logEntryFrom.revision, logEntryTo.revision)
    message = logEntryTo.msg + "\n\n" + svnRemote + " diff -r " + revisionsRange
    gitRepo.commit(message,
                  logEntryTo.author, # this normally matches a full earlier author entry that git will then use.
                  logEntryTo.date)

    print "Commit  author:", logEntryTo.author
    print "Commit    date:", logEntryTo.date
    print "Commit message:", logEntryTo.msg

    gitRepo.cleanDirsForced() # delete untracked directories and files

    if not gitRepo.workingDirectoryClean():
      errorExit(gitRepo, "on branch", gitRepo.getCurrentBranch(), "not clean")


if __name__ == "__main__":

  import sys

  testMode = False # when true, leave branch where it is, as if the last commits from upstream did not arrive
  defaultMaxCommits = 20
  maxCommits = defaultMaxCommits

  argv = sys.argv[1:]
  while argv:
    if argv[0] == "test":
      testMode = True
    else:
      try:
        maxCommits = int(argv[0])
        assert maxCommits >= 1
      except:
        errorExit("Argument(s) must be test and/or a maximum number of commits, defaults are false and " + defaultMaxCommits)
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


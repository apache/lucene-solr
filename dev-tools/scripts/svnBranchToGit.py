from __future__ import print_function
"""
To be done:
- Investigate whether it is possible to obtain the last svn revision number without switching to it.
- Investigate file mode differences reported by gitk, see svn revision 171449.
- simplify difference check to a single call to diff.
  Verify that all common files are equal, ignore non common files, check stderr and stdout of diff.
"""

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
Then it fetches the branch from the git upstream repo, and merges the branch locally.
Normally the local svn and git will then be at the same svn revision, and the script will exit.

Otherwise the remote git repo is out of date, and the following happens.
It is checked that the hostname and path and the uuid of the remote svn repo
as reported by the local svn working copy and as reported by the local git repo
are the same.

For the branch branchname in a local git repository following an upstream git-svn git repository,
this maintains commits on a temporary git branch branchname.svn in the local git repository.
These commits contain metdata that differs slightly from git svn (svn2git-id: instead of git-svn-id:).
Otherwise the messages of the added commits are the same as their counterparts from git svn,
except occasionally for an added or missed empty line when the svn commit message ends in new line.

Normally the added git commits and their git-svn counterparts have no differences between their working trees.
However such differences can occur, for example occasionally file modes are different in the git working tree.
See also the documentation of git-svn reset and the limitations below.
In order not to interfere with git-svn this script only adds commits to a temporary branch
branchname.svn, and the commit messages are chosen differently, they do not contain git-svn-id: .

In case an earlier branchname.svn exists, it will first be deleted if necessary,
and restarted at the later branch.
Therefore branchname.svn is temporary and should only be used locally.

By default, no more than 20 commits will be added to branchname.svn in a single run.

The earlier revision number is taken from the git-svn-id: message of git svn,
or from the latest revision number in the commit message on branchname.svn,
whichever is later.

This allows branchname.svn to be used as a local git branch instead of branchname
to develop new features locally, for example by merging branchname.svn into a feature branch.

This works by interpretation of the lines of svn update messages (U/A/D etc.)
by copying these files and their protection bits from the local svn working copy into the git working tree,
and by deleting files and directories in the git working tree.

An example commit in lucene-solr that adds a binary file, on which this script provides a correct git working tree:
svn revision 1707457
git-svn commit 3c0390f71e1f08a17f32bc207b4003362f8b6ac2


Limitations:

All svn properties are ignored here.
Commit messages added to the git repo occasionally do not have the same number of empty lines
as the corresponding svn commit message.
"""


""" This was developed on Linux using the following program versions:
python 2.7.6
python 3.4.3
git 1.9.1
svn 1.8.8
GNU bash, version 4.3.11(1)-release (x86_64-pc-linux-gnu)
sed (GNU sed) 4.2.2
grep (GNU grep) 2.16
diff (GNU diffutils) 3.3
cp (GNU coreutils) 8.21
rm (GNU coreutils) 8.21
mkdir (GNU coreutils) 8.21

gitk (part of git) was used for manual testing:
- delete branchname.svn, reset branchname.svn and branchname to earlier to simulate going back in history,
- diff a commit generated here to a commit from git svn, ideally there are no differences,
- update, reload, show commits in reverse order of commit date, ...
"""

import os
import subprocess
import shutil

from xml import sax
from xml.sax.handler import ContentHandler

try:
  from urllib.parse import urlparse # python 3
except ImportError:
  from urlparse import urlparse # python 2

import sys
binaryToString = sys.version_info >= (3, 0)

def decodeBytesToString(bytes):
  return bytes.decode("utf-8")


class SvnInfoHandler(ContentHandler):
  commitTag = "commit"
  revisionAttr = "revision"

  urlTag = "url"
  uuidTag = "uuid"
  charCollectTags = (urlTag, uuidTag) # also used as SvnInfoHandler attributes

  def __init__(self):
    self.lastChangeRev = None
    self.lastLogEntry = None
    for tag in self.charCollectTags:
      setattr(self, tag, None)
    self.chars = None

  def startElement(self, name, attrs):
    if name == self.commitTag:
      self.lastChangeRev = int(attrs.getValue(self.revisionAttr))
    elif name in self.charCollectTags:
      self.chars = ""

  def characters(self, content):
    if self.chars is not None:
      self.chars += content

  def endElement(self, name):
    if name in self.charCollectTags:
      chars = self.chars
      setattr(self, name, chars)
      self.chars = None

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
      chars = self.chars
      setattr(self.lastLogEntry, name, chars)
      self.chars = None
      return

    if name == self.logEntryTag:
      self.logEntries.append(self.lastLogEntry)
      self.lastLogEntry = None

  def getLogEntries(self):
    return self.logEntries


class SubProcessAtPath(object):
  def __init__(self, pathName, verbose=True):
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
    self.chDirToPath()
    if self.verbose:
      print("check_call args:", " ".join(*args), str(**kwArgs))
    subprocess.check_call(*args, **kwArgs)

  def checkOutput(self, *args, **kwArgs):
    self.chDirToPath()
    if self.verbose:
      print("check_output args:", " ".join(*args), str(**kwArgs))
    result = subprocess.check_output(*args, **kwArgs)
    if self.verbose:
      print("check_output result:", result)
    return result

  def checkOutputAsStr(self, *args, **kwArgs):
    self.chDirToPath()
    if self.verbose:
      print("check_output args:", " ".join(*args), str(**kwArgs))
    result = subprocess.check_output(*args, **kwArgs)
    if binaryToString:
      result = decodeBytesToString(result)
    if self.verbose:
      print("check_output result:", result)
    return result

def nonEmptyLines(text):
  return [line for line in text.split("\n") if len(line) > 0]



class SvnWorkingCopy(SubProcessAtPath):
  def __init__(self, pathName):
    SubProcessAtPath.__init__(self, pathName, verbose=False)
    self.url = None
    self.uuid = None

  svnCmd = "svn"

  def ensureNoLocalModifications(self):
    localMods = self.checkOutputAsStr((self.svnCmd, "status"))
    if localMods:
      errorExit(self, "should not have local modifications:\n", localMods)

  def updateOutput(self, revision):
    result = self.checkOutputAsStr((self.svnCmd, "update", "-r", str(revision)))
    return result

  def switch(self, repoBranchName):
    self.checkCall((self.svnCmd, "switch", ("^/" + repoBranchName)))

  def parseInfo(self):
    infoXml = self.checkOutput((self.svnCmd, "info", "--xml")) # bytes in python 3.
    infoHandler = SvnInfoHandler()
    sax.parseString(infoXml, infoHandler)
    self.uuid = infoHandler.uuid
    self.url = infoHandler.url
    self.lastChangeRev = infoHandler.getLastChangeRevision()

  def getUrl(self):
    if self.url == None:
      self.parseInfo()
    return self.url

  def getUuid(self):
    if self.uuid == None:
      self.parseInfo()
    return self.uuid

  def lastChangedRevision(self):
    self.parseInfo()
    return self.lastChangeRev

  def getLogEntries(self, fromRevision, toRevision, maxNumLogEntries):
    revRange = self.revisionsRange(fromRevision, toRevision)
    logXml = self.checkOutput((self.svnCmd, "log", "-r", revRange, "--xml", "-l", str(maxNumLogEntries)))
    logHandler = SvnLogHandler()
    sax.parseString(logXml, logHandler)
    return logHandler.getLogEntries()

  def revisionsRange(self, fromRevision, toRevision):
    return str(fromRevision) + ":" + str(toRevision)



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
      gitStatusOut = self.checkOutputAsStr((self.gitCmd, "status"))
      if gitStatusOut.startswith("On branch "):
        self.currentBranch = gitStatusOut.split()[2] # works also without () ???
      else:
        errorExit(self, "not on a branch:", gitStatusOut)
    return self.currentBranch

  def workingDirectoryClean(self):
    gitStatusOut = self.checkOutputAsStr((self.gitCmd, "status"))
    expSubString = "nothing to commit, working directory clean"
    return gitStatusOut.find(expSubString) >= 0

  def listBranches(self, pattern):
    result = self.checkOutputAsStr((self.gitCmd, "branch", "--list", pattern))
    return result

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
    result = self.checkOutputAsStr((self.gitCmd, "log", "--format=%B", "-n", "1", commitRef))
    return result

  def getCommitAuthorName(self, commitRef):
    result = self.checkOutputAsStr((self.gitCmd, "log", "--format=%aN", "-n", "1", commitRef))
    return result

  def getCommitAuthorEmail(self, commitRef):
    result = self.checkOutputAsStr((self.gitCmd, "log", "--format=%aE", "-n", "1", commitRef))
    return result

  def getLatestCommitForAuthor(self, svnAuthor):
    # print('Get git commit for author "%s, type=%s"' % (svnAuthor, str(type(svnAuthor))))
    authorCommit = self.checkOutputAsStr(
                     " ".join((self.gitCmd, "rev-list", "--all", "-i", ("--author=" + svnAuthor), # see git commit documentation on --author
                                "|",  # pipe should have a buffer for at most a few commit ids.
                                "head", "-1" # the first line
                              )),
                              shell=True) # use shell pipe
    authorCommit = authorCommit.rstrip("\n")
    return authorCommit

  gitSvnMarker = "git-svn-id:" # added and used by git svn dcommit
  svn2gitMarker = "svn2git-id:" # added and used here.

  def getSvnRemoteUuidRevisionFromCommitMessage(self, commitMessage, marker):
    words = commitMessage.split()
    if not marker in words:
      return (None, None, None)
    svnId = words[words.index(marker) + 1]
    splitSvnId = svnId.split("@")
    svnRemote = splitSvnId[0]
    svnRevision = int(splitSvnId[1])
    svnRepoUuid = words[words.index(marker) + 2]
    return (svnRemote, svnRepoUuid, svnRevision)

  def getSvnRemoteAndUuidAndRevision(self, gitSvnCommitRef):
    gitSvnCommitMessage = self.getCommitMessage(gitSvnCommitRef)
    return self.getSvnRemoteUuidRevisionFromCommitMessage(gitSvnCommitMessage, self.gitSvnMarker)

  def lastTempGitSvnRevision(self, tempBranchCommitRef): # at a commit generated here on the temp branch.
    gitCommitMessage = self.getCommitMessage(tempBranchCommitRef)
    (svnRemote, svnRepoUuid, svnRevision) = self.getSvnRemoteUuidRevisionFromCommitMessage(gitCommitMessage, self.svn2gitMarker)
    return svnRevision

  def addAllToIndex(self):
    self.checkCall((self.gitCmd, "add", "-A", self.getPathName()))

  def commit(self, message,
                  authorName, authorEmail, authorDate,
                  committerName, committerEmail, committerDate):
    author = ''.join((authorName, " <", authorEmail, ">"))
    os.environ["GIT_COMMITTER_NAME"] = committerName # no need to save/restore earlier environment state.
    os.environ["GIT_COMMITTER_EMAIL"] = committerEmail
    os.environ["GIT_COMMITTER_DATE"] = committerDate
    self.checkCall((self.gitCmd, "commit",
                                  "--allow-empty", # in case only svn poperties changed.
                                  ("--message=" + message),
                                  ("--author=" + author),
                                  ("--date=" + authorDate) ))

  def cleanDirsForced(self):
    self.checkCall((self.gitCmd, "clean", "-fd")) # Use -fdx to also remove ignored files.



def errorExit(*messageParts):
  raise RuntimeError(" ".join(map(str, messageParts)))


def allSuccessivePairs(lst):
  return [lst[i:i+2] for i in range(len(lst)-1)]

def octal(mode):
  return format(mode, 'o')

def checkEqualProtectionBits(fn1, fn2):
  stat1 = os.stat(fn1)
  stat2 = os.stat(fn2)
  if stat1.st_mode != stat2.st_mode:
    print("Protection bits %s of %s" % (octal(stat1.st_mode), fn1))
    print("Protection bits %s of %s" % (octal(stat2.st_mode), fn2))
    return False
  return True


def verifyGitFilesAgainstSvn(gitRepo, svnWorkingCopy):
  # The files under version control at the git repo can be enumerated quickly by: git ls-tree -r trunk.svn | cut  --fields=2-
  # This makes sense because all files, including binary files, are added.
  # svn ls -R  is too slow to use here (this lists about 12 file names per second, lucene-solr has well over 4000).
  fileNamesOut = gitRepo.checkOutputAsStr((gitRepo.gitCmd, "ls-tree", "-r", "--name-only", gitRepo.getCurrentBranch()))
  fileNames = nonEmptyLines(fileNamesOut)
  print("verifyGitFilesAgainstSvn checking", len(fileNames), "files")
  result = True
  for fileName in fileNames:
    #print("fileName", fileName)
    fileNameInGitRepo = os.path.join(gitRepo.getPathName(), fileName)
    #print("fileNameInGitRepo", fileNameInGitRepo)
    fileNameInSvnWorkingCopy = os.path.join(svnWorkingCopy.getPathName(), fileName)
    #print("fileNameInSvnWorkingCopy", fileNameInSvnWorkingCopy)
    try:
      diffOutput = subprocess.check_output(("diff", "-q", fileNameInGitRepo, fileNameInSvnWorkingCopy))
    except (subprocess.CalledProcessError, exitError):
      print("difference in file", fileName)
      print("diff exitError", exitError)
      result = False

    if not checkEqualProtectionBits(fileNameInSvnWorkingCopy, fileNameInGitRepo):
      result = False

  if result:
    print("no differences")
  else:
    print("some differences")

"""
On clean checkouts of both svn and git the command:
  diff -r svndir gitdir

reports only .svn .git and empty directories in the svn working copy, for example:

  Only in ./svnwork/lucene-solr/lucene/analysis/icu: lib

This diff output could be checked here.
To clean an svn working copy:

  rm -r * # also .hgignore .caches, all except .svn
  svn update # this is a local svn operation

To clean a git working directory:

  rm -r * # all except .git
  git checkout branchname -- .

"""


def deleteEmptyDirs(pathName, topDirName):
  """ Delete higher level directories of pathName when empty, but do not delete topDirName """
  head, tail = os.path.split(pathName)
  while (head != topDirName) and not os.listdir(head):
    assert head.startswith(topDirName) # , topDirName + " <<>> " + head
    # subprocess.check_call(("rm", "-r", head)) # delete empty directory
    os.rmdir(head)
    head, tail = os.path.split(head)


def setGitWorkingTreeViaSvnCheckout(svnWorkingCopy, revision, gitRepo):
  svnUpdateOutputLines = svnWorkingCopy.updateOutput(revision)
  """ Some example lines:
U    solr/solrj/src/test/org/apache/solr/client/solrj/io/sql/JdbcTest.java
 U   solr/core
Updated to revision 1707390.

From svn help update:

  For each updated item a line will be printed with characters reporting
  the action taken. These characters have the following meaning:

    A  Added
    D  Deleted
    U  Updated
    C  Conflict
    G  Merged
    E  Existed
    R  Replaced

  Characters in the first column report about the item itself.
  Characters in the second column report about properties of the item.
  A 'B' in the third column signifies that the lock for the file has
  been broken or stolen.
  A 'C' in the fourth column indicates a tree conflict, while a 'C' in
  the first and second columns indicate textual conflicts in files
  and in property values, respectively.

  """
  for svnUpdateLine in nonEmptyLines(svnUpdateOutputLines):

    if svnUpdateLine.startswith("Updating "): # first line
      continue

    if svnUpdateLine.startswith("Updated to"): # last line
      revisionStr = svnUpdateLine.split()[3][:-1]
      assert revision == int(revisionStr), revisionStr
      continue

    print(svnUpdateLine)
    itemChar = svnUpdateLine[0]
    itemPropChar = svnUpdateLine[1]
    lockChar = svnUpdateLine[2]
    treeConflictChar = svnUpdateLine[3]
    fileName = svnUpdateLine[5:]

    validItemChars = (" ", "A", "D", "U")
    assert itemChar in validItemChars,     "revision %d itemChar %s, fileName %s"                            % (revision, itemChar, fileName)
    assert itemPropChar in validItemChars, "revision %d itemPropChar %s, working copy not clean fileName %s" % (revision, itemPropChar, fileName)
    assert lockChar == " ",                "revision %d lockChar %s fileName %s"                             % (revision, lockChar, fileName)
    assert treeConflictChar == " ",        "revision %d treeConflictChar %s fileName %s"                     % (revision, treeConflictChar, fileName)

    fileNameInGitRepo = os.path.join(gitRepo.getPathName(), fileName)
    setFileProtectionBits = False
    if itemChar == "D": # deleted in svn working copy
      if os.path.isdir(fileNameInGitRepo):
        print("Deleting directory %s" % fileNameInGitRepo)
        # subprocess.check_call(("rm", "-r", fileNameInGitRepo)) # delete in git working tree
        shutil.rmtree(fileNameInGitRepo) # delete completely in git working tree
        deleteEmptyDirs(fileNameInGitRepo, gitRepo.getPathName()) # delete empty dirs in git repo
      elif os.path.isfile(fileNameInGitRepo):
        print("Deleting file %s" % fileNameInGitRepo)
        # subprocess.check_call(("rm", fileNameInGitRepo)) # delete in git working tree
        os.remove(fileNameInGitRepo)
        deleteEmptyDirs(fileNameInGitRepo, gitRepo.getPathName())
      else:
        print("Non deleting non existing file %s" % fileName)
    elif itemChar in ("A", "U"): # added or updated in svn working copy
      fileNameInSvnWorkingCopy = os.path.join(svnWorkingCopy.getPathName(), fileName)
      if os.path.isdir(fileNameInSvnWorkingCopy):
        if not os.path.isdir(fileNameInGitRepo):
          print("Creating directory %s" % fileName)
          #subprocess.check_call(("mkdir", fileNameInGitRepo)) # new directory in git working tree
          os.mkdir(fileNameInGitRepo)
        else:
          print("Not creating existing directory %s" % fileName)
      elif os.path.isfile(fileNameInSvnWorkingCopy):
        head, tail = os.path.split(fileNameInGitRepo)
        if not os.path.isdir(head):
          print("Creating directory for file %s" % fileNameInGitRepo)
          os.mkdir(head)
        # print("Copying file %s" % fileName # Common case)
        # subprocess.check_call(("cp", fileNameInSvnWorkingCopy, fileNameInGitRepo)) # copy into git working tree
        shutil.copyfile(fileNameInSvnWorkingCopy, fileNameInGitRepo)
        setFileProtectionBits = True
      else:
        assert False, "Cannot add or update non existing file %s" % fileNameInSvnWorkingCopy
    else:
      assert itemChar == " " # nothing to do

    if itemPropChar != " ":
      print("At revision %d ignoring svn property change type %s for file %s" % (revision, itemPropChar, fileName))
      setFileProtectionBits = True # svn:executable may have been set or unset.

    if setFileProtectionBits:
      statSvn = os.stat(fileNameInSvnWorkingCopy)
      statGit = os.stat(fileNameInGitRepo)
      if statSvn.st_mode != statGit.st_mode:
        print("Changing mode from %s to %s for %s" % (octal(statGit.st_mode), octal(statSvn.st_mode), fileNameInGitRepo))
        os.chmod(fileNameInGitRepo, statSvn.st_mode)


def assertUrlsSameExceptScheme(url1, url2): # may only differ by scheme http:// or https://
  scheme1, netloc1, path1, params1, query1, fragment1 = urlparse(url1)
  scheme2, netloc2, path2, params2, query2, fragment2 = urlparse(url2)
  #print(scheme1, netloc1, path1, params1, query1, fragment1)
  #print(scheme2, netloc2, path2, params2, query2, fragment2)
  assert netloc1 == netloc2
  assert path1 == path2
  assert params1 == params2
  assert query1 == query2
  assert fragment1 == fragment2


def maintainTempGitSvnBranch(branchName, tempGitBranchName,
                              svnWorkingCopyOfBranchPath, svnRepoBranchName,
                              gitRepoPath, gitUpstream,
                              maxCommits=20, # generate at most this number of commits on tempGitBranchName, rerun to add more.
                              testMode=False):

  assert maxCommits >= 1

  gitRepo = GitRepository(gitRepoPath)
  gitRepo.checkOutBranch(branchName) # fails with git message when working directory is not clean

  svnWorkingCopy = SvnWorkingCopy(svnWorkingCopyOfBranchPath)
  svnWorkingCopy.ensureNoLocalModifications()
  svnWorkingCopy.switch(svnRepoBranchName) # switch to repo branch, update to latest revision

  lastSvnRevision = svnWorkingCopy.lastChangedRevision() # int to allow comparison
  #print(svnWorkingCopy, "lastSvnRevision:", lastSvnRevision)

  gitRepo.fetch(gitUpstream)
  if testMode:
    pass # leave branch where it is, as if the last commits from upstream did not arrive
  else:
    gitRepo.merge(branchName, gitUpstream + "/" + branchName)

  (gitSvnRemote, gitSvnRepoUuid, lastSvnRevisionOnGitSvnBranch) = gitRepo.getSvnRemoteAndUuidAndRevision(branchName)
  svnUrl = svnWorkingCopy.getUrl()
  svnRepoUuid = svnWorkingCopy.getUuid()
  print("gitSvnRemote:", gitSvnRemote)
  print("svnUrl:", svnUrl)
  print("svn repo uuid:", svnRepoUuid)
  assertUrlsSameExceptScheme(gitSvnRemote, svnUrl)
  assert gitSvnRepoUuid == svnRepoUuid

  # check whether tempGitBranchName exists:
  diffBaseRevision = lastSvnRevisionOnGitSvnBranch
  svnTempRevision = None
  doCommitOnExistingTempBranch = False

  if gitRepo.branchExists(tempGitBranchName):
    print(tempGitBranchName, "exists")
    # update lastSvnRevisionOnGitSvnBranch from there.
    svnTempRevision = gitRepo.lastTempGitSvnRevision(tempGitBranchName)
    if svnTempRevision is None:
      print("Warning: no svn revision found on branch:", tempGitBranchName)
    else:
      if svnTempRevision > lastSvnRevisionOnGitSvnBranch:
        diffBaseRevision = svnTempRevision
        doCommitOnExistingTempBranch = True
        gitRepo.checkOutBranch(tempGitBranchName)

  if lastSvnRevision == diffBaseRevision:
    print(gitRepo, gitRepo.getCurrentBranch(), "up to date with", svnWorkingCopy, svnRepoBranchName)
    verifyGitFilesAgainstSvn(gitRepo, svnWorkingCopy)
    return

  if lastSvnRevision < diffBaseRevision: # unlikely, do nothing
    print(gitRepo, gitRepo.getCurrentBranch(), "later than", svnWorkingCopy, ", nothing to update.")
    return

  print(gitRepo, gitRepo.getCurrentBranch(), "earlier than", svnWorkingCopy)

  if not gitRepo.workingDirectoryClean():
    errorExit(gitRepo, "on branch", gitRepo.getCurrentBranch(), "not clean")

  print(gitRepo,"on branch", gitRepo.getCurrentBranch(), "and clean")

  if not doCommitOnExistingTempBranch: # restart temp branch from branch
    assert gitRepo.getCurrentBranch() == branchName
    if gitRepo.branchExists(tempGitBranchName): # tempGitBranchName exists, delete it first.
      print("Branch", tempGitBranchName, "exists, deleting")
      gitRepo.deleteBranch(tempGitBranchName)
      if gitRepo.branchExists(tempGitBranchName):
        errorExit("Could not delete branch", tempGitBranchName, "from", gitRepo)

    gitRepo.createBranch(tempGitBranchName)
    gitRepo.checkOutBranch(tempGitBranchName)
    print("Started branch", tempGitBranchName, "at", branchName)

  assert gitRepo.getCurrentBranch() == tempGitBranchName

  maxNumLogEntries = maxCommits + 1
  svnLogEntries = svnWorkingCopy.getLogEntries(diffBaseRevision, lastSvnRevision, maxNumLogEntries)

  numCommits = 0

  startRevision = svnLogEntries[0].revision
  ignore = svnWorkingCopy.updateOutput(startRevision)

  for (logEntryFrom, logEntryTo) in allSuccessivePairs(svnLogEntries):
    setGitWorkingTreeViaSvnCheckout(svnWorkingCopy, logEntryTo.revision, gitRepo)

    gitRepo.addAllToIndex() # add all changes from the git working tree to the git index.

    # commit, put toRevision at end so it can be picked up later.

    commitMessageMetaData = gitRepo.svn2gitMarker + " " + gitSvnRemote + "@" + str(logEntryTo.revision) + " " + gitSvnRepoUuid
    # git-svn adds this commit metadata:
    # git-svn-id: https://svn.apache.org/repos/asf/lucene/dev/trunk@1719562 13f79535-47bb-0310-9956-ffa450edef68
    # This script uses svn2git-id: instead of git-svn-id:

    message = logEntryTo.msg + "\n\n" + commitMessageMetaData

    authorCommit = gitRepo.getLatestCommitForAuthor(logEntryTo.author)
    authorName = gitRepo.getCommitAuthorName(authorCommit)
    authorEmail = gitRepo.getCommitAuthorEmail(authorCommit)
    # print("Author name and email:", authorName, authorEmail)
    gitRepo.commit(message,
                    authorName, authorEmail, logEntryTo.date,
                    authorName, authorEmail, logEntryTo.date) # author is also git committer, just like git-svn

    numCommits += 1

    #print("Commit  author:", logEntryTo.author)
    print("Commit date:", logEntryTo.date)
    #print("Commit message:", logEntryTo.msg)

    gitRepo.cleanDirsForced() # delete untracked directories and files

    if not gitRepo.workingDirectoryClean():
      errorExit(gitRepo, "on branch", gitRepo.getCurrentBranch(), "not clean, numCommits:", numCommits)

    diffBaseRevision = logEntryTo.revision
    print('') # show empty line after commit info

  print("Added", numCommits, "commit(s) to branch", tempGitBranchName)

  if lastSvnRevision == diffBaseRevision:
    print(gitRepo, gitRepo.getCurrentBranch(), "up to date with", svnWorkingCopy, svnRepoBranchName)
    verifyGitFilesAgainstSvn(gitRepo, svnWorkingCopy)
    return


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
        errorExit("Argument(s) [test] [maximum number of commits], defaults are false and " + defaultMaxCommits)
    argv = argv[1:]

  repo = "lucene-solr"
  branchName = "trunk"
  tempGitBranchName = branchName + ".svn"

  home = os.path.expanduser("~")

  svnWorkingCopyOfBranchPath = os.path.join(home, "svnwork", repo)
  svnRepoBranchName = "lucene/dev/" + branchName # for svn switch to

  gitRepoPath = os.path.join(home, "gitrepos", repo)
  gitUpstream = "upstream"

  maintainTempGitSvnBranch(branchName, tempGitBranchName,
                            svnWorkingCopyOfBranchPath, svnRepoBranchName,
                            gitRepoPath, gitUpstream,
                            maxCommits=maxCommits,
                            testMode=testMode)

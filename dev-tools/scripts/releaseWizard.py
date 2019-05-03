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

# This script is a wizard that replaces the todoList at https://wiki.apache.org/lucene-java/ReleaseTodo
# It will walk you through the steps of the release process, asking for decisions or input along the way
#
# Requirements:
#   python 3
#   pip3 install pickledb
#   pip3 install console-menu

import os
import sys
from enum import Enum
import scriptutil
from scriptutil import BranchType, Version, check_ant, getGitRev, run
import re
import pickledb
from consolemenu import ConsoleMenu, SelectionMenu
from consolemenu.screen import Screen
from consolemenu.items import FunctionItem, SubmenuItem, CommandItem, SelectionItem
from consolemenu.menu_component import Dimension

global release_version
global release_type

# Solr:Java version mapping
java_versions = { 6: 8, 7: 8, 8: 8, 9: 11 }

class ReleaseType(Enum):
    major  = 1
    minor  = 2
    bugfix = 3

major_minor = [ReleaseType.major, ReleaseType.minor]



prologue = {
    'major':
        """You are about to release a major version of Lucene/Solr.""",
    'minor':
        """You are about to release a minor version of Lucene/Solr.""",
    'bugfix':
        """You are about to release a bugfix version of Lucene/Solr.""",
}


class MyScreen(Screen):
    def clear(self):
        return

    screen_width = 200


class ReleaseBase:
    def __init__(self, branch, version):
        self.version = version
        self.branch = branch

    def is_valid(self):
        return True


#class MajorRelease(ReleaseBase):


def read_version():
    print ("Got version %s" % getScriptVersion())
    return getScriptVersion()


def getScriptVersion():
    topLevelDir = '../..'  # Assumption: this script is in dev-tools/scripts/ of a checkout
    m = re.compile(r'(.*)/').match(sys.argv[0])  # Get this script's directory
    if m is not None and m.group(1) != '.':
        origCwd = os.getcwd()
        os.chdir(m.group(1))
        os.chdir('../..')
        topLevelDir = os.getcwd()
        os.chdir(origCwd)
    reBaseVersion = re.compile(r'version\.base\s*=\s*(\d+\.\d+\.\d+)')
    return reBaseVersion.search(open('%s/lucene/version.properties' % topLevelDir).read()).group(1)


def get_version():
    version = db.get('version')


def check_prerequisites():
    print("Checking prerequisites...")
    if sys.version_info < (3, 4):
        sys.exit("Script requires Python v3.4 or later")
    try:
        run("gpg --version")
    except:
        sys.exit("You will need gpg installed")
    check_ant()
    if not 'JAVA8_HOME' in os.environ or not 'JAVA11_HOME' in os.environ:
        sys.exit("Please set environment variables JAVA8_HOME and JAVA11_HOME")
    #getGitRev()


class TodoGroup:
    def __init__(self, id, title, checklist, pre_fun=None):
        self.pre_fun = pre_fun
        self.title = title
        self.id = id
        self.checklist = checklist
        if not checklist:
            checklist = []

    def num_done(self):
        return sum(1 for x in self.checklist if x.done > 0)

    def num_applies(self):
        return sum(1 for x in self.checklist if x.applies(release_type))

    def is_done(self):
        return self.num_done() >= self.num_applies()

    def get_title(self):
        prefix = ""
        if self.is_done():
            prefix = "✓ "
        return "%s%s (%d/%d)" % (prefix, self.title, self.num_done(), self.num_applies())

    def get_submenu(self):
        menu = ConsoleMenu(title=self.title, screen=MyScreen())
        for todo in self.checklist:
            if todo.applies(release_type):
                menu.append_item(todo.get_menu_item())
            else:
                print("Todo %s does not apply" % todo.id)
        return menu

    def get_menu_item(self):
        item = SubmenuItem(self.get_title, self.get_submenu())
        return item

    def get_todos(self):
        return self.checklist


class Todo:
    def __init__(self, id, title, description='N/A', done=False, type=None, fun=None):
        self.fun = fun
        self.types = type
        if not self.types:
            self.types = [ReleaseType.bugfix, ReleaseType.minor, ReleaseType.major]
        if not isinstance(self.types, list):
            self.types = [self.types]
        self.done = done
        self.description = description
        self.title = title
        self.id = id

    def is_done(self):
        return self.done == True

    def applies(self, type):
        return type in self.types

    def get_title(self):
        done = ""
        prefix = ""
        if self.done:
            done = "(Done)"
            prefix = "✓ "
        return "%s%s %s" % (prefix, self.title, done)

    def display_and_confirm(self):
        print(self.description)
        if self.fun and not self.done:
            print("Running function")
            self.fun()
        menu = ConsoleMenu(title=self.title,
                           subtitle="Is this task completed?",
                           screen=MyScreen(),
                           show_exit_option=False)
        for index, item in enumerate(['Sure thing', "Not yet"]):
            menu.append_item(SelectionItem(item, index, menu))
        menu.show()
        menu.join()
        yes_no = menu.selected_option
        self.done = yes_no == 0
        write_db()

    def get_menu_item(self):
        return FunctionItem(self.get_title, self.display_and_confirm)


def get_gpg_key():
    print("Obtaining gpg key...")

def ant_precommit():
    print("Running ant precommit...")


todo_groups = [
    TodoGroup('prerequisites',
              'Prerequisites',
              [
                  Todo('tools',
                       'Necessary tools are found',
                       'Tools like java, ant, git etc are found and correct version',
                       done=True),
                  Todo('gpg', 'GPG key id is configured', fun=get_gpg_key)
              ]),
    TodoGroup('preparation',
              'Work with the community to decide when and how etc',
              [
                  Todo('decide_jira_issues',
                       'Select JIRA issues to be included',
                       'Set the appropriate "Fix Version" in JIRA for these issues'),
                  Todo('decide_branch_date',
                       'Decide the date for branching',
                       type=major_minor),
                  Todo('decide_freeze_length',
                       'Decide the lenght of feature freeze',
                       type=major_minor)
              ]),
    TodoGroup('branching',
              'Creating a branch for this release',
              [
                  Todo('ant_precommit',
                       'Run ant precommit to run a bunch of sanity & quality checks',
                       'Fix any problems that are found.',
                       fun=ant_precommit)
              ]),
    TodoGroup('jira',
              'Add new versions to JIRA',
              [
                  Todo('id', 'title')
              ]),
    TodoGroup('releasenotes',
              'Create release notes',
              [
                  Todo('id', 'title')
              ]),
    TodoGroup('tests',
              'Make sure tests pass',
              [
                  Todo('id', 'title')
              ]),
    TodoGroup('artifacts',
              'Build the release artifacts',
              [
                  Todo('id', 'title')
              ]),
    TodoGroup('vote',
              'Hold the vote',
              [
                  Todo('id', 'title')
              ]),
    TodoGroup('publish',
              'Publish the release artifacts',
              [
                  Todo('id', 'title')
              ]),
    TodoGroup('website',
              'Update the website',
              [
                  Todo('id', 'title')
              ]),
    TodoGroup('doap',
              'Update the DOAP file',
              [
                  Todo('id', 'title')
              ]),
    TodoGroup('announce',
              'Announce the release',
              [
                  Todo('id', 'title')
              ]),
    TodoGroup('post_release',
              'Tasks to do after release',
              [
                  Todo('id', 'title')
              ])
    ]


def make_main_menu(title, subtitle, prologue, epilogue, todo_groups):
    menu = ConsoleMenu(title=title, subtitle=subtitle, prologue_text=prologue, epilogue_text=epilogue, screen=MyScreen())

    for todo_group in todo_groups:
        menu.append_item(todo_group.get_menu_item())
    return menu


def read_db():
    global release_version
    print("Reading DB")
    keys = db.getall()
    if 'release_version' in keys:
        release_version = db.get('release_version')
    for todo_group in todo_groups:
        if todo_group.id in keys:
            dict = db.dgetall(todo_group.id)
            for todo in todo_group.checklist:
                if todo.id in dict:
                    todo.done = dict[todo.id] is True
                    # print(" - initialized %s/%s=%s from DB" % (todo_group.id, todo.id, todo.done))


def write_db():
    # print("Writing to DB")
    if (release_version):
        db.set('release_version', release_version)
    keys = db.getall()
    for todo_group in todo_groups:
        # print("handling %s" % todo_group.id)
        if todo_group.id in keys:
            db.rem(todo_group.id)
        db.dcreate(todo_group.id)
        for todo in todo_group.checklist:
            # print("handling %s/%s" % (todo_group.id, todo.id))
            db.dadd(todo_group.id, (todo.id, todo.done))
    db.set('branch', branch)
    db.dump()


def get_release_version():
    v = input("Which version are you releasing? ")
    version = Version.parse(v)
    return str(version)


def get_subtitle():
    done_groups = sum(1 for x in todo_groups if x.is_done())
    return "Please complete the below checklist (Complete: %s/%s)" % (done_groups, len(todo_groups))


def validate_release_version(branch_type, branch, release_version):
    ver = Version.parse(release_version)
    print("release_version=%s, ver=%s" % (release_version, ver))
    if branch_type == BranchType.release:
        if not branch.startswith('branch_'):
            sys.exit("Incompatible branch and branch_type")
        if not ver.is_bugfix_release():
            sys.exit("You can only release bugfix releases from an existing release branch")
    elif branch_type == BranchType.stable:
        if not branch.startswith('branch_') and branch.endswith('x'):
            sys.exit("Incompatible branch and branch_type")
        if not ver.is_minor_release():
            sys.exit("You can only release minor releases from an existing stable branch")
    elif branch_type == BranchType.unstable:
        if not branch == 'master':
            sys.exit("Incompatible branch and branch_type")
        if not ver.is_major_release():
            sys.exit("You can only release a new major version from master branch")
    if not getScriptVersion() == release_version:
        sys.exit("Expected version %s, but got %s", (getScriptVersion(), release_version))

def main():
    global title
    global release_version
    global release_type

    check_prerequisites()
    read_db()
    if not release_version:
        release_version = get_release_version()

    validate_release_version(branch_type, branch, release_version)

    write_db()

    release_type = ReleaseType.bugfix
    print ("Rendering main menu")
    main_menu = make_main_menu("Releasing Lucene/Solr %s" % release_version,
                               get_subtitle,
                               None,
                               None,
                               todo_groups)
    main_menu.show()



    # if BranchType.unstable == branch_type:
    #     print ("Branch master")
    # elif BranchType.stable == branch_type:
    #     print ("Branch master")
    # elif BranchType.release == branch_type:
    #     print ("Branch master")


    # release_version = getScriptVersion()
    # if release_version.endswith(".0.0"):
    #     release_type = 'major'
    # elif release_version.endswith(".0"):
    #     release_type = 'minor'
    # else:
    #     release_type = 'bugfix'

    # if not version.startswith(scriptVersion + '.'):
    #   raise RuntimeError('releaseWizard.py for %s.X is incompatible with a %s release.' % (scriptVersion, c.version))

    # mainMenu()


sys.path.append(os.path.dirname(__file__))

print ("Lucene/Solr releaseWizard v%s\n" % getScriptVersion())
# Find some generic environment stats
branch_type = scriptutil.find_branch_type()
branch = run("git rev-parse --abbrev-ref HEAD").strip()
db = pickledb.load("releaseWizard.json", False)
release_version = None

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('Keyboard interrupt...exiting')

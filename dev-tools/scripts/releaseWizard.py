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

# This script is a wizard that replaces the todoList at https://wiki.apache.org/lucene-java/ReleaseTodo
# It will walk you through the steps of the release process, asking for decisions or input along the way
#
# Requirements:
#   python 3
#   pip3 install console-menu

import os
import sys
import json
import copy
import subprocess
import shutil
from collections import OrderedDict
from enum import Enum
import scriptutil
from scriptutil import BranchType, Version, check_ant, getGitRev, run
import re
import datetime
from consolemenu import ConsoleMenu
from consolemenu.screen import Screen
from consolemenu.items import FunctionItem, SubmenuItem

global state
global root_folder

# Solr:Java version mapping
java_versions = { 6: 8, 7: 8, 8: 8, 9: 11 }

class ReleaseType(Enum):
    major  = 1
    minor  = 2
    bugfix = 3

major_minor = [ReleaseType.major, ReleaseType.minor]


class MyScreen(Screen):
    def clear(self):
        return

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
    # try:
    #     getGitRev()
    # except Exception as e:
    #     sys.exit(e.__str__())

epoch = datetime.datetime.utcfromtimestamp(0)

def unix_time_millis(dt):
    return int((dt - epoch).total_seconds() * 1000.0)

class ReleaseState:
    def __init__(self, todo_groups, config_path, script_version):
        self.script_version = script_version
        self.config_path = config_path
        self.todo_groups = todo_groups
        self.previous_rcs = OrderedDict()
        self.todos = {}
        for g in self.todo_groups:
            for t in g.get_todos():
                self.todos[t.id] = t
        self.release_version = None
        self.release_type = None
        self.rc_number = 1
        self.branch = run("git rev-parse --abbrev-ref HEAD").strip()
        try:
            self.branch_type = scriptutil.find_branch_type()
        except:
            print("WARNING: A release cannot happen from a feature branch (%s)" % self.branch)
            self.branch_type = 'feature'


    def set_release_version(self, version):
        self.release_version = version
        v = Version.parse(version)
        if v.is_major_release():
            self.release_type = ReleaseType.major
        elif v.is_minor_release():
            self.release_type = ReleaseType.minor
        else:
            self.release_type = ReleaseType.bugfix

    def new_rc(self):
        rc_id = str(self.rc_number)
        dict = OrderedDict()
        for g in list(filter(lambda x: x.in_rc_loop(), self.todo_groups)):
            for t in g.get_todos():
                if t.applies(self.release_type):
                    dict[t.id] = copy.deepcopy(t.state)
                    t.clear()
        self.previous_rcs[rc_id] = dict
        self.rc_number += 1
        self.save()

    def to_dict(self):
        tmp_todos = {}
        for todo_id in self.todos:
            t = self.todos[todo_id]
            tmp_todos[todo_id] = copy.deepcopy(t.state)
        return OrderedDict({
            'script_version': self.script_version,
            'release_version': self.release_version,
            'rc_number': self.rc_number,
            'script_branch': self.branch,
            'todos': tmp_todos,
            'previous_rcs': self.previous_rcs
        })

    def restore_from_dict(self, dict):
        self.script_version = dict['script_version']
        self.set_release_version(dict['release_version'])
        self.rc_number = dict['rc_number']
        self.branch = dict['script_branch']
        self.previous_rcs = copy.deepcopy(dict['previous_rcs'])
        for todo_id in dict['todos']:
            t = self.todos[todo_id]
            for k in dict['todos'][todo_id]:
                t.state[k] = dict['todos'][todo_id][k]

    def load(self):
        print("Loading")
        latest = None

        if not os.path.exists(self.config_path):
            print("Creating folder %s" % self.config_path)
            os.makedirs(self.config_path)
        else:
            if os.path.exists(os.path.join(self.config_path, 'latest.json')):
                with open(os.path.join(self.config_path, 'latest.json'), 'r') as fp:
                    latest = json.load(fp)['version']
                    print("Found version %s in %s" % (latest, os.path.join(self.config_path, 'latest.json')))

            if latest and os.path.exists(os.path.join(self.config_path, latest, 'state.json')):
                with open(os.path.join(self.config_path, latest, 'state.json'), 'r') as fp:
                    try:
                        dict = json.load(fp)
                        self.restore_from_dict(dict)
                    except:
                        print("Failed to load state from %s" % os.path.join(self.config_path, latest, 'state.json'))
        print("Loaded")

    def save(self):
        print("Saving")
        # Storing working version in latest.json
        with open(os.path.join(self.config_path, 'latest.json'), 'w') as fp:
            json.dump({'version':self.release_version}, fp)

        if not os.path.exists(os.path.join(self.config_path, self.release_version)):
            print("Creating folder %s" % os.path.join(self.config_path, self.release_version))
            os.makedirs(os.path.join(self.config_path, self.release_version))

        with open(os.path.join(self.config_path, self.release_version, 'state.json'), 'w') as fp:
            json.dump(self.to_dict(), fp, sort_keys=False, indent=4)

    def clear(self):
        self.previous_rcs = OrderedDict()
        self.rc_number = 1
        for t_id in self.todos:
            t = self.todos[t_id]
            t.state = {}
            t.set_done(t.done_initial_value)
        self.save()

    def get_rc_number(self):
        return self.rc_number

    def get_group_by_id(self, id):
        return list(filter(lambda x: x.id == id, self.todo_groups))[0]

    def get_todo_by_id(self, id):
        return list(filter(lambda x: x.id == id, self.todos.values()))[0]

    def get_release_folder(self):
        folder = os.path.join(self.config_path, self.release_version, "RC%d" % self.rc_number)
        if not os.path.exists(folder):
            print("Creating folder %s" % folder)
            os.makedirs(folder)
        return os.path.join(folder, 'build')

    def get_minor_branch_name(self):
        v = Version.parse(self.release_version)
        return "branch_%s_%s" % (v.major, v.minor)

    def get_stable_branch_name(self):
        v = Version.parse(self.release_version)
        return "branch_%sx" % v.major

class TodoGroup:
    def __init__(self, id, title, description, checklist, pre_fun=None, in_rc_loop=False, depends=None):
        self.depends = depends
        self.description = description
        self.is_in_rc_loop = in_rc_loop
        self.pre_fun = pre_fun
        self.title = title
        self.id = id
        self.checklist = checklist
        if not checklist:
            self.checklist = []

    def num_done(self):
        return sum(1 for x in self.checklist if x.is_done() > 0)

    def num_applies(self):
        count = sum(1 for x in self.checklist if x.applies(state.release_type))
        # print("num_applies=%s" % count)
        return count

    def is_done(self):
        # print("Done=%s, applies=%s" % (self.num_done(), self.num_applies()))
        return self.num_done() >= self.num_applies()

    def get_title(self):
        # print("get_title: %s" % self.is_done())
        prefix = ""
        if self.is_done():
            prefix = "✓ "
        return "%s%s (%d/%d)" % (prefix, self.title, self.num_done(), self.num_applies())

    def get_submenu(self):
        menu = ConsoleMenu(title=self.title, subtitle=self.get_subtitle, prologue_text=self.description, screen=MyScreen())
        for todo in self.get_todos():
            if todo.applies(state.release_type):
                menu.append_item(todo.get_menu_item())
        return menu

    def get_menu_item(self):
        item = SubmenuItem(self.get_title, self.get_submenu())
        return item

    def get_todos(self):
        return self.checklist

    def in_rc_loop(self):
        return self.is_in_rc_loop is True

    def get_subtitle(self):
        if self.depends:
            g = state.get_group_by_id(self.depends)
            if not g.is_done():
                return "NOTE: You must first complete tasks in %s" % g.title
        return None


class Todo:
    def __init__(self, id, title, description='N/A', done=False, type=None, fun=None, fun_args=None, links=None):
        self.links = links
        self.done_initial_value = done
        self.fun = fun
        self.fun_args = fun_args
        self.types = type
        if not self.types:
            self.types = [ReleaseType.bugfix, ReleaseType.minor, ReleaseType.major]
        if not isinstance(self.types, list):
            self.types = [self.types]
        self.description = description
        self.title = title
        self.id = id
        self.state = {}
        self.set_done(done)

    def set_done(self, is_done):
        self.state['done'] = is_done
        if is_done:
            self.state['done_date'] = unix_time_millis(datetime.datetime.now())
        else:
            self.state.pop('done_date', None)

    def applies(self, type):
        # print("applies type=%s, types=%s" % (type, self.types))
        return type in self.types

    def is_done(self):
        return self.state['done'] is True

    def get_title(self):
        # print("Building title for %s: done=%s" % (self.id, self.is_done()))
        done = ""
        prefix = ""
        if self.is_done():
            prefix = "✓ "
        return "%s%s" % (prefix, self.title)

    def display_and_confirm(self):
        print("\n%s" % self.description)
        if self.links:
            print("\nLinks:\n")
            for link in self.links:
                print("- %s" % link)
            print()
        if self.fun and not self.is_done():
            self.fun(self)
        completed = ask_yes_no("Mark task '%s' as completed?" % self.title)
        self.set_done(completed)
        state.save()

    def get_menu_item(self):
        return FunctionItem(self.get_title, self.display_and_confirm)

    def clone(self):
        clone = Todo(self.id, self.title, description=self.description)
        clone.state = copy.deepcopy(self.state)
        return clone

    def clear(self):
        self.state = {}
        self.set_done(self.done_initial_value)

    def get_state(self):
        return self.state

def get_gpg_key(todo):
    key_id = str(input("Please enter your gpg key ID, e.g. 0D8D0B93: "))
    todo.state['key_id'] = key_id

def ant_precommit(todo):
    print("Running ant precommit...")

def get_release_version():
    v = str(input("Which version are you releasing? (x.y.z) "))
    try:
        version = Version.parse(v)
    except:
        print("Not a valid version %s" % v)
        return get_release_version()

    return str(version)


def get_subtitle():
    done_groups = sum(1 for x in todo_templates if x.is_done())
    return "Please complete the below checklist (Complete: %s/%s)" % (done_groups, len(todo_templates))


def validate_release_version(branch_type, branch, release_version):
    ver = Version.parse(release_version)
    # print("release_version=%s, ver=%s" % (release_version, ver))
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
        print("Expected version %s when on branch %s, but got %s" % (getScriptVersion(), branch, release_version))


def get_todo_menuitem_title():
    return "Go to checklist (RC%d)" % (state.rc_number)


def get_releasing_text():
    return "Releasing Lucene/Solr %s RC%d" % (state.release_version, state.rc_number)


def get_start_new_rc_menu_title():
    return "Abort RC%d and start a new RC%d" % (state.rc_number, state.rc_number+1)


def start_new_rc():
    state.new_rc()
    print("Started RC%d" % state.rc_number)


def reset_state():
    global state
    shutil.rmtree(os.path.join(state.config_path, state.release_version))
    state.clear()

def main():
    global state

    print ("Lucene/Solr releaseWizard v%s\n" % getScriptVersion())

    check_prerequisites()
    state = ReleaseState(todo_templates, os.path.expanduser("~/.lucene-releases/"), getScriptVersion())
    state.load()

    if not state.release_version:
        input_version = get_release_version()
        validate_release_version(state.branch_type, state.branch, input_version)
        state.set_release_version(input_version)
        state.save()

    main_menu = ConsoleMenu(title="Lucene/Solr ReleaseWizard (script-ver=v%s)" % getScriptVersion(),
                            subtitle=get_releasing_text,
                            prologue_text="Welcome to the release wizard. From here you can manage the process including creating new RCs. "
                                          "All changes are persisted, so you can exit any time and continue later",
                            epilogue_text="® 2019 The Lucene/Solr project. Licensed under the Apache License 2.0",
                            screen=MyScreen())

    todo_menu = ConsoleMenu(title=get_releasing_text,
                            subtitle=get_subtitle,
                            prologue_text=None,
                            epilogue_text=None,
                            screen=MyScreen())

    for todo_group in state.todo_groups:
        todo_menu.append_item(todo_group.get_menu_item())

    main_menu.append_item(SubmenuItem(get_todo_menuitem_title, todo_menu))
    main_menu.append_item(FunctionItem(get_start_new_rc_menu_title, start_new_rc))
    main_menu.append_item(FunctionItem('Clear state, delete release-folder and restart from RC1', reset_state))

    main_menu.show()


sys.path.append(os.path.dirname(__file__))
root_folder = os.path.abspath(os.path.join(os.path.abspath(os.path.dirname(__file__)), os.path.pardir, os.path.pardir))

def run_tests(todo):
    print("Running tests")


def ask_yes_no(text):
    answer = None
    while answer not in ['y', 'n']:
        answer = str(input("\nQ: %s (y/n): " % text))
    print("\n")
    return answer == 'y'


def clear_ivy_cache(todo):
    print("You can clear or move the ivy cache located in ~/ivy2/cache yourself, or let this script do it.")
    print("This script will rename the ~/ivy2/cache folder as ~/ivy2/cache.bak and later let you restore it again.")
    if ask_yes_no("Shall I clear (backup) the cache for you now?"):
        ivy_path = os.path.expanduser("~/ivy2/")
        print("Going to clear")
    else:
        print("Not clearing")

def run_follow(command):
    process = subprocess.Popen(command, stdout=subprocess.PIPE)
    while True:
        output = process.stdout.readline()
        if output == '' and process.poll() is not None:
            break
        if output:
            print(output.strip().decode('utf-8'))
    rc = process.poll()
    return rc

def build_rc(todo):
    logfile = os.path.join(state.get_release_folder, 'buildAndPushRelease.log')
    cmdline = "python3 -u %s --root %s --push-local %s --rc-num %s --sign %s --logfile %s" \
              % (os.path.join(root_folder, 'dev-tools', 'scripts', 'buildAndPushRelease.py'),
                 root_folder,
                 state.get_release_folder(),
                 state.rc_number,
                 state.get_todo_by_id('gpg').get_state()['key_id'],
                 logfile)
    print("The command to build the RC is:\n\n%s\n\n" % cmdline)
    print("You most likely want to copy/paste the command into another Terminal and run it interactively")
    print("Or you can let me run it for you (NOTE: this will take looong time)")
    if ask_yes_no("Do you want me to run this command now?"):
        print("To follow the build/test log, run this command in another Terminal:\n\n%  tail -f %s\n" % logfile)
        run_follow(cmdline.split(" "))

def create_stable_branch(todo):
    assert state.branch == 'master'
    cmds = []
    cmds.append("cd %s" % root_folder)
    cmds.append("git checkout master")
    cmds.append("git checkout -b %s" % state.get_stable_branch_name())
    cmds.append("git push origin %s" % state.get_stable_branch_name())

    print("You can run these commands to create and push the branch:\n\n")
    for cmd in cmds:
        print("  %s" % cmd)

    if ask_yes_no("Do you want me to run these commands now?"):
        for cmd in cmds:
            run(cmd)

def create_minor_branch(todo):
    cmds = []
    cmds.append("cd %s" % root_folder)
    cmds.append("git checkout %s" % state.branch)
    cmds.append("git checkout -b %s" % state.get_minor_branch_name())
    cmds.append("git push origin %s" % state.get_minor_branch_name())

    print("You can run these commands to create and push the branch:\n\n")
    for cmd in cmds:
        print("  %s" % cmd)

    if ask_yes_no("Do you want me to run these commands now?"):
        for cmd in cmds:
            run(cmd)


def add_version_bugfix(todo):
    cmds = []
    cmds.append("git checkout %s" % state.get_minor_branch_name())
    cmds.append("python3 -u %s %s" % (os.path.join(root_folder, 'dev-tools/scripts/addVersion.py'), state.release_version))

    print("Do the following on the release branch only:")

    for cmd in cmds:
        print("  %s" % cmd)

    if ask_yes_no("Do you want me to run these commands now?"):
        for cmd in cmds:
            run(cmd)

def add_version_minor(todo):
    print("TODO")

def add_version_major(todo):
    print("TODO")


def inform_devs_release_branch(todo):
    v = Version.parse(state.release_version)
    next_version = "%d.%d" % (v.major, v.minor+1)
    mail_body = """NOTICE;\n\n%s has been cut and versions updated to %s on stable branch.\n\n""" % (state.get_minor_branch_name(), next_version)
    mail_body += """Please observe the normal rules:
    
* No new features may be committed to the branch.
* Documentation patches, build patches and serious bug fixes may be committed to the branch. However, you should submit all patches you want to commit to Jira first to give others the chance to review and possibly vote against the patch. Keep in mind that it is our main intention to keep the branch as stable as possible.
* All patches that are intended for the branch should first be committed to the unstable branch, merged into the stable branch, and then into the current release branch.
* Normal unstable and stable branch development may continue as usual. However, if you plan to commit a big change to the unstable branch while the branch feature freeze is in effect, think twice: can't the addition wait a couple more days? Merges of bug fixes into the branch may become more difficult.
* Only Jira issues with Fix version "%d.%d" and priority "Blocker" will delay a release candidate build.
""" % (v.major, v.minor)

    print("This is an e-mail template you can use as a basis for announcing the new branch and feature freeze.")
    print("The e-mail must be sent to dev@lucene.apache.org\n\n")
    print(mail_body)


todo_templates = [
    TodoGroup('prerequisites',
              'Prerequisites',
              'description',
              [
                  Todo('tools',
                       'Necessary tools are found',
                       description='Tools like java, ant, git etc are found and correct version',
                       done=True),
                  Todo('gpg',
                       'GPG key id is configured',
                       description="To sign the release you need to provide your GPG key ID. This must be the same key ID " +
                       "that you have registered in your Apache account. The ID is the key fingerprint, either full 40 bytes " +
                       "or last 8 bytes, e.g. the key of janhoy is 0D8D0B93." +
                       "\n- Make sure it is your 4096 bits key or larger" +
                       "\n- Upload your key to the MIT key server, pgp.mit.edu" +
                       "\n- Put you GPG key's fingerprint in the OpenPGP Public Key Primary Fingerprint field in your profile" +
                       """\n- The tests will complain if your GPG key has not been signed by another Lucene committer - this makes you a part of the GPG "web of trust" (WoT). Ask a committer that you know personally to sign your key for you, providing them with the fingerprint for the key."""
                       ,
                       links=['http://www.apache.org/dev/release-signing.html', 'https://id.apache.org'],
                       fun=get_gpg_key)
              ]),
    TodoGroup('preparation',
              'Work with the community to decide when and how etc',
              'description',
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
    TodoGroup('branching_versions',
              'Create branch (if needed) and update versions',
              "Here you'll do all the branching and version updates needed to prepare for the new release version",
              [
                  Todo('ant_precommit',
                       'Run ant precommit to run a bunch of sanity & quality checks',
                       'Fix any problems that are found.',
                       fun=ant_precommit),
                  Todo('create_major_release_branch',
                       'Create a new stable branch, i.e. branch_<major>x',
                       fun=create_stable_branch,
                       type=ReleaseType.major),
                  Todo('create_minor_release_branch',
                       'Create a minor release branch off the current stable branch',
                       fun=create_minor_branch,
                       type=ReleaseType.minor),
                  Todo('add_version_major',
                       'Add a new major version on master branch',
                       fun=add_version_major,
                       type=ReleaseType.major),
                  Todo('add_version_minor',
                       'Add a new minor version on stable branch',
                       fun=add_version_minor,
                       type=ReleaseType.minor),
                  Todo('add_version_bugfix',
                       'Add a new bugfix version on release branch',
                       fun=add_version_bugfix,
                       type=ReleaseType.bugfix),
                  Todo('sanity_check_doap',
                       'Sanity check the DOAP files',
                       description='Sanity check the DOAP files under dev-tools/doap/: do they contain all releases less than the one in progress?'),
                  Todo('jenkins_builds',
                       'Add Jenkins task for the release branch',
                       description='...so that builds run for the new branch. Consult the JenkinsReleaseBuilds page.',
                       fun=add_version_bugfix,
                       links=['https://wiki.apache.org/lucene-java/JenkinsReleaseBuilds'],
                       type=major_minor),
                  Todo('inform_devs_release_branch',
                       'Inform Devs of the Release Branch',
                       description="Send a note to dev@ to inform the committers that the branch has been created and the feature freeze phase has started",
                       fun=inform_devs_release_branch,
                       links=['https://wiki.apache.org/lucene-java/JenkinsReleaseBuilds'],
                       type=major_minor),
                  Todo('release_notes',
                       'Get a draft of the release notes in place',
                       description="These are typically edited on the Wiki.\n"
                       "Clone a page for a previous version as a starting point for your release notes. You will need two pages, one for Lucene and another for Solr, see links.\n"
                       "Edit the contents of CHANGES.txt into a more concise format for public consumption.\n"
                       "Ask on dev@ for input. Ideally the timing of this request mostly coincides with the release branch creation. It's a good idea to remind the devs of this later in the release too.",
                       links=['https://wiki.apache.org/lucene-java/ReleaseNote77', 'https://wiki.apache.org/solr/ReleaseNote77'],
                       )
              ]),
    TodoGroup('jira',
              'Add new versions to JIRA',
              'The next version after the release-version must be created in JIRA now',
              [
                  Todo('new_jira_version_lucene',
                       'Add a new version in Lucene JIRA for the next release',
                       description="""Go to the JIRA "Manage Versions" Administration pages and add a new (unreleased) version for the next release on the unstable branch (for a major release) or the stable branch (for a minor release).""",
                       links=['https://issues.apache.org/jira/plugins/servlet/project-config/LUCENE/versions'],
                       type=major_minor),
                  Todo('new_jira_version_solr',
                       'Add a new version in Solr JIRA for the next release',
                       description="""Go to the JIRA "Manage Versions" Administration pages and add a new (unreleased) version for the next release on the unstable branch (for a major release) or the stable branch (for a minor release).""",
                       links=['https://issues.apache.org/jira/plugins/servlet/project-config/SOLR/versions'],
                       type=major_minor)
              ]),
    TodoGroup('tests',
              'Make sure tests pass',
              'description',
              [
                  Todo('run_tests',
                       'Confirm that the tests pass within your release branch',
                       fun=run_tests)
              ],
              in_rc_loop=True),
    TodoGroup('artifacts',
              'Build the release artifacts',
              """If after the last day of the feature freeze phase no blocking issues are in JIRA with "Fix Version" X.Y then it's time to build the release artifacts """
              """run the smoke tester and stage the RC in svn""",
              [
                  Todo('clear_ivy_cache',
                       'Clear the ivy cache',
                       description='It is recommended to clean your Ivy cache before building the artifacts. This ensures that all Ivy dependencies are freshly downloaded, so we emulate a user that never used the Lucene build system before.',
                       fun=clear_ivy_cache),
                  Todo('build_rc',
                       'Build the release candidate',
                       description="This involves running the buildAndPushRelease.py script from your clean checkout.",
                       fun=build_rc)
              ],
              depends='tests',
              in_rc_loop=True),
    TodoGroup('vote',
              'Hold the vote',
              'description',
              [
                  Todo('id', 'title')
              ],
              in_rc_loop=True),
    TodoGroup('publish',
              'Publish the release artifacts',
              'description',
              [
                  Todo('id', 'title')
              ]),
    TodoGroup('website',
              'Update the website',
              'description',
              [
                  Todo('id', 'title')
              ]),
    TodoGroup('doap',
              'Update the DOAP file',
              'description',
              [
                  Todo('id', 'title')
              ]),
    TodoGroup('announce',
              'Announce the release',
              'description',
              [
                  Todo('id', 'title')
              ]),
    TodoGroup('post_release',
              'Tasks to do after release',
              'description',
              [
                  Todo('id', 'title')
              ])
]

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('Keyboard interrupt...exiting')

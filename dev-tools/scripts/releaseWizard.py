#!/usr/bin/env python3
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

# This script is a wizard that aims to (some day) replace the todoList at https://wiki.apache.org/lucene-java/ReleaseTodo
# It will walk you through the steps of the release process, asking for decisions or input along the way
# CAUTION: This is an alpha version, please read the HELP section in the main menu.
#
# Requirements:
#   python 3
#   pip3 install console-menu

import os
import platform
import sys
import json
import copy
import subprocess
import shutil
import shlex
import time
import fcntl
from collections import OrderedDict
from enum import Enum
import scriptutil
from scriptutil import BranchType, Version, check_ant, getGitRev, run
import re
from datetime import datetime
from datetime import timedelta
from consolemenu import ConsoleMenu
from consolemenu.screen import Screen
from consolemenu.items import FunctionItem, SubmenuItem

global state
global current_git_root
global todo_methods

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

epoch = datetime.utcfromtimestamp(0)

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
        self.release_version_major = None
        self.release_version_minor = None
        self.release_version_bugfix = None
        self.rc_number = 1
        self.start_date = unix_time_millis(datetime.now())
        self.branch = run("git rev-parse --abbrev-ref HEAD").strip()
        self.release_branch = self.branch
        try:
            self.branch_type = scriptutil.find_branch_type()
        except:
            print("WARNING: A release cannot happen from a feature branch (%s)" % self.branch)
            self.branch_type = 'feature'

    def set_release_version(self, version):
        self.release_version = version
        v = Version.parse(version)
        self.release_version_major = v.major
        self.release_version_minor = v.minor
        self.release_version_bugfix = v.bugfix
        if v.is_major_release():
            self.release_type = ReleaseType.major
            self.release_branch = "master"
        elif v.is_minor_release():
            self.release_type = ReleaseType.minor
            self.release_branch = self.get_stable_branch_name()
        else:
            self.release_type = ReleaseType.bugfix
            self.release_branch = self.get_minor_branch_name()

    def new_rc(self):
        if ask_yes_no("Are you sure? This will abort current RC"):
            dict = OrderedDict()
            for g in list(filter(lambda x: x.in_rc_loop(), self.todo_groups)):
                for t in g.get_todos():
                    if t.applies(self.release_type):
                        dict[t.id] = copy.deepcopy(t.state)
                        t.clear()
            self.previous_rcs["RC%d" % self.rc_number] = dict
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
            'start_date': self.start_date,
            'rc_number': self.rc_number,
            'script_branch': self.branch,
            'todos': tmp_todos,
            'previous_rcs': self.previous_rcs
        })

    def restore_from_dict(self, dict):
        self.script_version = dict['script_version']
        self.set_release_version(dict['release_version'])
        if 'start_date' in dict:
            self.start_date = dict['start_date']
        self.rc_number = dict['rc_number']
        self.branch = dict['script_branch']
        self.previous_rcs = copy.deepcopy(dict['previous_rcs'])
        for todo_id in dict['todos']:
            t = self.todos[todo_id]
            for k in dict['todos'][todo_id]:
                t.state[k] = dict['todos'][todo_id][k]

    def load(self):
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
        print("Loaded state from disk")

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
        lst = list(filter(lambda x: x.id == id, self.todo_groups))
        if len(lst) == 1:
            return lst[0]
        else:
            return None

    def get_todo_by_id(self, id):
        lst = list(filter(lambda x: x.id == id, self.todos.values()))
        if len(lst) == 1:
            return lst[0]
        else:
            return None

    def get_release_folder(self):
        folder = os.path.join(self.config_path, self.release_version)
        if not os.path.exists(folder):
            print("Creating folder %s" % folder)
            os.makedirs(folder)
        return folder

    def get_rc_folder(self):
        folder = os.path.join(self.get_release_folder(), "RC%d" % self.rc_number)
        if not os.path.exists(folder):
            print("Creating folder %s" % folder)
            os.makedirs(folder)
        return folder

    def get_git_checkout_folder(self):
        folder = os.path.join(self.get_release_folder(), "lucene-solr")
        return folder

    def get_minor_branch_name(self):
        return "branch_%s_%s" % (self.release_version_major, self.release_version_minor)

    def get_stable_branch_name(self):
        return "branch_%sx" % self.release_version_major

    def get_java_cmd(self):
        v = Version.parse(self.release_version)
        java_ver = java_versions[v.major]
        java_home = os.environ.get("JAVA%s_HOME" % java_ver)
        print("Using Java in %s for all 'ant' commands" % java_home)
        return os.path.join(java_home, "bin", "java")


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
            ret_str = ""
            for dep in ensure_list(self.depends):
                g = state.get_group_by_id(dep)
                if not g:
                    g = state.get_todo_by_id(dep)
                if g and not g.is_done():
                    ret_str += "NOTE: Please first complete '%s'\n" % g.title
                    return ret_str.strip()
        return None


class Todo:
    def __init__(self, id, title, description=None, done=False, type=None, fun=None, fun_args=None, links=None, commands=None, user_input=None, depends=None):
        self.depends = depends
        self.user_input = user_input
        self.commands = commands
        self.links = links
        self.done_initial_value = done
        self.fun = fun
        self.fun_args = fun_args
        self.types = type
        if not self.types:
            self.types = [ReleaseType.bugfix, ReleaseType.minor, ReleaseType.major]
        else:
            self.types = ensure_list(self.types)
        self.description = description
        self.title = title
        self.id = id
        self.state = {}
        self.set_done(done)

    def set_done(self, is_done):
        if is_done:
            self.state['done_date'] = unix_time_millis(datetime.now())
        else:
            self.state.clear()
        self.state['done'] = is_done

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
        try:
            if self.depends:
                ret_str = ""
                for dep in ensure_list(self.depends):
                    g = state.get_group_by_id(dep)
                    if not g:
                        g = state.get_todo_by_id(dep)
                    if not g.is_done():
                        print("This step depends on '%s'. Please complete that first\n" % g.title)
                        return
            desc = self.get_description()
            if desc:
                print("%s" % desc)
            if self.links:
                print("\nLinks:\n")
                for link in self.links:
                    print("- %s" % link)
                print()
            try:
                attr = getattr(todo_methods, self.id)
                if callable(attr) and not self.is_done():
                    # print("Calling %s by reclection" % self.id)
                    attr(self)
            except Exception as e:
                if self.fun and not self.is_done():
                    # print("Calling defined fun %s" % self.fun)
                    self.fun(self)
            if self.user_input and not self.is_done():
                ui_list = ensure_list(self.user_input)
                for ui in ui_list:
                    ui.run(self.state)
            cmds = self.get_commands()
            if cmds and not self.is_done():
                if not cmds.logs_prefix:
                    cmds.logs_prefix = self.id
                cmds.run()
            completed = ask_yes_no("Mark task '%s' as completed?" % self.title)
            self.set_done(completed)
            state.save()
        except Exception as e:
            print("ERROR while executing todo %s (%s)" % (self.title, e))

    def get_menu_item(self):
        return FunctionItem(self.get_title, self.display_and_confirm)

    def clone(self):
        clone = Todo(self.id, self.title, description=self.description)
        clone.state = copy.deepcopy(self.state)
        return clone

    def clear(self):
        self.state.clear()
        self.set_done(self.done_initial_value)

    def get_state(self):
        return self.state

    def get_description(self):
        desc = self.description
        try:
            attr = getattr(todo_methods, "%s_desc" % self.id)
            if callable(attr):
                desc = attr(self)
        except:
            pass
        return desc

    def get_commands(self):
        cmds = self.commands
        try:
            attr = getattr(todo_methods, "%s_commands" % self.id)
            if callable(attr):
                cmds = attr(self)
        except:
            pass
        return cmds

    def get_asciidoc(self):
        try:
            attr = getattr(todo_methods, "%s_asciidoc" % self.id)
            if callable(attr):
                return attr(self)
        except:
            pass
        return None


def get_release_version():
    v = str(input("Which version are you releasing? (x.y.z) "))
    try:
        version = Version.parse(v)
    except:
        print("Not a valid version %s" % v)
        return get_release_version()

    return str(version)


def get_subtitle():
    applying_groups = list(filter(lambda x: x.num_applies() > 0, state.todo_groups))
    done_groups = sum(1 for x in applying_groups if x.is_done())
    return "Please complete the below checklist (Complete: %s/%s)" % (done_groups, len(applying_groups))


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
        print("WARNING: Expected release version %s when on branch %s, but got %s" % (getScriptVersion(), branch, release_version))


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
    if ask_yes_no("Are you sure? This will erase all current progress"):
        shutil.rmtree(os.path.join(state.config_path, state.release_version))
        state.clear()

def help():
    print("""
Welcome to the role as Release Manager for Lucene/Solr, and the releaseWizard!

This tool aims to walk you through the whole release process step by step,
helping you to to run the right commands in the right order, generating
e-mail templates for you with the correct texts, versions, paths etc, obeying 
the voting rules and much more. It also serves as a documentation of all the
steps, with timestamps, preserving log files from each command etc.

As you complete each step the tool will ask you if the task is complete, making
it easy for you to know what is done and what is left to do. If you need to
re-spin a Release Candidata (RC) the Wizard will also help.

The Lucene project has automated much of the release process with various scripts,
and this wizard is the glue that binds it all together.

In the first TODO step in the checklist you will be asked to read up on the
Apache release policy and other relevant documents before you start the release. 

NOTE: Even if we have great tooling and some degree of automation, there are 
      still many manual steps and it is also important that the RM validates
      and QAs the process, validating that the right commands are run, and that
      the output from scripts are correct before proceeding.

DISCLAIMER: This is an alpha version. The wizard may be buggy and may generate
            faulty commands, commands that won't work on your OS or with all
            versions of tooling etc. So pleaes keep the old ReleaseTODO handy
            for cross-checking for now :)
""")


def ensure_list(o):
    if o is None:
        return []
    if not isinstance(o, list):
        return [o]
    else:
        return o


def open_file(filename):
    print("Opening file %s" % filename)
    if platform.system().startswith("Win"):
        run("start %s" % filename)
    else:
        run("open %s" % filename)


def generate_asciidoc():
    base_filename = os.path.join(state.get_release_folder(),
                            "lucene_solr_release_%s"
                            % (state.release_version.replace("\.", "_")))

    filename_adoc = "%s.adoc" % base_filename
    filename_html = "%s.html" % base_filename
    fh = open(filename_adoc, "w")

    fh.write("= Lucene/Solr Release %s\n\n" % state.release_version)
    fh.write("(_releaseWizard.py v%s ALPHA_)\n\n" % getScriptVersion())
    fh.write(":numbered:\n\n")
    for group in state.todo_groups:
        if group.num_applies() == 0:
            continue
        fh.write("== %s\n\n" % group.title)
        fh.write("%s\n\n" % group.description)
        for todo in group.get_todos():
            if not todo.applies(state.release_type):
                continue
            fh.write("=== %s\n\n" % todo.title)
            if todo.is_done():
                fh.write("_Completed %s_\n\n" % datetime.utcfromtimestamp(todo.state['done_date']/1000).strftime("%Y-%m-%d %H:%M UTC"))
            desc = todo.get_description()
            if desc:
                fh.write("%s\n\n" % desc)
            if todo.get_asciidoc():
                fh.write("%s\n\n" % todo.get_asciidoc())
            state_copy = copy.deepcopy(todo.state)
            state_copy.pop('done', None)
            state_copy.pop('done_date', None)
            if len(state_copy) > 0 or todo.user_input is not None:
                fh.write(".Variables collected in this step\n")
                fh.write("|===\n")
                fh.write("|Variable |Value\n")
                mykeys = set()
                for e in ensure_list(todo.user_input):
                    mykeys.add(e.name)
                for e in state_copy.keys():
                    mykeys.add(e)
                for key in mykeys:
                    val = "(not set)"
                    if key in state_copy:
                        val = state_copy[key]
                    fh.write("\n|%s\n|%s\n" % (key, val))
                fh.write("|===\n\n")
            if todo.links:
                fh.write("Links:\n\n")
                for l in todo.links:
                    fh.write("* %s\n" % l)
                fh.write("\n")
            cmds = todo.get_commands()
            if cmds:
                fh.write("%s\n\n" % cmds.commands_text)
                fh.write("[source,sh]\n----\n")
                if cmds.env:
                    for key in cmds.env:
                        val = cmds.env[key]
                        if is_windows():
                            fh.write("SET %s=%s\n" % (key, val))
                        else:
                            fh.write("export %s=%s\n" % (key, val))
                fh.write("cd %s\n" % cmds.get_root_folder())
                cmds2 = ensure_list(cmds.commands)
                for c in cmds2:
                    pre = post = ""
                    if c.cwd:
                        pre = "pushd %s && " % c.cwd
                        post = " && popd"
                    if c.comment:
                        if is_windows():
                            fh.write("REM %s\n" % c.comment)
                        else:
                            fh.write("# %s\n" % c.comment)
                    fh.write("%s%s%s\n" % (pre, c, post))
                fh.write("----\n\n")
    fh.close()
    print("Wrote file %s" % os.path.join(state.get_release_folder(), filename_adoc))
    print("Running command 'asciidoctor %s'" % filename_adoc)
    run_follow("asciidoctor %s" % filename_adoc)
    if os.path.exists(filename_html):
        open_file(filename_html)
    else:
        print("Failed generating HTML version, please install asciidoctor")


def release_other_version():
    os.remove(os.path.join(state.config_path, 'latest.json'))
    print("Please restart the wizard")
    sys.exit(0)



def main():
    global state
    global todo_methods

    todo_methods = TodoMethods()

    print ("Lucene/Solr releaseWizard v%s\n" % getScriptVersion())

    check_prerequisites()
    state = ReleaseState(todo_templates, os.path.expanduser("~/.lucene-releases/"), getScriptVersion())
    state.load()

    if not state.release_version:
        input_version = get_release_version()
        validate_release_version(state.branch_type, state.branch, input_version)
        state.set_release_version(input_version)
        state.save()

    os.environ['JAVACMD'] = state.get_java_cmd()

    main_menu = ConsoleMenu(title="Lucene/Solr ReleaseWizard (script-ver=v%s ALPHA)" % getScriptVersion(),
                            subtitle=get_releasing_text,
                            prologue_text="Welcome to the release wizard. From here you can manage the process including creating new RCs. "
                                          "All changes are persisted, so you can exit any time and continue later. Make sure to read the Help section.",
                            epilogue_text="® 2019 The Lucene/Solr project. Licensed under the Apache License 2.0",
                            screen=MyScreen())

    todo_menu = ConsoleMenu(title=get_releasing_text,
                            subtitle=get_subtitle,
                            prologue_text=None,
                            epilogue_text=None,
                            screen=MyScreen())

    for todo_group in state.todo_groups:
        if todo_group.num_applies() >= 0:
            todo_menu.append_item(todo_group.get_menu_item())

    main_menu.append_item(SubmenuItem(get_todo_menuitem_title, todo_menu))
    main_menu.append_item(FunctionItem(get_start_new_rc_menu_title, start_new_rc))
    main_menu.append_item(FunctionItem('Clear state, delete release-folder and restart from RC1', reset_state))
    main_menu.append_item(FunctionItem('Start release for a different version', release_other_version))
    main_menu.append_item(FunctionItem('Generate Asciidoc guide', generate_asciidoc))
    main_menu.append_item(FunctionItem('Help', help))

    main_menu.show()


sys.path.append(os.path.dirname(__file__))
current_git_root = os.path.abspath(os.path.join(os.path.abspath(os.path.dirname(__file__)), os.path.pardir, os.path.pardir))


def git_checkout_folder():
    return state.get_git_checkout_folder()

def tail_file(file, lines):
    bufsize = 8192
    fsize = os.stat(file).st_size
    iter = 0
    with open(file) as f:
        if bufsize >= fsize:
            bufsize = fsize
        while True:
            iter +=1
            seek_pos = fsize-bufsize*iter
            if seek_pos < 0:
                seek_pos = 0
            f.seek(seek_pos)
            data = []
            data.extend(f.readlines())
            if len(data) >= lines or f.tell() == 0 or seek_pos == 0:
                if not seek_pos == 0:
                    print("Tailing last %d lines of file %s" % (lines, file))
                print(''.join(data[-lines:]))
                break


def run_with_log_tail(command, cwd, logfile=None, tail_lines=10, tee=False):
    fh = sys.stdout
    if logfile:
        logdir = os.path.dirname(logfile)
        if not os.path.exists(logdir):
            os.makedirs(logdir)
        fh = open(logfile, 'w')
    rc = run_follow(command, cwd, fh=fh, tee=tee)
    if logfile:
        fh.close()
        if not tee and tail_lines and tail_lines > 0:
            tail_file(logfile, tail_lines)
    return rc


def ask_yes_no(text):
    answer = None
    while answer not in ['y', 'n']:
        answer = str(input("\nQ: %s (y/n): " % text))
    print("\n")
    return answer == 'y'


def abbreviate_line(line, width):
    line = line.strip()
    if len(line) > width:
        line = "%s.....%s" % (line[:(width/2-5)], line[-(width/2):])
    else:
        line = "%s%s" % (line, " " * (width-len(line)+2))
    return line


def print_line_cr(line, linenum, stdout=True, tee=False):
    if not tee:
        if not stdout:
            print("[line %s] %s" % (linenum, abbreviate_line(line, 80)), end='\r')
    else:
        if line.endswith("\r"):
            print(line.strip(), end='\r')
        else:
            print(line.strip())



def run_follow(command, cwd=None, fh=sys.stdout, tee=False):
    if not isinstance(command, list):
        command = shlex.split(command)
    process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, cwd=cwd, universal_newlines=True, bufsize=0, close_fds=True)
    lines_written = 0

    fl = fcntl.fcntl(process.stdout, fcntl.F_GETFL)
    fcntl.fcntl(process.stdout, fcntl.F_SETFL, fl | os.O_NONBLOCK)

    flerr = fcntl.fcntl(process.stderr, fcntl.F_GETFL)
    fcntl.fcntl(process.stderr, fcntl.F_SETFL, flerr | os.O_NONBLOCK)

    endstdout = endstderr = False
    errlines = []
    while not (endstderr and endstdout):
        lines_before = lines_written
        if not endstdout:
            try:
                line = process.stdout.readline()
                if line == '' and process.poll() is not None:
                    endstdout = True
                else:
                    fh.write("%s\n" % line.strip())
                    fh.flush()
                    lines_written += 1
                    print_line_cr(line, lines_written, stdout=fh == sys.stdout, tee=tee)

            except Exception as ioe:
                pass
        if not endstderr:
            try:
                line = process.stderr.readline()
                if line == '' and process.poll() is not None:
                    endstderr = True
                else:
                    errlines.append("%s\n" % line.strip())
                    lines_written += 1
                    print_line_cr(line, lines_written, stdout=fh == sys.stdout, tee=tee)
            except Exception as e:
                pass

        if not lines_written > lines_before:
            # if no output then sleep a bit before checking again
            time.sleep(0.1)

    print(" " * 80)
    rc = process.poll()
    if len(errlines) > 0:
        fh.write("--- STDERR ---\n")
        for line in errlines:
            fh.write("%s\n" % line.strip())
            fh.flush()
    return rc


def is_windows():
    return platform.system().startswith("Win")


class Commands:
    def __init__(self, root_folder, commands_text, commands,
                 logs_prefix=None, run_text=None, ask_run=True, ask_each=True, tail_lines=25,
                 stdout=False, env=None):
        self.env = env
        self.logs_prefix = logs_prefix
        self.stdout = stdout
        self.tail_lines = tail_lines
        self.ask_each = ask_each
        self.ask_run = ask_run
        self.commands = commands
        self.run_text = run_text
        self.commands_text = commands_text
        self.root_folder = root_folder

    def run(self):
        root = self.get_root_folder()

        print(self.commands_text)
        if self.env:
            for key in self.env:
                val = self.env[key]
                os.environ[key] = val
                if is_windows():
                    print("\n  SET %s=%s" % (key, val))
                else:
                    print("\n  export %s=%s" % (key, val))
        print("\n  cd %s" % root)
        commands = ensure_list(self.commands)
        for cmd in commands:
            pre = post = ''
            if cmd.comment:
                if is_windows():
                    print("  REM %s" % cmd.comment)
                else:
                    print("  # %s" % cmd.comment)
            if cmd.cwd:
                pre = "pushd %s && " % cmd.cwd
                post = " && popd"
            print("  %s%s%s" % (pre, cmd, post))

        if self.ask_run:
            if self.run_text:
                print("\n%s\n" % self.run_text)
            print("\nI can execute these commands for you if you choose.")
            if self.ask_each:
                print("You will get prompted before running each individual command.")
            else:
                print("You will not be prompted for each command but will see the ouput of each. If one command fails the execution will stop.")
            success = True
            if ask_yes_no("Do you want me to run these commands now?"):
                index = 0
                log_folder = self.logs_prefix if len(commands) > 1 else None
                for cmd in commands:
                    index += 1
                    if len(commands) > 1:
                        log_prefix = "%02d_" % index
                    else:
                        log_prefix = self.logs_prefix if self.logs_prefix else ''
                    if not log_prefix[-1:] == '_':
                        log_prefix += "_"
                    cwd = root
                    if cmd.cwd:
                        cwd = os.path.join(root, cmd.cwd)
                    folder_prefix = ''
                    if cmd.cwd:
                        folder_prefix = cmd.cwd + "_"
                    if not self.ask_each or ask_yes_no("Shall I run '%s' in folder '%s'" % (cmd, cwd)):
                        if not self.ask_each:
                            print("------------\nRunning '%s' in folder '%s'" % (cmd, cwd))
                        logfilename = cmd.logfile
                        logfile = None
                        if not cmd.stdout and not self.stdout:
                            if not log_folder:
                                log_folder = os.path.join(state.get_rc_folder(), "logs")
                            elif not os.path.isabs(log_folder):
                                log_folder = os.path.join(state.get_rc_folder(), "logs", log_folder)
                            if not logfilename:
                                logfilename = "%s.log" % re.sub(r"\W", "_", cmd.cmd)
                            logfile = os.path.join(log_folder, "%s%s%s" % (log_prefix, folder_prefix, logfilename))
                            print("Wait until command completes... Full log in %s\n" % logfile)
                        if not run_with_log_tail(cmd.cmd, cwd, logfile=logfile, tee=cmd.tee, tail_lines=self.tail_lines) == 0:
                            print("WARN: Command %s returned with error" % cmd.cmd)
                            success = False
                            if not self.ask_each:
                                print("Aborting")
                                break
            if not success:
                print("WARNING: One or more commands failed, you may want to check the logs")
            return success

    def get_root_folder(self):
        root = self.root_folder
        if callable(root):
            root = root()
        return root


class Command:
    def __init__(self, cmd, cwd=None, stdout=False, logfile=None, tee=False, comment=None):
        self.comment = comment
        self.tee = tee
        self.logfile = logfile
        self.stdout = stdout
        self.cwd = cwd
        self.cmd = cmd

    def __str__(self):
        return self.cmd


class UserInput():
    def __init__(self, name, prompt):
        self.prompt = prompt
        self.name = name

    def run(self, dict=None):
        result = str(input("%s : " % self.prompt))
        if dict:
            dict[self.name] = result
        return result


class TodoMethods:
    # These are called with reflection based on method matching to_do ID
    def clean_git_checkout(self, todo):
        if os.path.exists(state.get_git_checkout_folder()):
            if ask_yes_no("Found existing folder %s. Delete it now?" % state.get_git_checkout_folder()):
                shutil.rmtree(state.get_git_checkout_folder())

    def clean_git_checkout_commands(self, todo):
        return Commands(state.get_release_folder(),
                 "Run these commands to make a fresh clone in the release folder",
                        [
                            Command("git clone --progress https://gitbox.apache.org/repos/asf/lucene-solr.git lucene-solr", logfile="git_clone.log")
                        ],
                 ask_each=False
                        )

    def ant_precommit_commands(self, todo):
        return Commands(git_checkout_folder,
"""Fix any problems that are found by pushing fixes to the release branch and then running this task again. 
This task will always do `git pull` before `ant precommit` so it will catch changes to your branch :)""",
                        [
                            Command("git checkout %s" % get_release_branch(), stdout=True),
                            Command("git pull", stdout=True),
                            Command("ant clean precommit")
                        ],
                        env={'JAVACMD': state.get_java_cmd()},
                        ask_each=False)

    def create_stable_branch_commands(self, todo):
        return Commands(state.get_git_checkout_folder(),
                 "Run these commands to create a stable branch",
                 [
                     Command("git checkout %s" % 'master', stdout=True),
                     Command("git update", stdout=True),
                     Command("git checkout -b %s" % state.get_stable_branch_name()),
                     Command("git push origin %s" % state.get_stable_branch_name())
                 ],
                 ask_each=True)


    def create_minor_branch_commands(self, todo):
        return Commands(state.get_git_checkout_folder(),
                 "Run these commands to create a release branch",
                 [
                     Command("git checkout %s" % state.get_stable_branch_name(), stdout=True),
                     Command("git update", stdout=True),
                     Command("git checkout -b %s" % state.get_minor_branch_name()),
                     Command("git push origin %s" % state.get_minor_branch_name())
                 ],
                 ask_each=True)

    def add_version_bugfix_commands(self, todo):
        return Commands(state.get_git_checkout_folder(),
            "Do the following on the release branch only:",
            [
                Command("git checkout %s" % state.get_minor_branch_name()),
                Command("python3 -u %s %s" % (os.path.join(current_git_root, 'dev-tools/scripts/addVersion.py'), state.release_version)),
            ],
            ask_each=False)

    def add_version_minor(self, todo):
        print("TODO")

    def add_version_major(self, todo):
        print("TODO")


    def inform_devs_desc(self, todo):
        release_version = "%d.%d" % (state.release_version_major, state.release_version_minor)
        next_version = "%d.%d" % (state.release_version_major, state.release_version_minor+1)
        desc = """This is an e-mail template you can use as a basis for 
announcing the new branch and feature freeze.

.Mail template
----
To: dev@lucene.apache.org
Subject: New branch and feature freeze for Lucene/Solr %s

NOTICE:

Branch %s has been cut and versions updated to %s on stable branch.

Please observe the normal rules:
        
* No new features may be committed to the branch.
* Documentation patches, build patches and serious bug fixes may be 
  committed to the branch. However, you should submit all patches you 
  want to commit to Jira first to give others the chance to review 
  and possibly vote against the patch. Keep in mind that it is our 
  main intention to keep the branch as stable as possible.
* All patches that are intended for the branch should first be committed 
  to the unstable branch, merged into the stable branch, and then into 
  the current release branch.
* Normal unstable and stable branch development may continue as usual. 
  However, if you plan to commit a big change to the unstable branch 
  while the branch feature freeze is in effect, think twice: can't the 
  addition wait a couple more days? Merges of bug fixes into the branch 
  may become more difficult.
* Only Jira issues with Fix version %s and priority "Blocker" will delay 
  a release candidate build.
----
""" % (release_version, state.get_minor_branch_name(), next_version, release_version)
        return desc

    def inform_devs_bugfix_desc(self, todo):
        desc = """This is an e-mail template you can use as a basis for 
announcing the rules for committing to the releaes branch

.Mail template
----
To: dev@lucene.apache.org
Subject: Bugfix release Lucene/Solr %s

NOTICE:

I am now preparing for a bugfix release from branch %s

Please observe the normal rules for committing to this branch:
        
* Before committing to the branch, reply to this thread and argue
  why the fix needs backporting and how long it will take.
* All issues accepted for backporting should be marked with %s
  in JIRA and issues that should delay the release must be marked as Blocker
* All patches that are intended for the branch should first be committed 
  to the unstable branch, merged into the stable branch, and then into 
  the current release branch.
* Only Jira issues with Fix version %s and priority "Blocker" will delay 
  a release candidate build.
----
""" % (state.release_version, state.get_minor_branch_name(), state.release_version, state.release_version)
        return desc

    def clear_ivy_cache_commands(self, todo):
        ivy_path = os.path.expanduser("~/.ivy2/")
        return Commands(
            ivy_path,
            """It is recommended to clean your Ivy cache before building the artifacts.
This ensures that all Ivy dependencies are freshly downloaded, 
so we emulate a user that never used the Lucene build system before.
One way is to rename the ivy cache folder before building.
""",
            [
                Command("mv cache cache_bak", stdout=True),
            ],
            ask_each=False)

    def run_tests_commands(self, todo):
        return Commands(
            git_checkout_folder,
            "Run some tests not ran by `buildAndPublishRelease.py`",
            [
                Command("git checkout %s" % get_release_branch(), stdout=True),
                Command('ant javadocs', 'lucene'),
                Command('ant javadocs', 'solr')
            ],
            ask_each=True)

    def smoke_tester_commands(self, todo):
        os.chdir(state.get_git_checkout_folder)
        git_rev = open('rev.txt', encoding='UTF-8').read()
        cmdline = "python3 -u %s --tmp-dir %s %s" \
                  % (os.path.join('dev-tools', 'scripts', 'smokeTestRelease.py'),
                     os.path.join(state.get_rc_folder(), "smoketest"),
                     "file://%s" % dir)
                     # --revision $REV --version $VER $NOTSIGNED file://$DIR
                     # --revision %s --version %s

        return Commands(
            state.get_git_checkout_folder(),
            """Here we'll test the release""",
            [
                Command(cmdline, logfile="smoketest.log", tee=True),
            ],
            env={'JAVACMD': state.get_java_cmd()},
            ask_run=False,
            ask_each=False)

    def build_rc_commands(self, todo):
        try:
            key_id = state.get_todo_by_id('gpg').get_state()['gpg_key']
        except Exception as e:
            raise Exception("Faild getting key: %s" % e)

        # logfile = os.path.join(state.get_rc_folder(), 'logs', 'buildAndPushRelease.log')
        logfile = "/tmp/release.log"
        cmdline = "python3 -u %s --push-local %s --rc-num %s --sign %s" \
                  % (os.path.join('dev-tools', 'scripts', 'buildAndPushRelease.py'),
                     os.path.join(state.get_rc_folder(), "dist"),
                     state.rc_number,
                     key_id)
        # Add --logfile %s"

        return Commands(
            state.get_git_checkout_folder(),
            """In this step we will build the RC using python script `buildAndPushRelease.py`
We have tried to compile the correct command below, and you need to execute
it in another Terminal window yourself.

Note that the script will take a long time. To follow the detailed build 
log, tail the log in another Terminal:
`tail -f %s`
""" % logfile,
            [
                Command("git checkout %s" % get_release_branch(), stdout=True),
                Command("git clean -df", stdout=True, comment="Make sure checkout is clean and up to date"),
                Command("git checkout -- .", stdout=True),
                Command("git pull"),
                Command(cmdline, logfile="build_rc.log", tee=True),
            ],
            env={'JAVACMD': state.get_java_cmd()},
            ask_run=False,
            ask_each=False)


    def initiate_vote_desc(self, todo):
        dow = datetime.utcnow().weekday()
        if dow == 6:            # Sun
            days_to_add = 1
        elif dow in [2, 3, 4, 5]: # Wed, Thu, Fri, Sat
            days_to_add = 2
        else:
            days_to_add = 0
        vote_close_date = (datetime.utcnow() + timedelta(hours=73) + timedelta(days=days_to_add))
        vote_close = vote_close_date.strftime("%Y-%m-%d %H:00 UTC")
        todo.state['vote_close'] = vote_close
        todo.state['vote_close_epoch'] = unix_time_millis(vote_close_date)
        return """Initiate the vote on the dev mailing list
        
.Mail template
----
To: dev@lucene.apache.org
Subject: [VOTE] Release Lucene/Solr %s RC%s

Please vote for release candidate %s for Lucene/Solr %s

The artifacts can be downloaded from:
https://dist.apache.org/repos/dist/dev/lucene/lucene-solr-8.1.0-RC1-reve5839fb416083fcdaeedfb1e329a9fdaa29fdc50

You can run the smoke tester directly with this command:

python3 -u dev-tools/scripts/smokeTestRelease.py \
https://dist.apache.org/repos/dist/dev/lucene/lucene-solr-8.1.0-RC1-reve5839fb416083fcdaeedfb1e329a9fdaa29fdc50

Vote will be open for at least 72 hours excluding weekends, i.e. until %s.

[ ] +1  approve
[ ] +0  no opinion
[ ] -1  disapprove (and reason why)

Here is my +1
----
""" % (state.release_version, state.rc_number, state.rc_number, state.release_version, vote_close)


    def end_vote(self, todo):
        initiate_vote_dict = state.get_todo_by_id("initiate_vote").state
        if not initiate_vote_dict['done'] is True:
            print("A vote has not been initiated, cannot close.")
            return
        else:
            if initiate_vote_dict['vote_close_epoch'] > unix_time_millis(datetime.now()):
                print("Cannot close vote until 72h vote time plus weekends have passed, which is %s" % initiate_vote_dict['vote_close'])
                if not ask_yes_no("Continue with closing the vote anyway?"):
                    return

        print("Please sum up the ")
        plus_binding = int(UserInput("plus_binding", "Number of binding +1 votes (PMC members)").run(todo.state))
        plus_other = int(UserInput("plus_other", "Number of other +1 votes").run(todo.state))
        zero = int(UserInput("zero", "Number of 0 votes").run(todo.state))
        minus = int(UserInput("minus", "Number of -1 votes").run(todo.state))

        success, desc, template = self.end_vote_result(plus_binding, plus_other, zero, minus)

        print("%s\n\n%s" % (desc, template))

    def end_vote_asciidoc(self, todo):
        return """Note down how many votes were cast, summing as:

* Binding PMC-member +1 votes
* Non-binding +1 votes
* Neutral +/-0 votes
* Negative -1 votes

You need 3 binding +1 votes and more +1 than -1 votes for the release to happen.
A release cannot be vetoed, see more in provided links.

Here are some mail templates for successful and failed vote results with sample numbers:

%s

%s

%s
""" % (self.end_vote_result(5, 1, 0, 2)[2],
       self.end_vote_result(2, 3, 0, 0)[2],
       self.end_vote_result(3, 0, 1, 4)[2])

    def end_vote_result(self, plus_binding, plus_other, zero, minus):
        desc = ""
        mail_template = ""
        if plus_binding >= 3 and plus_binding > minus:
            success = True
            desc += "The vote has succeeded\n\n"
            if minus > 0:
                desc += """
However, there were negative votes. A release cannot be vetoed, and as long as
there are more positive than negative votes you can techically release
the software. However, please review the negative votes and consider
a re-spin."""

            mail_template += """            
.Mail template successful vote
----
To: dev@lucene.apache.org
Subject: [RESULT] [VOTE] Release Lucene/Solr %s RC%s

It's been >72h since the vote was initiated and the result is:

+1  %s  (%s binding)
 0  %s
-1  %s

This vote has PASSED.
----""" % (state.release_version, state.rc_number, plus_binding + plus_other, plus_binding, zero, minus)
        else:
            success = False
            if plus_binding < 3:
                reason = "less than three binding +1 votes"
            else:
                reason = "too many -1 votes"
            desc += "The vote was not successful"
            mail_template += """
.Mail template failed vote
----
To: dev@lucene.apache.org
Subject: [FAILED] [VOTE] Release Lucene/Solr %s RC%s

This vote has FAILED due to %s.
The vote result was:

+1  %s  (%s binding)
 0  %s
-1  %s
----
    """ % (state.release_version, state.rc_number, reason, plus_binding + plus_other, plus_binding, zero, minus)
        return success, desc, mail_template


def get_release_branch():
    return state.release_branch


todo_templates = [
    TodoGroup('prerequisites',
              'Prerequisites',
              'description',
              [
                  Todo('read_up',
                       'Read up on the release process',
                       description=
"""As a Release Manager (RM) you should be familiar with Apache's release policy, voting rules, 
create a PGP/GPG key for use with signing and more. Please familiarise yourself with the resources 
listed below.
""",
                       links=["http://www.apache.org/dev/release-publishing.html",
                              "http://www.apache.org/legal/release-policy.html",
                              "http://www.apache.org/dev/release-signing.html",
                              "https://wiki.apache.org/lucene-java/ReleaseTodo"],
                       done=False),
                  Todo('tools',
                       'Necessary tools are installed',
                       description=
"""You will need these tools:

* Python v3.4 or later
* Java 8 in $JAVA8_HOME and Java 11 in $JAVA11_HOME
* Apache Ant 1.8-1.10
* gpg
* git
* asciidoc (to generate HTML version)
""",
                       done=True),
                  Todo('gpg',
                       'GPG key id is configured',
                       description=
"""To sign the release you need to provide your GPG key ID. This must be 
the same key ID that you have registered in your Apache account. 
The ID is the key fingerprint, either full 40 bytes or last 8 bytes, e.g. 0D8D0B93.

* Make sure it is your 4096 bits key or larger
* Upload your key to the MIT key server, pgp.mit.edu
* Put you GPG key's fingerprint in the OpenPGP Public Key Primary Fingerprint 
  field in your profile
* The tests will complain if your GPG key has not been signed by another Lucene 
  committer. This makes you a part of the GPG "web of trust" (WoT). Ask a committer 
  that you know personally to sign your key for you, providing them with the 
  fingerprint for the key.""",
                       links=['http://www.apache.org/dev/release-signing.html', 'https://id.apache.org'],
                       user_input=[
                           UserInput("gpg_key", "Please enter your gpg key ID, e.g. 0D8D0B93")
                       ])
              ]),
    TodoGroup('preparation',
              'Work with the community to decide when and how etc',
              'description',
              [
                  Todo('decide_jira_issues',
                       'Select JIRA issues to be included',
                       description='Set the appropriate "Fix Version" in JIRA for the issues that should be included in the release.'),
                  Todo('decide_branch_date',
                       'Decide the date for branching',
                       user_input=UserInput("date", "Enter date (YYYY-MM-DD)"),
                       type=major_minor),
                  Todo('decide_freeze_length',
                       'Decide the lenght of feature freeze',
                       user_input=UserInput("date", "Enter end date of feature freeze (YYYY-MM-DD)"),
                       type=major_minor)
              ]),
    TodoGroup('branching_versions',
              'Create branch (if needed) and update versions',
              "Here you'll do all the branching and version updates needed to prepare for the new release version",
              [
                  Todo('clean_git_checkout',
                       'Do a clean git clone to do the release from.',
                       description="This eliminates the risk of a dirty checkout"),
                  Todo('ant_precommit',
                       'Run ant precommit and fix issues',
                       depends="clean_git_checkout"
                       ),
                  Todo('create_stable_branch',
                       'Create a new stable branch, i.e. branch_<major>x',
                       type=ReleaseType.major,
                       depends="clean_git_checkout"),
                  Todo('create_minor_branch',
                       'Create a minor release branch off the current stable branch',
                       type=ReleaseType.minor,
                       depends="clean_git_checkout"),
                  Todo('add_version_major',
                       'Add a new major version on master branch',
                       type=ReleaseType.major),
                  Todo('add_version_minor',
                       'Add a new minor version on stable branch',
                       type=ReleaseType.minor),
                  Todo('sanity_check_doap',
                       'Sanity check the DOAP files',
                       description="""
Sanity check the DOAP files under dev-tools/doap/
Do they contain all releases less than the one in progress?

TIP: The buildAndPushRelease script run later will check this automatically"""),
                  Todo('jenkins_builds',
                       'Add Jenkins task for the release branch',
                       description='...so that builds run for the new branch. Consult the JenkinsReleaseBuilds page.',
                       links=['https://wiki.apache.org/lucene-java/JenkinsReleaseBuilds'],
                       type=major_minor),
                  Todo('inform_devs',
                       'Inform Devs of the Release Branch',
                       description="Send a note to dev@ to inform the committers that the branch has been created and the feature freeze phase has started",
                       type=major_minor),
                  Todo('inform_devs_bugfix',
                       'Inform Devs about the release',
                       description="Send a note to dev@ to inform the committers about the rules for committing to the branch",
                       type=major_minor),
                  Todo('draft_release_notes',
                       'Get a draft of the release notes in place',
                       description=
"""These are typically edited on the Wiki.
                       
Clone a page for a previous version as a starting point for your release notes. 
You will need two pages, one for Lucene and another for Solr, see links.
Edit the contents of `CHANGES.txt` into a more concise format for public consumption.
Ask on dev@ for input. Ideally the timing of this request mostly coincides with the 
release branch creation. It's a good idea to remind the devs of this later in the release too.""",
                       links=['https://wiki.apache.org/lucene-java/ReleaseNote77', 'https://wiki.apache.org/solr/ReleaseNote77'],
                       )
              ]),
    TodoGroup('jira',
              'Add new versions to JIRA',
              'The next version after the release-version must be created in JIRA now',
              [
                  Todo('new_jira_version_lucene',
                       'Add a new version in Lucene JIRA for the next release',
                       description=
"""Go to the JIRA "Manage Versions" Administration pages
and add a new (unreleased) version for the next release on the unstable branch (for a major release)
or the stable branch (for a minor release).""",
                       links=['https://issues.apache.org/jira/plugins/servlet/project-config/LUCENE/versions']),
                  Todo('new_jira_version_solr',
                       'Add a new version in Solr JIRA for the next release',
                       description=
"""Go to the JIRA "Manage Versions" Administration pages
and add a new (unreleased) version for the next release on the unstable branch (for a major release)
or the stable branch (for a minor release).""",
                       links=['https://issues.apache.org/jira/plugins/servlet/project-config/SOLR/versions'])
              ]),
    TodoGroup('artifacts',
              'Build the release artifacts',
"""If after the last day of the feature freeze phase no blocking issues are in JIRA with "Fix Version" X.Y 
then it's time to build the release artifacts, run the smoke tester and stage the RC in svn""",
              [
                  Todo('run_tests',
                       'Run javadoc tests'
                       ),
                  # TODO: Examine the results. Did it build without errors? Were there Javadoc warnings? Did the tests succeed? Does the demo application work correctly? Does Test2BTerms pass (this takes a lot of memory)?
                  # Remove lucene/benchmark/{work,temp}/ if present
                  Todo('clear_ivy_cache',
                       'Clear the ivy cache'),
                  Todo('build_rc',
                       'Build the release candidate',
                       depends="gpg"),
                  Todo('smoke_tester',
                       'Build the release candidate',
                       depends="build_rc")
                  # Run the smoke test script against the local release candidate: python3 -u dev-tools/scripts/smokeTestRelease.py /tmp/releases/6.0.1/lucene-solr-6.0.1-RC1-rev...
                  # Import the artifacts into SVN: svn -m "Lucene/Solr 6.0.1 RC1" import /tmp/releases/6.0.1/lucene-solr-6.0.1-RC1-rev... https://dist.apache.org/repos/dist/dev/lucene/lucene-solr-6.0.1-RC1-rev...
                  # If you have cancelled a prior release candidate, remove it from SVN: svn -m "Remove cancelled Lucene/Solr 6.0.1 RC1" rm https://dist.apache.org/repos/dist/dev/lucene/lucene-solr-6.0.1-RC1-rev...
                  # Don't delete these artifacts from your local workstation as you'll need to publish the maven subdirectories once the RC passes (see below).
              ],
              depends=['test', 'prerequisites'],
              in_rc_loop=True),
    TodoGroup('voting',
              'Hold the vote and sum up the results',
              'description',
              [
                  Todo('initiate_vote',
                       'Initiate the vote',
                       links=["https://www.apache.org/foundation/voting.html"]),
                  Todo('end_vote',
                       'End vote',
                       description="At the end of the voting deadline, count the votes and send RESULT message to mailing list.",
                       links=["https://www.apache.org/foundation/voting.html"])
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
                  Todo('add_version_bugfix',
                       'Add a new bugfix version on release branch',
                       type=ReleaseType.bugfix),
              ])
]

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('Keyboard interrupt...exiting')

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
#   Install requirements with this command:
#   pip3 install -r requirements.txt
#
# Usage:
#   releaseWizard.py [-h] [--dry-run] [--root PATH]
#
#   optional arguments:
#   -h, --help   show this help message and exit
#   --dry-run    Do not execute any commands, but echo them instead. Display
#   extra debug info
#   --root PATH  Specify different root folder than ~/.lucene-releases

import os
import argparse
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
import scriptutil
from scriptutil import BranchType, Version, check_ant, download, run
import re
from datetime import datetime
from datetime import timedelta
from consolemenu import ConsoleMenu
from consolemenu.screen import Screen
from consolemenu.items import FunctionItem, SubmenuItem
from jinja2 import Environment
import yaml

global current_git_root

# Solr:Java version mapping
java_versions = {6: 8, 7: 8, 8: 8, 9: 11}
dry_run = False

major_minor = ['major', 'minor']
script_path = os.path.dirname(os.path.realpath(__file__))
os.chdir(script_path)

# Edit this to add other global jinja2 variables or filters
def expand_jinja(text, vars=None):
    global_vars = OrderedDict({
        'script_version': state.script_version,
        'release_version': state.release_version,
        'ivy2_folder': os.path.expanduser("~/.ivy2/"),
        'config_path': state.config_path,
        'rc_number': state.rc_number,
        'script_branch': state.script_branch,
        'release_folder': state.get_release_folder(),
        'git_checkout_folder': state.get_git_checkout_folder(),
        'dist_url_base': 'https://dist.apache.org/repos/dist/dev/lucene',
        'm2_repository_url': 'https://repository.apache.org/service/local/staging/deploy/maven2',
        'dist_file_path': state.get_dist_folder(),
        'rc_folder': state.get_rc_folder(),
        'base_branch': state.get_base_branch_name(),
        'release_branch': state.release_branch,
        'stable_branch': state.get_stable_branch_name(),
        'minor_branch': state.get_minor_branch_name(),
        'release_type': state.release_type,
        'release_version_major': state.release_version_major,
        'release_version_minor': state.release_version_minor,
        'release_version_bugfix': state.release_version_bugfix,
        'state': state,
        'epoch': unix_time_millis(datetime.utcnow()),
        'get_next_version': state.get_next_version(),
        'current_git_rev': state.get_current_git_rev(),
        'keys_downloaded': keys_downloaded(),
        'vote_close_72h': vote_close_72h_date().strftime("%Y-%m-%d %H:00 UTC"),
        'vote_close_72h_epoch': unix_time_millis(vote_close_72h_date())
    })
    global_vars.update(state.get_todo_states())
    if vars:
        global_vars.update(vars)

    filled = replace_templates(text)

    try:
        env = Environment(lstrip_blocks=True, keep_trailing_newline=False, trim_blocks=True)
        env.filters['path_join'] = lambda paths: os.path.join(*paths)
        template = env.from_string(str(filled), globals=global_vars)
        filled = template.render()
    except Exception as e:
        print("Exception while rendering jinja template %s: %s" % (str(filled)[:10], e))
    return filled


def replace_templates(text):
    tpl_lines = []
    for line in text.splitlines():
        if line.startswith("(( template="):
            match = re.search(r"^\(\( template=(.+?) \)\)", line)
            name = match.group(1)
            tpl_lines.append(replace_templates(templates[name].strip()))
        else:
            tpl_lines.append(line)
    return "\n".join(tpl_lines)


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
    if sys.version_info < (3, 4):
        sys.exit("Script requires Python v3.4 or later")
    try:
        run("gpg --version")
    except:
        sys.exit("You will need gpg installed")
    if not check_ant().startswith('1.8'):
        print("WARNING: This script will work best with ant 1.8. The script buildAndPushRelease.py may have problems with PGP password input under ant 1.10")
    if not 'JAVA8_HOME' in os.environ or not 'JAVA11_HOME' in os.environ:
        sys.exit("Please set environment variables JAVA8_HOME and JAVA11_HOME")
    try:
        run("asciidoctor -V")
    except:
        print("WARNING: In order to export asciidoc version to HTML, you will need asciidoctor installed")
    try:
        run("git --version")
    except:
        sys.exit("You will need git installed")


epoch = datetime.utcfromtimestamp(0)


def unix_time_millis(dt):
    return int((dt - epoch).total_seconds() * 1000.0)


def bootstrap_todos(state):
    file = open(os.path.join(script_path, "releaseWizard.yaml"), "r")
    todo_list = yaml.load(file, Loader=yaml.Loader).get('groups')
    # Establish links from commands to to_do for finding todo vars
    for tg in todo_list:
        if dry_run:
            print("Group %s" % tg.id)
        for td in tg.get_todos():
            if dry_run:
                print("  Todo %s" % td.id)
            cmds = td.commands
            if cmds:
                if dry_run:
                    print("  Commands")
                cmds.todo_id = td.id
                for cmd in cmds.commands:
                    if dry_run:
                        print("    Command %s" % cmd.cmd)
                    cmd.todo_id = td.id

    print("Loaded TODO definitions from releaseWizard.yaml")
    return todo_list


def maybe_remove_rc_from_svn():
    todo = state.get_todo_by_id('import_svn')
    if todo and todo.is_done():
        print("import_svn done")
        Commands(state.get_git_checkout_folder(),
                 """Looks like you uploaded artifacts for {{ build_rc.git_rev | default("<git_rev>", True) }} to svn which needs to be removed.""",
                 [Command(
                 """svn -m "Remove cancelled Lucene/Solr {{ release_version }} RC{{ rc_number }}" rm {{ dist_url }}""",
                 logfile="svn_rm.log",
                 tee=True,
                 vars={
                     'dist_folder': """lucene-solr-{{ release_version }}-RC{{ rc_number }}-rev{{ build_rc.git_rev | default("<git_rev>", True) }}""",
                     'dist_url': "{{ dist_url_base }}/{{ dist_folder }}"
                 }
             )],
                 enable_execute=True, confirm_each_command=False).run()


# To be able to hide fields when dumping Yaml
class SecretYamlObject(yaml.YAMLObject):
    hidden_fields = []
    @classmethod
    def to_yaml(cls,dumper,data):
        print("Dumping object %s" % type(data))

        new_data = copy.deepcopy(data)
        for item in cls.hidden_fields:
            if item in new_data.__dict__:
                del new_data.__dict__[item]
        for item in data.__dict__:
            if item in new_data.__dict__ and new_data.__dict__[item] is None:
                del new_data.__dict__[item]
        return dumper.represent_yaml_object(cls.yaml_tag, new_data, cls,
                                            flow_style=cls.yaml_flow_style)


def str_presenter(dumper, data):
    if len(data.split('\n')) > 1:  # check for multiline string
        return dumper.represent_scalar('tag:yaml.org,2002:str', data, style='|')
    return dumper.represent_scalar('tag:yaml.org,2002:str', data)


class ReleaseState:
    def __init__(self, config_path, script_version):
        self.script_version = script_version
        self.config_path = config_path
        self.todo_groups = None
        self.todos = None
        self.previous_rcs = OrderedDict()
        self.release_version = None
        self.release_type = None
        self.release_version_major = None
        self.release_version_minor = None
        self.release_version_bugfix = None
        self.rc_number = 1
        self.start_date = unix_time_millis(datetime.now())
        self.script_branch = run("git rev-parse --abbrev-ref HEAD").strip()
        self.release_branch = None
        try:
            self.branch_type = scriptutil.find_branch_type()
        except:
            print("WARNING: This script shold (ideally) run from the release branch, not a feature branch (%s)" % self.script_branch)
            self.branch_type = 'feature'

    def set_release_version(self, version):
        self.release_version = version
        v = Version.parse(version)
        self.release_version_major = v.major
        self.release_version_minor = v.minor
        self.release_version_bugfix = v.bugfix
        self.release_branch = self.get_minor_branch_name()
        if v.is_major_release():
            self.release_type = 'major'
        elif v.is_minor_release():
            self.release_type = 'minor'
        else:
            self.release_type = 'bugfix'

    def get_base_branch_name(self):
        v = Version.parse(self.release_version)
        if v.is_major_release():
            return 'master'
        elif v.is_minor_release():
            return self.get_stable_branch_name()
        else:
            return self.get_minor_branch_name()

    def clear_rc(self):
        if ask_yes_no("Are you sure? This will clear and restart RC%s" % self.rc_number):
            maybe_remove_rc_from_svn()
            dict = OrderedDict()
            for g in list(filter(lambda x: x.in_rc_loop(), self.todo_groups)):
                for t in g.get_todos():
                    t.clear()
            print("Cleared RC TODO state")
            shutil.rmtree(self.get_rc_folder())
            print("Cleared folder %s" % self.get_rc_folder())
            self.save()

    def new_rc(self):
        if ask_yes_no("Are you sure? This will abort current RC"):
            maybe_remove_rc_from_svn()
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
            'script_branch': self.script_branch,
            'todos': tmp_todos,
            'previous_rcs': self.previous_rcs
        })

    def restore_from_dict(self, dict):
        self.script_version = dict['script_version']
        self.set_release_version(dict['release_version'])
        if 'start_date' in dict:
            self.start_date = dict['start_date']
        self.rc_number = dict['rc_number']
        self.script_branch = dict['script_branch']
        self.previous_rcs = copy.deepcopy(dict['previous_rcs'])
        for todo_id in dict['todos']:
            if todo_id in self.todos:
                t = self.todos[todo_id]
                for k in dict['todos'][todo_id]:
                    t.state[k] = dict['todos'][todo_id][k]
            else:
                print("Warning: Could not restore state for %s, Todo definition not found" % todo_id)

    def load(self):
        latest = None

        if not self.todo_groups:
            self.todo_groups = bootstrap_todos(self)
            self.init_todos()
        if not os.path.exists(self.config_path):
            print("Creating folder %s" % self.config_path)
            os.makedirs(self.config_path)
        if os.path.exists(os.path.join(self.config_path, 'latest.json')):
            with open(os.path.join(self.config_path, 'latest.json'), 'r') as fp:
                latest = json.load(fp)['version']
                print("Continuing an in-progress release version %s in %s" % (latest, os.path.join(self.config_path, 'latest.json')))

        if latest and os.path.exists(os.path.join(self.config_path, latest, 'state.json')):
            state_file = os.path.join(self.config_path, latest, 'state.json')
            with open(state_file, 'r') as fp:
                try:
                    dict = json.load(fp)
                    self.restore_from_dict(dict)
                    print("Loaded state from %s" % state_file)
                except Exception as e:
                    print("Failed to load state from %s: %s" % (state_file, e))

    def save(self):
        print("Saving")
        # Storing working version in latest.json
        with open(os.path.join(self.config_path, 'latest.json'), 'w') as fp:
            json.dump({'version': self.release_version}, fp)

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
        self.save()

    def get_rc_number(self):
        return self.rc_number

    def get_current_git_rev(self):
        try:
            return run("git rev-parse HEAD", cwd=self.get_git_checkout_folder()).strip()
        except:
            return "<git-rev>"

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

    def get_todo_state_by_id(self, id):
        lst = list(filter(lambda x: x.id == id, self.todos.values()))
        if len(lst) == 1:
            return lst[0].state
        else:
            return {}

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

    def get_dist_folder(self):
        folder = os.path.join(self.get_rc_folder(), "dist")
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

    def get_next_version(self):
        if self.release_type == 'major':
            return "master (%s.0)" % (self.release_version_major + 1)
        if self.release_type == 'minor':
            return "%s.%s" % (self.release_version_major, self.release_version_minor + 1)
        if self.release_type == 'bugfix':
            return "%s.%s.%s" % (self.release_version_major, self.release_version_minor, self.release_version_bugfix + 1)

    def get_java_home(self):
        v = Version.parse(self.release_version)
        java_ver = java_versions[v.major]
        java_home_var = "JAVA%s_HOME" % java_ver
        if java_home_var in os.environ:
            return os.environ.get(java_home_var)
        else:
            raise Exception("Script needs environment variable %s" % java_home_var )

    def get_java_cmd(self):
        return os.path.join(self.get_java_home(), "bin", "java")

    def get_todo_states(self):
        states = {}
        if self.todos:
            for todo_id in self.todos:
                t = self.todos[todo_id]
                states[todo_id] = copy.deepcopy(t.state)
        return states

    def get_release_type(self):
        return self.release_type.value

    def init_todos(self):
        self.todos = {}
        for g in self.todo_groups:
            for t in g.get_todos():
                self.todos[t.id] = t


class TodoGroup(SecretYamlObject):
    yaml_tag = u'!TodoGroup'
    hidden_fields = []
    def __init__(self, id, title, description, todos, is_in_rc_loop=None, depends=None):
        self.id = id
        self.title = title
        self.description = description
        self.depends = depends
        self.is_in_rc_loop = is_in_rc_loop
        self.todos = todos

    @classmethod
    def from_yaml(cls, loader, node):
        fields = loader.construct_mapping(node, deep = True)
        return TodoGroup(**fields)

    def num_done(self):
        return sum(1 for x in self.todos if x.is_done() > 0)

    def num_applies(self):
        count = sum(1 for x in self.todos if x.applies(state.release_type))
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
        menu = ConsoleMenu(title=self.title, subtitle=self.get_subtitle, prologue_text=self.get_description(),
                           screen=MyScreen(), exit_option_text='Return')
        for todo in self.get_todos():
            if todo.applies(state.release_type):
                menu.append_item(todo.get_menu_item())
        return menu

    def get_menu_item(self):
        item = SubmenuItem(self.get_title, self.get_submenu())
        return item

    def get_todos(self):
        return self.todos

    def in_rc_loop(self):
        return self.is_in_rc_loop is True

    def get_description(self):
        desc = self.description
        if desc:
            return expand_jinja(desc)
        else:
            return None

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


class Todo(SecretYamlObject):
    yaml_tag = u'!Todo'
    hidden_fields = ['state']
    def __init__(self, id, title, description=None, post_description=None, done=None, types=None, links=None,
                 commands=None, user_input=None, depends=None, vars=None, asciidoc=None, persist_vars=None):
        self.id = id
        self.title = title
        self.description = description
        self.asciidoc = asciidoc
        self.types = types
        self.depends = depends
        self.vars = vars
        self.persist_vars = persist_vars
        self.user_input = user_input
        self.commands = commands
        self.post_description = post_description
        self.links = links
        self.state = {}

        self.set_done(done)
        if self.types:
            self.types = ensure_list(self.types)
            for t in self.types:
                if not t in ['minor', 'major', 'bugfix']:
                    sys.exit("Wrong Todo config for '%s'. Type needs to be either 'minor', 'major' or 'bugfix'" % self.id)
        if commands:
            self.commands.todo_id = self.id
            for c in commands.commands:
                c.todo_id = self.id

    @classmethod
    def from_yaml(cls, loader, node):
        fields = loader.construct_mapping(node, deep = True)
        return Todo(**fields)

    def get_vars(self):
        myvars = {}
        if self.vars:
            for k in self.vars:
                val = self.vars[k]
                if callable(val):
                    myvars[k] = expand_jinja(val(), vars=myvars)
                else:
                    myvars[k] = expand_jinja(val, vars=myvars)
        return myvars

    def set_done(self, is_done):
        if is_done:
            self.state['done_date'] = unix_time_millis(datetime.now())
            if self.persist_vars:
                for k in self.persist_vars:
                    self.state[k] = self.get_vars()[k]
        else:
            self.state.clear()
        self.state['done'] = is_done

    def applies(self, type):
        if self.types:
            return type in self.types
        return True

    def is_done(self):
        return 'done' in self.state and self.state['done'] is True

    def get_title(self):
        prefix = ""
        if self.is_done():
            prefix = "✓ "
        return expand_jinja("%s%s" % (prefix, self.title), self.get_vars_and_state())

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
            if self.user_input and not self.is_done():
                ui_list = ensure_list(self.user_input)
                for ui in ui_list:
                    ui.run(self.state)
                print()
            cmds = self.get_commands()
            if cmds:
                if not self.is_done():
                    if not cmds.logs_prefix:
                        cmds.logs_prefix = self.id
                    cmds.run()
                else:
                    print("This step is already completed. You have to first set it to 'not completed' in order to execute commands again.")
                print()
            if self.post_description:
                print("%s" % self.get_post_description())
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

    def get_state(self):
        return self.state

    def get_description(self):
        desc = self.description
        if desc:
            return expand_jinja(desc, vars=self.get_vars_and_state())
        else:
            return None

    def get_post_description(self):
        if self.post_description:
            return expand_jinja(self.post_description, vars=self.get_vars_and_state())
        else:
            return None

    def get_commands(self):
        cmds = self.commands
        return cmds

    def get_asciidoc(self):
        if self.asciidoc:
            return expand_jinja(self.asciidoc, vars=self.get_vars_and_state())
        else:
            return None

    def get_vars_and_state(self):
        d = self.get_vars().copy()
        d.update(self.get_state())
        return d

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
        print("WARNING: Expected release version %s when on branch %s, but got %s" % (
        getScriptVersion(), branch, release_version))


def get_todo_menuitem_title():
    return "Go to checklist (RC%d)" % (state.rc_number)


def get_releasing_text():
    return "Releasing Lucene/Solr %s RC%d" % (state.release_version, state.rc_number)


def get_start_new_rc_menu_title():
    return "Abort RC%d and start a new RC%d" % (state.rc_number, state.rc_number + 1)


def start_new_rc():
    state.new_rc()
    print("Started RC%d" % state.rc_number)


def reset_state():
    global state
    if ask_yes_no("Are you sure? This will erase all current progress"):
        maybe_remove_rc_from_svn()
        shutil.rmtree(os.path.join(state.config_path, state.release_version))
        state.clear()


def template(name, vars=None):
    return expand_jinja(templates[name], vars=vars)


def help():
    print(template('help'))


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
    fh.write("(_Generated by releaseWizard.py v%s ALPHA at %s_)\n\n"
             % (getScriptVersion(), datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")))
    fh.write(":numbered:\n\n")
    for group in state.todo_groups:
        if group.num_applies() == 0:
            continue
        fh.write("== %s\n\n" % group.title)
        fh.write("%s\n\n" % group.get_description())
        for todo in group.get_todos():
            if not todo.applies(state.release_type):
                continue
            fh.write("=== %s\n\n" % todo.title)
            if todo.is_done():
                fh.write("_Completed %s_\n\n" % datetime.utcfromtimestamp(todo.state['done_date'] / 1000).strftime(
                    "%Y-%m-%d %H:%M UTC"))
            if todo.get_asciidoc():
                fh.write("%s\n\n" % todo.get_asciidoc())
            else:
                desc = todo.get_description()
                if desc:
                    fh.write("%s\n\n" % desc)
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
                fh.write("%s\n\n" % cmds.get_commands_text())
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
                            fh.write("REM %s\n" % c.get_comment())
                        else:
                            fh.write("# %s\n" % c.get_comment())
                    fh.write("%s%s%s\n" % (pre, c, post))
                fh.write("----\n\n")
            if todo.post_description and not todo.get_asciidoc():
                fh.write("\n%s\n\n" % todo.get_post_description())

    fh.close()
    print("Wrote file %s" % os.path.join(state.get_release_folder(), filename_adoc))
    print("Running command 'asciidoctor %s'" % filename_adoc)
    run_follow("asciidoctor %s" % filename_adoc)
    if os.path.exists(filename_html):
        open_file(filename_html)
    else:
        print("Failed generating HTML version, please install asciidoctor")


def release_other_version():
    maybe_remove_rc_from_svn()
    os.remove(os.path.join(state.config_path, 'latest.json'))
    print("Please restart the wizard")
    sys.exit(0)


def download_keys():
    download('KEYS', "https://archive.apache.org/dist/lucene/KEYS", state.config_path)


def keys_downloaded():
    return os.path.exists(os.path.join(state.config_path, "KEYS"))


def dump_yaml():
    file = open(os.path.join(script_path, "releaseWizard.yaml"), "w")
    yaml.add_representer(str, str_presenter)
    yaml.Dumper.ignore_aliases = lambda *args : True
    dump_obj = {'templates': templates,
                'groups': state.todo_groups}
    yaml.dump(dump_obj, width=180, stream=file, sort_keys=False, default_flow_style=False)


def load_templates():
    global templates
    templates = yaml.load(open(os.path.join(script_path, "releaseWizard.yaml"), "r"), Loader=yaml.Loader).get('templates')


def parse_config():
    description = 'Script to guide a RM through the whole release process'
    parser = argparse.ArgumentParser(description=description, epilog="Go push that release!",
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--dry-run', dest='dry', action='store_true', default=False,
                        help='Do not execute any commands, but echo them instead. Display extra debug info')
    parser.add_argument('--root', metavar='PATH',
                        help='Specify different root folder than ~/.lucene-releases')
    config = parser.parse_args()

    return config


def main():
    global state
    global dry_run
    global templates

    print("Lucene/Solr releaseWizard v%s" % getScriptVersion())
    c = parse_config()
    if c.dry:
        print("Entering dry-run mode where all commands will be echoed instead of executed")
        dry_run = True

    release_root = os.path.expanduser("~/.lucene-releases")
    if c.root:
        release_root = c.root
        if not os.path.exists(release_root):
            sys.exit("Custom release root %s does not exist, please create" % release_root)
        print("Using %s as release root instead of ~/.lucene-releases" % release_root)

    try:
        load_templates()
    except Exception as e:
        sys.exit("Failed loading templates. %s" % e)

    check_prerequisites()
    state = ReleaseState(release_root, getScriptVersion())
    state.load()

    if not state.release_version:
        input_version = get_release_version()
        validate_release_version(state.branch_type, state.script_branch, input_version)
        state.set_release_version(input_version)
        state.save()

    # Smoketester requires JAVA_HOME to point to JAVA8 and JAVA11_HOME to point ot Java11
    os.environ['JAVA_HOME'] = state.get_java_home()
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
                            exit_option_text='Return',
                            screen=MyScreen())

    for todo_group in state.todo_groups:
        if todo_group.num_applies() >= 0:
            todo_menu.append_item(todo_group.get_menu_item())

    main_menu.append_item(SubmenuItem(get_todo_menuitem_title, todo_menu))
    main_menu.append_item(FunctionItem(get_start_new_rc_menu_title, start_new_rc))
    main_menu.append_item(FunctionItem('Clear and restart current RC', state.clear_rc))
    main_menu.append_item(FunctionItem("Clear all state, restart the %s release" % state.release_version, reset_state))
    main_menu.append_item(FunctionItem('Start release for a different version', release_other_version))
    main_menu.append_item(FunctionItem('Generate Asciidoc guide for this release', generate_asciidoc))
    main_menu.append_item(FunctionItem('Download gpg KEYS file for offline use', download_keys))
    # main_menu.append_item(FunctionItem('Dump YAML', dump_yaml))
    main_menu.append_item(FunctionItem('Help', help))

    main_menu.show()


sys.path.append(os.path.dirname(__file__))
current_git_root = os.path.abspath(
    os.path.join(os.path.abspath(os.path.dirname(__file__)), os.path.pardir, os.path.pardir))


def git_checkout_folder():
    return state.get_git_checkout_folder()


def tail_file(file, lines):
    bufsize = 8192
    fsize = os.stat(file).st_size
    with open(file) as f:
        if bufsize >= fsize:
            bufsize = fsize
        idx = 0
        while True:
            idx += 1
            seek_pos = fsize - bufsize * idx
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
    line = line.rstrip()
    if len(line) > width:
        line = "%s.....%s" % (line[:(width / 2 - 5)], line[-(width / 2):])
    else:
        line = "%s%s" % (line, " " * (width - len(line) + 2))
    return line


def print_line_cr(line, linenum, stdout=True, tee=False):
    if not tee:
        if not stdout:
            print("[line %s] %s" % (linenum, abbreviate_line(line, 80)), end='\r')
    else:
        if line.endswith("\r"):
            print(line.rstrip(), end='\r')
        else:
            print(line.rstrip())


def run_follow(command, cwd=None, fh=sys.stdout, tee=False):
    if not isinstance(command, list):
        command = shlex.split(command)
    process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, cwd=cwd,
                               universal_newlines=True, bufsize=0, close_fds=True)
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
                    fh.write("%s\n" % line.rstrip())
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
                    errlines.append("%s\n" % line.rstrip())
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
        for line in errlines:
            fh.write("%s\n" % line.rstrip())
            fh.flush()
    return rc


def is_windows():
    return platform.system().startswith("Win")


class Commands(SecretYamlObject):
    yaml_tag = u'!Commands'
    hidden_fields = ['todo_id']
    def __init__(self, root_folder, commands_text, commands, logs_prefix=None, run_text=None, enable_execute=None,
                 confirm_each_command=None, env=None, vars=None, todo_id=None, remove_files=None):
        self.root_folder = root_folder
        self.commands_text = commands_text
        self.vars = vars
        self.env = env
        self.run_text = run_text
        self.remove_files = remove_files
        self.todo_id = todo_id
        self.logs_prefix = logs_prefix
        self.enable_execute = enable_execute
        self.confirm_each_command = confirm_each_command
        self.commands = commands
        for c in self.commands:
            c.todo_id = todo_id

    @classmethod
    def from_yaml(cls, loader, node):
        fields = loader.construct_mapping(node, deep = True)
        return Commands(**fields)

    def run(self):
        root = self.get_root_folder()

        print(expand_jinja(self.commands_text))
        if self.env:
            for key in self.env:
                val = expand_jinja(self.env[key])
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
            print("  %s%s%s" % (pre, cmd.get_cmd(), post))
        print()
        if not self.enable_execute is False:
            if self.run_text:
                print("\n%s\n" % self.get_run_text())
            if len(commands) > 1:
                if not self.confirm_each_command is False:
                    print("You will get prompted before running each individual command.")
                else:
                    print(
                        "You will not be prompted for each command but will see the ouput of each. If one command fails the execution will stop.")
            success = True
            if ask_yes_no("Do you want me to run these commands now?"):
                if self.remove_files:
                    for f in self.get_remove_files():
                        if os.path.exists(f):
                            filefolder = "File" if os.path.isfile(f) else "Folder"
                            if ask_yes_no("%s %s already exists. Shall I remove it now?" % (filefolder, f)) and not dry_run:
                                if os.path.isdir(f):
                                    shutil.rmtree(f)
                                else:
                                    os.remove(f)
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
                    if self.confirm_each_command is False or len(commands) == 1 or ask_yes_no("Shall I run '%s' in folder '%s'" % (cmd, cwd)):
                        if self.confirm_each_command is False:
                            print("------------\nRunning '%s' in folder '%s'" % (cmd, cwd))
                        logfilename = cmd.logfile
                        logfile = None
                        if not cmd.stdout:
                            if not log_folder:
                                log_folder = os.path.join(state.get_rc_folder(), "logs")
                            elif not os.path.isabs(log_folder):
                                log_folder = os.path.join(state.get_rc_folder(), "logs", log_folder)
                            if not logfilename:
                                logfilename = "%s.log" % re.sub(r"\W", "_", cmd.get_cmd())
                            logfile = os.path.join(log_folder, "%s%s%s" % (log_prefix, folder_prefix, logfilename))
                            print("Wait until command completes... Full log in %s\n" % logfile)
                        cmd_to_run = "%s%s" % ("echo Dry run, command is: " if dry_run else "", cmd.get_cmd())
                        returncode = run_with_log_tail(cmd_to_run, cwd, logfile=logfile, tee=cmd.tee, tail_lines=25)
                        if not returncode == 0:
                            if cmd.should_fail:
                                print("Command failed, which was expected")
                                success = True
                            else:
                                print("WARN: Command %s returned with error" % cmd.get_cmd())
                                success = False
                                break
                        else:
                            if cmd.should_fail and not dry_run:
                                print("Expected command to fail, but it succeeded.")
                                success = False
                                break
            if not success:
                print("WARNING: One or more commands failed, you may want to check the logs")
            return success

    def get_root_folder(self):
        return expand_jinja(self.root_folder)

    def get_commands_text(self):
        return self.jinjaify(self.commands_text)

    def get_run_text(self):
        return self.jinjaify(self.run_text)

    def get_remove_files(self):
        return self.jinjaify(self.remove_files)

    def get_vars(self):
        myvars = {}
        if self.vars:
            for k in self.vars:
                val = self.vars[k]
                if callable(val):
                    myvars[k] = expand_jinja(val(), vars=myvars)
                else:
                    myvars[k] = expand_jinja(val, vars=myvars)
        return myvars

    def jinjaify(self, data, join=False):
        if not data:
            return None
        v = self.get_vars()
        if self.todo_id:
            v.update(state.get_todo_by_id(self.todo_id).get_vars())
        if isinstance(data, list):
            if join:
                return expand_jinja(" ".join(data), v)
            else:
                res = []
                for rf in data:
                    res.append(expand_jinja(rf, v))
                return res
        else:
            return expand_jinja(data, v)


class Command(SecretYamlObject):
    yaml_tag = u'!Command'
    hidden_fields = ['todo_id']
    def __init__(self, cmd, cwd=None, stdout=None, logfile=None, tee=None, comment=None, vars=None, todo_id=None, should_fail=None):
        self.cmd = cmd
        self.cwd = cwd
        self.comment = comment
        self.logfile = logfile
        self.vars = vars
        self.tee = tee
        self.stdout = stdout
        self.should_fail = should_fail
        self.todo_id = todo_id

    @classmethod
    def from_yaml(cls, loader, node):
        fields = loader.construct_mapping(node, deep = True)
        return Command(**fields)

    def get_comment(self):
        return self.jinjaify(self.comment)

    def get_cmd(self):
        return self.jinjaify(self.cmd, join=True)

    def get_vars(self):
        myvars = {}
        if self.vars:
            for k in self.vars:
                val = self.vars[k]
                if callable(val):
                    myvars[k] = expand_jinja(val(), vars=myvars)
                else:
                    myvars[k] = expand_jinja(val, vars=myvars)
        return myvars

    def __str__(self):
        return self.get_cmd()

    def jinjaify(self, data, join=False):
        v = self.get_vars()
        if self.todo_id:
            v.update(state.get_todo_by_id(self.todo_id).get_vars())
        if isinstance(data, list):
            if join:
                return expand_jinja(" ".join(data), v)
            else:
                res = []
                for rf in data:
                    res.append(expand_jinja(rf, v))
                return res
        else:
            return expand_jinja(data, v)


class UserInput(SecretYamlObject):
    yaml_tag = u'!UserInput'

    def __init__(self, name, prompt, type=None):
        self.type = type
        self.prompt = prompt
        self.name = name

    @classmethod
    def from_yaml(cls, loader, node):
        fields = loader.construct_mapping(node, deep = True)
        return UserInput(**fields)

    def run(self, dict=None):
        correct = False
        while not correct:
            try:
                result = str(input("%s : " % self.prompt))
                if self.type and self.type == 'int':
                    result = int(result)
                correct = True
            except Exception as e:
                print("Incorrect input: %s, try again" % e)
                continue
            if dict:
                dict[self.name] = result
            return result


class MyScreen(Screen):
    def clear(self):
        return


def vote_close_72h_date():
    dow = datetime.utcnow().weekday()
    if dow == 6:  # Sun
        days_to_add = 1
    elif dow in [2, 3, 4, 5]:  # Wed, Thu, Fri, Sat
        days_to_add = 2
    else:
        days_to_add = 0
    return (datetime.utcnow() + timedelta(hours=73) + timedelta(days=days_to_add))


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('Keyboard interrupt...exiting')

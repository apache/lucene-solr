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

# This script is the Release Manager's best friend, ensuring all details of a release are handled correctly.
# It will walk you through the steps of the release process, asking for decisions or input along the way.
# CAUTION: You still need to use your head! Please read the HELP section in the main menu.
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

import argparse
import copy
import fcntl
import json
import os
import platform
import re
import shlex
import shutil
import subprocess
import sys
import textwrap
import time
import urllib
from collections import OrderedDict
from datetime import datetime
from datetime import timedelta

try:
    import holidays
    import yaml
    from ics import Calendar, Event
    from jinja2 import Environment
except:
    print("You lack some of the module dependencies to run this script.")
    print("Please run 'pip3 install -r requirements.txt' and try again.")
    sys.exit(1)

import scriptutil
from consolemenu import ConsoleMenu
from consolemenu.items import FunctionItem, SubmenuItem, ExitItem
from consolemenu.screen import Screen
from scriptutil import BranchType, Version, check_ant, download, run

# Solr-to-Java version mapping
java_versions = {6: 8, 7: 8, 8: 8, 9: 11, 10: 11}
editor = None

# Edit this to add other global jinja2 variables or filters
def expand_jinja(text, vars=None):
    global_vars = OrderedDict({
        'script_version': state.script_version,
        'release_version': state.release_version,
        'release_version_underscore': state.release_version.replace('.', '_'),
        'release_date': state.get_release_date(),
        'ivy2_folder': os.path.expanduser("~/.ivy2/"),
        'config_path': state.config_path,
        'rc_number': state.rc_number,
        'script_branch': state.script_branch,
        'release_folder': state.get_release_folder(),
        'git_checkout_folder': state.get_git_checkout_folder(),
        'lucene_git_checkout_folder': state.get_lucene_git_checkout_folder(),
        'solr_git_checkout_folder': state.get_solr_git_checkout_folder(),
        'git_lucene_website_folder': state.get_lucene_website_git_folder(),
        'git_solr_website_folder': state.get_solr_website_git_folder(),
        'dist_url_base': 'https://dist.apache.org/repos/dist/dev/lucene',
        'm2_repository_url': 'https://repository.apache.org/service/local/staging/deploy/maven2',
        'dist_file_path': state.get_dist_folder(),
        'rc_folder': state.get_rc_folder(),
        'base_branch': state.get_base_branch_name(),
        'release_branch': state.release_branch,
        'stable_branch': state.get_stable_branch_name(),
        'minor_branch': state.get_minor_branch_name(),
        'release_type': state.release_type,
        'is_feature_release': state.release_type in ['minor', 'major'],
        'release_version_major': state.release_version_major,
        'release_version_minor': state.release_version_minor,
        'release_version_bugfix': state.release_version_bugfix,
        'release_version_refguide': state.get_refguide_release() ,
        'state': state,
        'gpg_key': state.get_gpg_key(),
        'epoch': unix_time_millis(datetime.utcnow()),
        'get_next_version': state.get_next_version(),
        'current_git_rev': state.get_current_git_rev(),
        'keys_downloaded': keys_downloaded(),
        'editor': get_editor(),
        'rename_cmd': 'ren' if is_windows() else 'mv',
        'vote_close_72h': vote_close_72h_date().strftime("%Y-%m-%d %H:00 UTC"),
        'vote_close_72h_epoch': unix_time_millis(vote_close_72h_date()),
        'vote_close_72h_holidays': vote_close_72h_holidays(),
        'lucene_news_file': lucene_news_file,
        'solr_news_file': solr_news_file,
        'load_lines': load_lines,
        'set_java_home': set_java_home,
        'latest_version': state.get_latest_version(),
        'latest_lts_version': state.get_latest_lts_version(),
        'master_version': state.get_master_version(),
        'mirrored_versions': state.get_mirrored_versions(),
        'mirrored_versions_to_delete': state.get_mirrored_versions_to_delete(),
        'home': os.path.expanduser("~")
    })
    global_vars.update(state.get_todo_states())
    if vars:
        global_vars.update(vars)

    filled = replace_templates(text)

    try:
        env = Environment(lstrip_blocks=True, keep_trailing_newline=False, trim_blocks=True)
        env.filters['path_join'] = lambda paths: os.path.join(*paths)
        env.filters['expanduser'] = lambda path: os.path.expanduser(path)
        env.filters['formatdate'] = lambda date: (datetime.strftime(date, "%-d %B %Y") if date else "<date>" )
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
    topLevelDir = os.path.join(os.path.abspath("%s/" % script_path), os.path.pardir, os.path.pardir)
    reBaseVersion = re.compile(r'version\.base\s*=\s*(\d+\.\d+\.\d+)')
    return reBaseVersion.search(open('%s/lucene/version.properties' % topLevelDir).read()).group(1)


def get_editor():
    global editor
    if editor is None:
      if 'EDITOR' in os.environ:
          if os.environ['EDITOR'] in ['vi', 'vim', 'nano', 'pico', 'emacs']:
              print("WARNING: You have EDITOR set to %s, which will not work when launched from this tool. Please use an editor that launches a separate window/process" % os.environ['EDITOR'])
          editor = os.environ['EDITOR']
      elif is_windows():
          editor = 'notepad.exe'
      elif is_mac():
          editor = 'open -a TextEdit'
      else:
          sys.exit("On Linux you have to set EDITOR variable to a command that will start an editor in its own window")
    return editor


def check_prerequisites(todo=None):
    if sys.version_info < (3, 4):
        sys.exit("Script requires Python v3.4 or later")
    try:
        gpg_ver = run("gpg --version").splitlines()[0]
    except:
        sys.exit("You will need gpg installed")
    if not check_ant().startswith('1.8'):
        print("WARNING: This script will work best with ant 1.8. The script buildAndPushRelease.py may have problems with PGP password input under ant 1.10")
    if not 'GPG_TTY' in os.environ:
        print("WARNING: GPG_TTY environment variable is not set, GPG signing may not work correctly (try 'export GPG_TTY=$(tty)'")
    if not 'JAVA8_HOME' in os.environ or not 'JAVA11_HOME' in os.environ:
        sys.exit("Please set environment variables JAVA8_HOME and JAVA11_HOME")
    try:
        asciidoc_ver = run("asciidoctor -V").splitlines()[0]
    except:
        asciidoc_ver = ""
        print("WARNING: In order to export asciidoc version to HTML, you will need asciidoctor installed")
    try:
        git_ver = run("git --version").splitlines()[0]
    except:
        sys.exit("You will need git installed")
    try:
        svn_ver = run("svn --version").splitlines()[0]
    except:
        sys.exit("You will need svn installed")
    if not 'EDITOR' in os.environ:
        print("WARNING: Environment variable $EDITOR not set, using %s" % get_editor())

    if todo:
        print("%s\n%s\n%s\n" % (gpg_ver, asciidoc_ver, git_ver))
    return True


epoch = datetime.utcfromtimestamp(0)


def unix_time_millis(dt):
    return int((dt - epoch).total_seconds() * 1000.0)


def bootstrap_todos(todo_list):
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
    def __init__(self, config_path, release_version, script_version):
        self.script_version = script_version
        self.config_path = config_path
        self.todo_groups = None
        self.todos = None
        self.latest_version = None
        self.previous_rcs = {}
        self.rc_number = 1
        self.start_date = unix_time_millis(datetime.utcnow())
        self.script_branch = run("git rev-parse --abbrev-ref HEAD").strip()
        self.mirrored_versions = None
        try:
            self.script_branch_type = scriptutil.find_branch_type()
        except:
            print("WARNING: This script shold (ideally) run from the release branch, not a feature branch (%s)" % self.script_branch)
            self.script_branch_type = 'feature'
        self.set_release_version(release_version)

    def set_release_version(self, version):
        self.validate_release_version(self.script_branch_type, self.script_branch, version)
        self.release_version = version
        v = Version.parse(version)
        self.release_version_major = v.major
        self.release_version_minor = v.minor
        self.release_version_bugfix = v.bugfix
        self.release_branch = "branch_%s_%s" % (v.major, v.minor)
        if v.is_major_release():
            self.release_type = 'major'
        elif v.is_minor_release():
            self.release_type = 'minor'
        else:
            self.release_type = 'bugfix'

    def is_released(self):
        return self.get_todo_by_id('announce_lucene').is_done()

    def get_gpg_key(self):
        gpg_task = self.get_todo_by_id('gpg')
        if gpg_task.is_done():
            return gpg_task.get_state()['gpg_key']
        else:
            return None

    def get_release_date(self):
        publish_task = self.get_todo_by_id('publish_maven')
        if publish_task.is_done():
            return unix_to_datetime(publish_task.get_state()['done_date'])
        else:
            return None

    def get_release_date_iso(self):
        release_date = self.get_release_date()
        if release_date is None:
            return "yyyy-mm-dd"
        else:
            return release_date.isoformat()[:10]

    def get_latest_version(self):
        if self.latest_version is None:
            versions = self.get_mirrored_versions()
            latest = versions[0]
            for ver in versions:
                if Version.parse(ver).gt(Version.parse(latest)):
                    latest = ver
            self.latest_version = latest
            self.save()
        return state.latest_version

    def get_mirrored_versions(self):
        if state.mirrored_versions is None:
            releases_str = load("https://projects.apache.org/json/foundation/releases.json", "utf-8")
            releases = json.loads(releases_str)['lucene']
            vers = [ r for r in list(map(lambda y: y[7:], filter(lambda x: x.startswith('lucene-'), list(releases.keys())))) ]
            state.mirrored_versions = vers
        return state.mirrored_versions

    def get_mirrored_versions_to_delete(self):
        versions = self.get_mirrored_versions()
        to_keep = versions
        if state.release_type == 'major':
          to_keep = [self.release_version, self.get_latest_version()]
        if state.release_type == 'minor':
          to_keep = [self.release_version, self.get_latest_lts_version()]
        if state.release_type == 'bugfix':
          if Version.parse(state.release_version).major == Version.parse(state.get_latest_version()).major:
            to_keep = [self.release_version, self.get_latest_lts_version()]
          elif Version.parse(state.release_version).major == Version.parse(state.get_latest_lts_version()).major:
            to_keep = [self.get_latest_version(), self.release_version]
          else:
            raise Exception("Release version %s must have same major version as current minor or lts release")
        return [ver for ver in versions if ver not in to_keep]

    def get_master_version(self):
        v = Version.parse(self.get_latest_version())
        return "%s.%s.%s" % (v.major + 1, 0, 0)

    def get_latest_lts_version(self):
        versions = self.get_mirrored_versions()
        latest = self.get_latest_version()
        lts_prefix = "%s." % (Version.parse(latest).major - 1)
        lts_versions = list(filter(lambda x: x.startswith(lts_prefix), versions))
        latest_lts = lts_versions[0]
        for ver in lts_versions:
            if Version.parse(ver).gt(Version.parse(latest_lts)):
                latest_lts = ver
        return latest_lts

    def validate_release_version(self, branch_type, branch, release_version):
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

    def get_base_branch_name(self):
        v = Version.parse(self.release_version)
        if v.is_major_release():
            return 'master'
        elif v.is_minor_release():
            return self.get_stable_branch_name()
        elif v.major == Version.parse(self.get_latest_version()).major:
            return self.get_minor_branch_name()
        else:
            return self.release_branch

    def clear_rc(self):
        if ask_yes_no("Are you sure? This will clear and restart RC%s" % self.rc_number):
            maybe_remove_rc_from_svn()
            dict = {}
            for g in list(filter(lambda x: x.in_rc_loop(), self.todo_groups)):
                for t in g.get_todos():
                    t.clear()
            print("Cleared RC TODO state")
            try:
                shutil.rmtree(self.get_rc_folder())
                print("Cleared folder %s" % self.get_rc_folder())
            except Exception as e:
                print("WARN: Failed to clear %s, please do it manually with higher privileges" % self.get_rc_folder())
            self.save()

    def new_rc(self):
        if ask_yes_no("Are you sure? This will abort current RC"):
            maybe_remove_rc_from_svn()
            dict = {}
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
        dict = {
            'script_version': self.script_version,
            'release_version': self.release_version,
            'start_date': self.start_date,
            'rc_number': self.rc_number,
            'script_branch': self.script_branch,
            'todos': tmp_todos,
            'previous_rcs': self.previous_rcs
        }
        if self.latest_version:
            dict['latest_version'] = self.latest_version
        return dict

    def restore_from_dict(self, dict):
        self.script_version = dict['script_version']
        assert dict['release_version'] == self.release_version
        if 'start_date' in dict:
            self.start_date = dict['start_date']
        if 'latest_version' in dict:
            self.latest_version = dict['latest_version']
        else:
            self.latest_version = None
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
        if os.path.exists(os.path.join(self.config_path, self.release_version, 'state.yaml')):
            state_file = os.path.join(self.config_path, self.release_version, 'state.yaml')
            with open(state_file, 'r') as fp:
                try:
                    dict = yaml.load(fp, Loader=yaml.Loader)
                    self.restore_from_dict(dict)
                    print("Loaded state from %s" % state_file)
                except Exception as e:
                    print("Failed to load state from %s: %s" % (state_file, e))

    def save(self):
        print("Saving")
        if not os.path.exists(os.path.join(self.config_path, self.release_version)):
            print("Creating folder %s" % os.path.join(self.config_path, self.release_version))
            os.makedirs(os.path.join(self.config_path, self.release_version))

        with open(os.path.join(self.config_path, self.release_version, 'state.yaml'), 'w') as fp:
            yaml.dump(self.to_dict(), fp, sort_keys=False, default_flow_style=False)

    def clear(self):
        self.previous_rcs = {}
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
        return folder

    def get_git_checkout_folder(self):
        folder = os.path.join(self.get_release_folder(), "lucene-solr")
        return folder

    def get_lucene_git_checkout_folder(self):
        folder = os.path.join(self.get_release_folder(), "lucene")
        return folder

    def get_solr_git_checkout_folder(self):
        folder = os.path.join(self.get_release_folder(), "solr")
        return folder

    def get_lucene_website_git_folder(self):
        folder = os.path.join(self.get_release_folder(), "lucene-site")
        return folder

    def get_solr_website_git_folder(self):
        folder = os.path.join(self.get_release_folder(), "solr-site")
        return folder

    def get_minor_branch_name(self):
        latest = state.get_latest_version()
        if latest is not None:
          v = Version.parse(latest)
          return "branch_%s_%s" % (v.major, v.minor)
        else:
            raise Exception("Cannot find latest version")

    def get_stable_branch_name(self):
        v = Version.parse(self.get_latest_version())
        return "branch_%sx" % v.major

    def get_next_version(self):
        if self.release_type == 'major':
            return "%s.0" % (self.release_version_major + 1)
        if self.release_type == 'minor':
            return "%s.%s" % (self.release_version_major, self.release_version_minor + 1)
        if self.release_type == 'bugfix':
            return "%s.%s.%s" % (self.release_version_major, self.release_version_minor, self.release_version_bugfix + 1)

    def get_refguide_release(self):
        return "%s_%s" % (self.release_version_major, self.release_version_minor)

    def get_java_home(self):
        return self.get_java_home_for_version(self.release_version)

    def get_java_home_for_version(self, version):
        v = Version.parse(version)
        java_ver = java_versions[v.major]
        java_home_var = "JAVA%s_HOME" % java_ver
        if java_home_var in os.environ:
            return os.environ.get(java_home_var)
        else:
            raise Exception("Script needs environment variable %s" % java_home_var )

    def get_java_cmd_for_version(self, version):
        return os.path.join(self.get_java_home_for_version(version), "bin", "java")

    def get_java_cmd(self):
        return os.path.join(self.get_java_home(), "bin", "java")

    def get_todo_states(self):
        states = {}
        if self.todos:
            for todo_id in self.todos:
                t = self.todos[todo_id]
                states[todo_id] = copy.deepcopy(t.state)
        return states

    def init_todos(self, groups):
        self.todo_groups = groups
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
        menu = UpdatableConsoleMenu(title=self.title, subtitle=self.get_subtitle, prologue_text=self.get_description(),
                           screen=MyScreen())
        menu.exit_item = CustomExitItem("Return")
        for todo in self.get_todos():
            if todo.applies(state.release_type):
                menu.append_item(todo.get_menu_item())
        return menu

    def get_menu_item(self):
        item = UpdatableSubmenuItem(self.get_title, self.get_submenu())
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
                 commands=None, user_input=None, depends=None, vars=None, asciidoc=None, persist_vars=None,
                 function=None):
        self.id = id
        self.title = title
        self.description = description
        self.asciidoc = asciidoc
        self.types = types
        self.depends = depends
        self.vars = vars
        self.persist_vars = persist_vars
        self.function = function
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
            self.state['done_date'] = unix_time_millis(datetime.utcnow())
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
            try:
                if self.function and not self.is_done():
                    if not eval(self.function)(self):
                        return
            except Exception as e:
                print("Function call to %s for todo %s failed: %s" % (self.function, self.id, e))
                raise e
            if self.user_input and not self.is_done():
                ui_list = ensure_list(self.user_input)
                for ui in ui_list:
                    ui.run(self.state)
                print()
            if self.links:
                print("\nLinks:\n")
                for link in self.links:
                    print("- %s" % expand_jinja(link, self.get_vars_and_state()))
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
            todostate = self.get_state()
            if self.is_done() and len(todostate) > 2:
                print("Variables registered\n")
                for k in todostate:
                    if k == 'done' or k == 'done_date':
                        continue
                    print("* %s = %s" % (k, todostate[k]))
                print()
            completed = ask_yes_no("Mark task '%s' as completed?" % self.get_title())
            self.set_done(completed)
            state.save()
        except Exception as e:
            print("ERROR while executing todo %s (%s)" % (self.get_title(), e))

    def get_menu_item(self):
        return UpdatableFunctionItem(self.get_title, self.display_and_confirm)

    def clone(self):
        clone = Todo(self.id, self.title, description=self.description)
        clone.state = copy.deepcopy(self.state)
        return clone

    def clear(self):
        self.state.clear()
        self.set_done(False)

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
    pause()


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


def expand_multiline(cmd_txt, indent=0):
    return re.sub(r'  +', " %s\n    %s" % (Commands.cmd_continuation_char, " "*indent), cmd_txt)


def unix_to_datetime(unix_stamp):
    return datetime.utcfromtimestamp(unix_stamp / 1000)


def generate_asciidoc():
    base_filename = os.path.join(state.get_release_folder(),
                                 "lucene_solr_release_%s"
                                 % (state.release_version.replace("\.", "_")))

    filename_adoc = "%s.adoc" % base_filename
    filename_html = "%s.html" % base_filename
    fh = open(filename_adoc, "w")

    fh.write("= Lucene/Solr Release %s\n\n" % state.release_version)
    fh.write("(_Generated by releaseWizard.py v%s at %s_)\n\n"
             % (getScriptVersion(), datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")))
    fh.write(":numbered:\n\n")
    fh.write("%s\n\n" % template('help'))
    for group in state.todo_groups:
        if group.num_applies() == 0:
            continue
        fh.write("== %s\n\n" % group.get_title())
        fh.write("%s\n\n" % group.get_description())
        for todo in group.get_todos():
            if not todo.applies(state.release_type):
                continue
            fh.write("=== %s\n\n" % todo.get_title())
            if todo.is_done():
                fh.write("_Completed %s_\n\n" % unix_to_datetime(todo.state['done_date']).strftime(
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
            cmds = todo.get_commands()
            if cmds:
                if cmds.commands_text:
                    fh.write("%s\n\n" % cmds.get_commands_text())
                fh.write("[source,sh]\n----\n")
                if cmds.env:
                    for key in cmds.env:
                        val = cmds.env[key]
                        if is_windows():
                            fh.write("SET %s=%s\n" % (key, val))
                        else:
                            fh.write("export %s=%s\n" % (key, val))
                fh.write(abbreviate_homedir("cd %s\n" % cmds.get_root_folder()))
                cmds2 = ensure_list(cmds.commands)
                for c in cmds2:
                    for line in c.display_cmd():
                        fh.write("%s\n" % line)
                fh.write("----\n\n")
            if todo.post_description and not todo.get_asciidoc():
                fh.write("\n%s\n\n" % todo.get_post_description())
            if todo.links:
                fh.write("Links:\n\n")
                for l in todo.links:
                    fh.write("* %s\n" % expand_jinja(l))
                fh.write("\n")

    fh.close()
    print("Wrote file %s" % os.path.join(state.get_release_folder(), filename_adoc))
    print("Running command 'asciidoctor %s'" % filename_adoc)
    run_follow("asciidoctor %s" % filename_adoc)
    if os.path.exists(filename_html):
        open_file(filename_html)
    else:
        print("Failed generating HTML version, please install asciidoctor")
    pause()


def load_rc():
    lucenerc = os.path.expanduser("~/.lucenerc")
    try:
        with open(lucenerc, 'r') as fp:
            return json.load(fp)
    except:
        return None


def store_rc(release_root, release_version=None):
    lucenerc = os.path.expanduser("~/.lucenerc")
    dict = {}
    dict['root'] = release_root
    if release_version:
        dict['release_version'] = release_version
    with open(lucenerc, "w") as fp:
        json.dump(dict, fp, indent=2)


def release_other_version():
    if not state.is_released():
        maybe_remove_rc_from_svn()
    store_rc(state.config_path, None)
    print("Please restart the wizard")
    sys.exit(0)

def file_to_string(filename):
    with open(filename, encoding='utf8') as f:
        return f.read().strip()

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


def parse_config():
    description = 'Script to guide a RM through the whole release process'
    parser = argparse.ArgumentParser(description=description, epilog="Go push that release!",
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--dry-run', dest='dry', action='store_true', default=False,
                        help='Do not execute any commands, but echo them instead. Display extra debug info')
    parser.add_argument('--init', action='store_true', default=False,
                        help='Re-initialize root and version')
    config = parser.parse_args()

    return config


def load(urlString, encoding="utf-8"):
    try:
        content = urllib.request.urlopen(urlString).read().decode(encoding)
    except Exception as e:
        print('Retrying download of url %s after exception: %s' % (urlString, e))
        content = urllib.request.urlopen(urlString).read().decode(encoding)
    return content


def configure_pgp(gpg_todo):
    print("Based on your Apache ID we'll lookup your key online\n"
          "and through this complete the 'gpg' prerequisite task.\n")
    gpg_state = gpg_todo.get_state()
    id = str(input("Please enter your Apache id: (ENTER=skip) "))
    if id.strip() == '':
        return False
    key_url = "https://home.apache.org/keys/committer/%s.asc" % id.strip()
    committer_key = load(key_url)
    lines = committer_key.splitlines()
    keyid_linenum = None
    for idx, line in enumerate(lines):
        if line == 'ASF ID: %s' % id:
            keyid_linenum = idx+1
            break
    if keyid_linenum:
        keyid_line = lines[keyid_linenum]
        assert keyid_line.startswith('LDAP PGP key: ')
        gpg_id = keyid_line[14:].replace(" ", "")[-8:]
        print("Found gpg key id %s on file at Apache (%s)" % (gpg_id, key_url))
    else:
        print(textwrap.dedent("""\
            Could not find your GPG key from Apache servers.
            Please make sure you have registered your key ID in
            id.apache.org, see links for more info."""))
        gpg_id = str(input("Enter your key ID manually, 8 last characters (ENTER=skip): "))
        if gpg_id.strip() == '':
            return False
        elif len(gpg_id) != 8:
            print("gpg id must be the last 8 characters of your key id")
        gpg_id = gpg_id.upper()
    try:
        res = run("gpg --list-secret-keys %s" % gpg_id)
        print("Found key %s on your private gpg keychain" % gpg_id)
        # Check rsa and key length >= 4096
        match = re.search(r'^sec +((rsa|dsa)(\d{4})) ', res)
        type = "(unknown)"
        length = -1
        if match:
            type = match.group(2)
            length = int(match.group(3))
        else:
            match = re.search(r'^sec +((\d{4})([DR])/.*?) ', res)
            if match:
                type = 'rsa' if match.group(3) == 'R' else 'dsa'
                length = int(match.group(2))
            else:
                print("Could not determine type and key size for your key")
                print("%s" % res)
                if not ask_yes_no("Is your key of type RSA and size >= 2048 (ideally 4096)? "):
                    print("Sorry, please generate a new key, add to KEYS and register with id.apache.org")
                    return False
        if not type == 'rsa':
            print("We strongly recommend RSA type key, your is '%s'. Consider generating a new key." % type.upper())
        if length < 2048:
            print("Your key has key length of %s. Cannot use < 2048, please generate a new key before doing the release" % length)
            return False
        if length < 4096:
            print("Your key length is < 4096, Please generate a stronger key.")
            print("Alternatively, follow instructions in http://www.apache.org/dev/release-signing.html#note")
            if not ask_yes_no("Have you configured your gpg to avoid SHA-1?"):
                print("Please either generate a strong key or reconfigure your client")
                return False
        print("Validated that your key is of type RSA and has a length >= 2048 (%s)" % length)
    except:
        print(textwrap.dedent("""\
            Key not found on your private gpg keychain. In order to sign the release you'll
            need to fix this, then try again"""))
        return False
    try:
        lines = run("gpg --check-signatures %s" % gpg_id).splitlines()
        sigs = 0
        apache_sigs = 0
        for line in lines:
            if line.startswith("sig") and not gpg_id in line:
                sigs += 1
                if '@apache.org' in line:
                    apache_sigs += 1
        print("Your key has %s signatures, of which %s are by committers (@apache.org address)" % (sigs, apache_sigs))
        if apache_sigs < 1:
            print(textwrap.dedent("""\
                Your key is not signed by any other committer. 
                Please review http://www.apache.org/dev/openpgp.html#apache-wot
                and make sure to get your key signed until next time.
                You may want to run 'gpg --refresh-keys' to refresh your keychain."""))
        uses_apacheid = is_code_signing_key = False
        for line in lines:
            if line.startswith("uid") and "%s@apache" % id in line:
                uses_apacheid = True
                if 'CODE SIGNING KEY' in line.upper():
                    is_code_signing_key = True
        if not uses_apacheid:
            print("WARNING: Your key should use your apache-id email address, see http://www.apache.org/dev/release-signing.html#user-id")
        if not is_code_signing_key:
            print("WARNING: You code signing key should be labeled 'CODE SIGNING KEY', see http://www.apache.org/dev/release-signing.html#key-comment")
    except Exception as e:
        print("Could not check signatures of your key: %s" % e)

    download_keys()
    keys_text = file_to_string(os.path.join(state.config_path, "KEYS"))
    if gpg_id in keys_text or "%s %s" % (gpg_id[:4], gpg_id[-4:]) in keys_text:
        print("Found your key ID in official KEYS file. KEYS file is not cached locally.")
    else:
        print(textwrap.dedent("""\
            Could not find your key ID in official KEYS file.
            Please make sure it is added to https://dist.apache.org/repos/dist/release/lucene/KEYS
            and committed to svn. Then re-try this initialization"""))
        if not ask_yes_no("Do you want to continue without fixing KEYS file? (not recommended) "):
            return False

    gpg_state['apache_id'] = id
    gpg_state['gpg_key'] = gpg_id
    return True


def pause(fun=None):
    if fun:
        fun()
    input("\nPress ENTER to continue...")


# Custom classes for ConsoleMenu, to make menu texts dynamic
# Needed until https://github.com/aegirhall/console-menu/pull/25 is released
# See https://pypi.org/project/console-menu/ for other docs

class UpdatableConsoleMenu(ConsoleMenu):

    def __repr__(self):
        return "%s: %s. %d items" % (self.get_title(), self.get_subtitle(), len(self.items))

    def draw(self):
        """
        Refreshes the screen and redraws the menu. Should be called whenever something changes that needs to be redrawn.
        """
        self.screen.printf(self.formatter.format(title=self.get_title(), subtitle=self.get_subtitle(), items=self.items,
                                                 prologue_text=self.get_prologue_text(), epilogue_text=self.get_epilogue_text()))

    # Getters to get text in case method reference
    def get_title(self):
        return self.title() if callable(self.title) else self.title

    def get_subtitle(self):
        return self.subtitle() if callable(self.subtitle) else self.subtitle

    def get_prologue_text(self):
        return self.prologue_text() if callable(self.prologue_text) else self.prologue_text

    def get_epilogue_text(self):
        return self.epilogue_text() if callable(self.epilogue_text) else self.epilogue_text


class UpdatableSubmenuItem(SubmenuItem):
    def __init__(self, text, submenu, menu=None, should_exit=False):
        """
        :ivar ConsoleMenu self.submenu: The submenu to be opened when this item is selected
        """
        super(SubmenuItem, self).__init__(text=text, menu=menu, should_exit=should_exit)

        self.submenu = submenu
        if menu:
            self.get_submenu().parent = menu

    def show(self, index):
        return "%2d - %s" % (index + 1, self.get_text())

    # Getters to get text in case method reference
    def get_text(self):
        return self.text() if callable(self.text) else self.text

    def set_menu(self, menu):
        """
        Sets the menu of this item.
        Should be used instead of directly accessing the menu attribute for this class.

        :param ConsoleMenu menu: the menu
        """
        self.menu = menu
        self.get_submenu().parent = menu

    def action(self):
        """
        This class overrides this method
        """
        self.get_submenu().start()

    def clean_up(self):
        """
        This class overrides this method
        """
        self.get_submenu().join()
        self.menu.clear_screen()
        self.menu.resume()

    def get_return(self):
        """
        :return: The returned value in the submenu
        """
        return self.get_submenu().returned_value

    def get_submenu(self):
        """
        We unwrap the submenu variable in case it is a reference to a method that returns a submenu
        """
        return self.submenu if not callable(self.submenu) else self.submenu()


class UpdatableFunctionItem(FunctionItem):
    def show(self, index):
        return "%2d - %s" % (index + 1, self.get_text())

    # Getters to get text in case method reference
    def get_text(self):
        return self.text() if callable(self.text) else self.text


class MyScreen(Screen):
    def clear(self):
        return


class CustomExitItem(ExitItem):
    def show(self, index):
        return super(ExitItem, self).show(index)

    def get_return(self):
        return ""


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
    if not load_rc() or c.init:
        print("Initializing")
        dir_ok = False
        root = str(input("Choose root folder: [~/.lucene-releases] "))
        if os.path.exists(root) and (not os.path.isdir(root) or not os.access(root, os.W_OK)):
            sys.exit("Root %s exists but is not a directory or is not writable" % root)
        if not root == '':
            if root.startswith("~/"):
                release_root = os.path.expanduser(root)
            else:
                release_root = os.path.abspath(root)
        if not os.path.exists(release_root):
            try:
                print("Creating release root %s" % release_root)
                os.makedirs(release_root)
            except Exception as e:
                sys.exit("Error while creating %s: %s" % (release_root, e))
        release_version = get_release_version()
    else:
        conf = load_rc()
        release_root = conf['root']
        if 'release_version' in conf:
            release_version = conf['release_version']
        else:
            release_version = get_release_version()
    store_rc(release_root, release_version)

    check_prerequisites()

    try:
        y = yaml.load(open(os.path.join(script_path, "releaseWizard.yaml"), "r"), Loader=yaml.Loader)
        templates = y.get('templates')
        todo_list = y.get('groups')
        state = ReleaseState(release_root, release_version, getScriptVersion())
        state.init_todos(bootstrap_todos(todo_list))
        state.load()
    except Exception as e:
        sys.exit("Failed initializing. %s" % e)

    state.save()

    # Smoketester requires JAVA_HOME to point to JAVA8 and JAVA11_HOME to point ot Java11
    os.environ['JAVA_HOME'] = state.get_java_home()
    os.environ['JAVACMD'] = state.get_java_cmd()

    global lucene_news_file
    global solr_news_file
    lucene_news_file = os.path.join(state.get_lucene_website_git_folder(), 'content', 'core', 'core_news',
      "%s-%s-available.md" % (state.get_release_date_iso(), state.release_version.replace(".", "-")))
    solr_news_file = os.path.join(state.get_solr_website_git_folder(), 'content', 'solr', 'solr_news',
      "%s-%s-available.md" % (state.get_release_date_iso(), state.release_version.replace(".", "-")))

    main_menu = UpdatableConsoleMenu(title="Lucene/Solr ReleaseWizard",
                            subtitle=get_releasing_text,
                            prologue_text="Welcome to the release wizard. From here you can manage the process including creating new RCs. "
                                          "All changes are persisted, so you can exit any time and continue later. Make sure to read the Help section.",
                            epilogue_text="® 2020 The Lucene/Solr project. Licensed under the Apache License 2.0\nScript version v%s)" % getScriptVersion(),
                            screen=MyScreen())

    todo_menu = UpdatableConsoleMenu(title=get_releasing_text,
                            subtitle=get_subtitle,
                            prologue_text=None,
                            epilogue_text=None,
                            screen=MyScreen())
    todo_menu.exit_item = CustomExitItem("Return")

    for todo_group in state.todo_groups:
        if todo_group.num_applies() >= 0:
            menu_item = todo_group.get_menu_item()
            menu_item.set_menu(todo_menu)
            todo_menu.append_item(menu_item)

    main_menu.append_item(UpdatableSubmenuItem(get_todo_menuitem_title, todo_menu, menu=main_menu))
    main_menu.append_item(UpdatableFunctionItem(get_start_new_rc_menu_title, start_new_rc))
    main_menu.append_item(UpdatableFunctionItem('Clear and restart current RC', state.clear_rc))
    main_menu.append_item(UpdatableFunctionItem("Clear all state, restart the %s release" % state.release_version, reset_state))
    main_menu.append_item(UpdatableFunctionItem('Start release for a different version', release_other_version))
    main_menu.append_item(UpdatableFunctionItem('Generate Asciidoc guide for this release', generate_asciidoc))
    # main_menu.append_item(UpdatableFunctionItem('Dump YAML', dump_yaml))
    main_menu.append_item(UpdatableFunctionItem('Help', help))

    main_menu.show()


sys.path.append(os.path.dirname(__file__))
current_git_root = os.path.abspath(
    os.path.join(os.path.abspath(os.path.dirname(__file__)), os.path.pardir, os.path.pardir))

dry_run = False

major_minor = ['major', 'minor']
script_path = os.path.dirname(os.path.realpath(__file__))
os.chdir(script_path)


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


def run_with_log_tail(command, cwd, logfile=None, tail_lines=10, tee=False, live=False, shell=None):
    fh = sys.stdout
    if logfile:
        logdir = os.path.dirname(logfile)
        if not os.path.exists(logdir):
            os.makedirs(logdir)
        fh = open(logfile, 'w')
    rc = run_follow(command, cwd, fh=fh, tee=tee, live=live, shell=shell)
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


def run_follow(command, cwd=None, fh=sys.stdout, tee=False, live=False, shell=None):
    doShell = '&&' in command or '&' in command or shell is not None
    if not doShell and not isinstance(command, list):
        command = shlex.split(command)
    process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, cwd=cwd,
                               universal_newlines=True, bufsize=0, close_fds=True, shell=doShell)
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
                if live:
                    chars = process.stdout.read()
                    if chars == '' and process.poll() is not None:
                        endstdout = True
                    else:
                        fh.write(chars)
                        fh.flush()
                        if '\n' in chars:
                            lines_written += 1
                else:
                    line = process.stdout.readline()
                    if line == '' and process.poll() is not None:
                        endstdout = True
                    else:
                        fh.write("%s\n" % line.rstrip())
                        fh.flush()
                        lines_written += 1
                        print_line_cr(line, lines_written, stdout=(fh == sys.stdout), tee=tee)

            except Exception as ioe:
                pass
        if not endstderr:
            try:
                if live:
                    chars = process.stderr.read()
                    if chars == '' and process.poll() is not None:
                        endstderr = True
                    else:
                        fh.write(chars)
                        fh.flush()
                        if '\n' in chars:
                            lines_written += 1
                else:
                    line = process.stderr.readline()
                    if line == '' and process.poll() is not None:
                        endstderr = True
                    else:
                        errlines.append("%s\n" % line.rstrip())
                        lines_written += 1
                        print_line_cr(line, lines_written, stdout=(fh == sys.stdout), tee=tee)
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

def is_mac():
    return platform.system().startswith("Darwin")

def is_linux():
    return platform.system().startswith("Linux")

class Commands(SecretYamlObject):
    yaml_tag = u'!Commands'
    hidden_fields = ['todo_id']
    cmd_continuation_char = "^" if is_windows() else "\\"
    def __init__(self, root_folder, commands_text=None, commands=None, logs_prefix=None, run_text=None, enable_execute=None,
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

        if self.commands_text:
            print(self.get_commands_text())
        if self.env:
            for key in self.env:
                val = self.jinjaify(self.env[key])
                os.environ[key] = val
                if is_windows():
                    print("\n  SET %s=%s" % (key, val))
                else:
                    print("\n  export %s=%s" % (key, val))
        print(abbreviate_homedir("\n  cd %s" % root))
        commands = ensure_list(self.commands)
        for cmd in commands:
            for line in cmd.display_cmd():
                print("  %s" % line)
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
                    for _f in ensure_list(self.get_remove_files()):
                        f = os.path.join(root, _f)
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
                        cmd_to_run = "%s%s" % ("echo Dry run, command is: " if dry_run else "", cmd.get_cmd())
                        if cmd.redirect:
                            try:
                                out = run(cmd_to_run, cwd=cwd)
                                mode = 'a' if cmd.redirect_append is True else 'w'
                                with open(os.path.join(root, cwd, cmd.get_redirect()), mode) as outfile:
                                    outfile.write(out)
                                    outfile.flush()
                                print("Wrote %s bytes to redirect file %s" % (len(out), cmd.get_redirect()))
                            except Exception as e:
                                print("Command %s failed: %s" % (cmd_to_run, e))
                                success = False
                                break
                        else:
                            if not cmd.stdout:
                                if not log_folder:
                                    log_folder = os.path.join(state.get_rc_folder(), "logs")
                                elif not os.path.isabs(log_folder):
                                    log_folder = os.path.join(state.get_rc_folder(), "logs", log_folder)
                                if not logfilename:
                                    logfilename = "%s.log" % re.sub(r"\W", "_", cmd.get_cmd())
                                logfile = os.path.join(log_folder, "%s%s%s" % (log_prefix, folder_prefix, logfilename))
                                if cmd.tee:
                                    print("Output of command will be printed (logfile=%s)" % logfile)
                                elif cmd.live:
                                    print("Output will be shown live byte by byte")
                                    logfile = None
                                else:
                                    print("Wait until command completes... Full log in %s\n" % logfile)
                                if cmd.comment:
                                    print("# %s\n" % cmd.get_comment())
                            start_time = time.time()
                            returncode = run_with_log_tail(cmd_to_run, cwd, logfile=logfile, tee=cmd.tee, tail_lines=25,
                                                           live=cmd.live, shell=cmd.shell)
                            elapsed = time.time() - start_time
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
                                else:
                                    if elapsed > 30:
                                        print("Command completed in %s seconds" % elapsed)
            if not success:
                print("WARNING: One or more commands failed, you may want to check the logs")
            return success

    def get_root_folder(self):
        return self.jinjaify(self.root_folder)

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


def abbreviate_homedir(line):
    if is_windows():
        if 'HOME' in os.environ:
            return re.sub(r'([^/]|\b)%s' % os.path.expanduser('~'), "\\1%HOME%", line)
        elif 'USERPROFILE' in os.environ:
            return re.sub(r'([^/]|\b)%s' % os.path.expanduser('~'), "\\1%USERPROFILE%", line)
    else:
        return re.sub(r'([^/]|\b)%s' % os.path.expanduser('~'), "\\1~", line)


class Command(SecretYamlObject):
    yaml_tag = u'!Command'
    hidden_fields = ['todo_id']
    def __init__(self, cmd, cwd=None, stdout=None, logfile=None, tee=None, live=None, comment=None, vars=None,
                 todo_id=None, should_fail=None, redirect=None, redirect_append=None, shell=None):
        self.cmd = cmd
        self.cwd = cwd
        self.comment = comment
        self.logfile = logfile
        self.vars = vars
        self.tee = tee
        self.live = live
        self.stdout = stdout
        self.should_fail = should_fail
        self.shell = shell
        self.todo_id = todo_id
        self.redirect_append = redirect_append
        self.redirect = redirect
        if tee and stdout:
            self.stdout = None
            print("Command %s specifies 'tee' and 'stdout', using only 'tee'" % self.cmd)
        if live and stdout:
            self.stdout = None
            print("Command %s specifies 'live' and 'stdout', using only 'live'" % self.cmd)
        if live and tee:
            self.tee = None
            print("Command %s specifies 'tee' and 'live', using only 'live'" % self.cmd)
        if redirect and (tee or stdout or live):
            self.tee = self.stdout = self.live = None
            print("Command %s specifies 'redirect' and other out options at the same time. Using redirect only" % self.cmd)

    @classmethod
    def from_yaml(cls, loader, node):
        fields = loader.construct_mapping(node, deep = True)
        return Command(**fields)

    def get_comment(self):
        return self.jinjaify(self.comment)

    def get_redirect(self):
        return self.jinjaify(self.redirect)

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

    def display_cmd(self):
        lines = []
        pre = post = ''
        if self.comment:
            if is_windows():
                lines.append("REM %s" % self.get_comment())
            else:
                lines.append("# %s" % self.get_comment())
        if self.cwd:
            lines.append("pushd %s" % self.cwd)
        redir = "" if self.redirect is None else " %s %s" % (">" if self.redirect_append is None else ">>" , self.get_redirect())
        line = "%s%s" % (expand_multiline(self.get_cmd(), indent=2), redir)
        # Print ~ or %HOME% rather than the full expanded homedir path
        line = abbreviate_homedir(line)
        lines.append(line)
        if self.cwd:
            lines.append("popd")
        return lines

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


def create_ical(todo):
    if ask_yes_no("Do you want to add a Calendar reminder for the close vote time?"):
        c = Calendar()
        e = Event()
        e.name = "Lucene/Solr %s vote ends" % state.release_version
        e.begin = vote_close_72h_date()
        e.description = "Remember to sum up votes and continue release :)"
        c.events.add(e)
        ics_file = os.path.join(state.get_rc_folder(), 'vote_end.ics')
        with open(ics_file, 'w') as my_file:
            my_file.writelines(c)
        open_file(ics_file)
    return True


today = datetime.utcnow().date()
sundays = {(today + timedelta(days=x)): 'Sunday' for x in range(10) if (today + timedelta(days=x)).weekday() == 6}
y = datetime.utcnow().year
years = [y, y+1]
non_working = holidays.CA(years=years) + holidays.US(years=years) + holidays.England(years=years) \
              + holidays.DE(years=years) + holidays.NO(years=years) + holidays.IND(years=years) + holidays.RU(years=years)


def vote_close_72h_date():
    # Voting open at least 72 hours according to ASF policy
    return datetime.utcnow() + timedelta(hours=73)


def vote_close_72h_holidays():
    days = 0
    day_offset = -1
    holidays = []
    # Warn RM about major holidays coming up that should perhaps extend the voting deadline
    # Warning will be given for Sunday or a public holiday observed by 3 or more [CA, US, EN, DE, NO, IND, RU]
    while days < 3:
        day_offset += 1
        d = today + timedelta(days=day_offset)
        if not (d in sundays or (d in non_working and len(non_working[d]) >= 2)):
            days += 1
        else:
            if d in sundays:
                holidays.append("%s (Sunday)" % d)
            else:
                holidays.append("%s (%s)" % (d, non_working[d]))
    return holidays if len(holidays) > 0 else None


def prepare_announce_lucene(todo):
    if not os.path.exists(lucene_news_file):
        lucene_text = expand_jinja("(( template=announce_lucene ))")
        with open(lucene_news_file, 'w') as fp:
            fp.write(lucene_text)
        # print("Wrote Lucene announce draft to %s" % lucene_news_file)
    else:
        print("Draft already exist, not re-generating")
    return True

def prepare_announce_solr(todo):
    if not os.path.exists(solr_news_file):
        solr_text = expand_jinja("(( template=announce_solr ))")
        with open(solr_news_file, 'w') as fp:
            fp.write(solr_text)
        # print("Wrote Solr announce draft to %s" % solr_news_file)
    else:
        print("Draft already exist, not re-generating")
    return True


def check_artifacts_available(todo):
  try:
    cdnUrl = expand_jinja("https://dlcdn.apache.org/lucene/java/{{ release_version }}/lucene-{{ release_version }}-src.tgz.asc")
    load(cdnUrl)
    print("Found %s" % cdnUrl)
  except Exception as e:
    print("Could not fetch %s (%s)" % (cdnUrl, e))
    return False

  try:
    mavenUrl = expand_jinja("https://repo1.maven.org/maven2/org/apache/lucene/lucene-core/{{ release_version }}/lucene-core-{{ release_version }}.pom.asc")
    load(mavenUrl)
    print("Found %s" % mavenUrl)
  except Exception as e:
    print("Could not fetch %s (%s)" % (mavenUrl, e))
    return False

  return True

def set_java_home(version):
    os.environ['JAVA_HOME'] = state.get_java_home_for_version(version)
    os.environ['JAVACMD'] = state.get_java_cmd_for_version(version)


def load_lines(file, from_line=0):
    if os.path.exists(file):
        with open(file, 'r') as fp:
            return fp.readlines()[from_line:]
    else:
        return ["<Please paste the announcement text here>\n"]


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('Keyboard interrupt...exiting')

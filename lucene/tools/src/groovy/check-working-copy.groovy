/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/** Task script that is called by Ant's build.xml file:
 * Checks GIT working copy for unversioned or modified files.
 */

import org.apache.tools.ant.BuildException;
import org.apache.tools.ant.Project;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.Status;
import org.eclipse.jgit.lib.Repository;
import org.eclipse.jgit.storage.file.FileRepositoryBuilder;
import org.eclipse.jgit.errors.*;

def setProjectPropertyFromSet = { prop, set ->
  if (set) {
    properties[prop] = '* ' + set.join(properties['line.separator'] + '* ');
  }
};

try {
  task.log('Initializing working copy...', Project.MSG_INFO);
  final Repository repository = new FileRepositoryBuilder()
    .setWorkTree(project.getBaseDir())
    .setMustExist(true)
    .build();

  task.log('Checking working copy status...', Project.MSG_INFO);
  final Status status = new Git(repository).status().call();
  if (!status.isClean()) {
    final SortedSet unversioned = new TreeSet(), modified = new TreeSet();
    status.properties.each{ prop, val ->
      if (val instanceof Set) {
        if (prop in ['untracked', 'untrackedFolders', 'missing']) {
          unversioned.addAll(val);
        } else if (prop != 'ignoredNotInIndex') {
          modified.addAll(val);
        }
      }
    };
    setProjectPropertyFromSet('wc.unversioned.files', unversioned);
    setProjectPropertyFromSet('wc.modified.files', modified);
  }
} catch (RepositoryNotFoundException | NoWorkTreeException | NotSupportedException e) {
  task.log('WARNING: Development directory is not a valid GIT checkout! Disabling checks...', Project.MSG_WARN);
}

package org.apache.lucene.gradle
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

import org.eclipse.jgit.api.Git
import org.eclipse.jgit.api.Status
import org.eclipse.jgit.lib.Repository
import org.eclipse.jgit.storage.file.FileRepositoryBuilder
import org.eclipse.jgit.api.ResetCommand.ResetType
import org.eclipse.jgit.errors.*
import org.gradle.api.DefaultTask
import org.gradle.api.GradleException
import org.gradle.api.tasks.Input
import org.gradle.api.tasks.TaskAction


class PristineClean extends DefaultTask {


  PristineClean() {
    this.group = 'Build'
    this.description = "Cleans the checkout for a pristine local state (WARNING: the ultimate clean task - removes all files unknown by or ignored by git: git clean -d -x -f!)."
  }

  @TaskAction
  void check() {
    try {
      logger.info('Cleaning working copy to pristine state...')
      final Repository repository = new FileRepositoryBuilder()
        .setWorkTree(project.getRootDir())
        .setMustExist(true)
        .build();
        
      new Git(repository).clean().setCleanDirectories(true).setIgnore(false).setForce(true).call();


    } catch (RepositoryNotFoundException | NoWorkTreeException | NotSupportedException e) {
      logger.error('WARNING: Development directory is not a valid GIT checkout!')
    }
  }
}



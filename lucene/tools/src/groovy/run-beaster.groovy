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

/** Task script that is called by Ant's common-build.xml file:
 * Runs test beaster.
 */

import org.apache.tools.ant.BuildException;
import org.apache.tools.ant.BuildLogger;
import org.apache.tools.ant.Project;

int iters = (properties['beast.iters'] ?: '1') as int;
if (iters <= 1) {
  throw new BuildException("Please give -Dbeast.iters with an int value > 1.");
}

def antcall = project.createTask('antcall');
antcall.with {
  target = '-test';
  inheritAll = true;
  inheritRefs = true;
  createParam().with {
    name = "tests.isbeasting";
    value = "true";
  };
};

(1..iters).each { i ->
  task.log('Beast round: ' + i, Project.MSG_INFO);
  try {
    // disable verbose build logging:
    project.buildListeners.each { listener ->
      if (listener instanceof BuildLogger) {
        listener.messageOutputLevel = Project.MSG_WARN;
      }
    };
    
    antcall.execute();
    
  } catch (BuildException be) {
    def logFile = new File(properties["junit.output.dir"], "tests-failures.txt");
    if (logFile.exists()) {
      logFile.eachLine("UTF-8", { line ->
        task.log(line, Project.MSG_ERR);
      });
    }
    throw be;
  } finally {
    // restore build logging (unfortunately there is no way to get the original logging level (write-only property):
    project.buildListeners.each { listener ->
      if (listener instanceof BuildLogger) {
        listener.messageOutputLevel = Project.MSG_INFO;
      }
    };
  }
};
task.log('Beasting finished.', Project.MSG_INFO);

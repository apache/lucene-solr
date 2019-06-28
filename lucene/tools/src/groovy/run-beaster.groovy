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


static boolean logFailOutput(Object task, String outdir) {
  def logFile = new File(outdir, "tests-failures.txt");
  if (logFile.exists()) {
    logFile.eachLine("UTF-8", { line ->
      task.log(line, Project.MSG_ERR);
    });
  }
}

int iters = (properties['beast.iters'] ?: '1') as int;
if (iters <= 1) {
  throw new BuildException("Please give -Dbeast.iters with an int value > 1.");
}

def antcall = project.createTask('antcallback');

def junitOutDir = properties["junit.output.dir"];

def failed = false;

(1..iters).each { i ->

  def outdir = junitOutDir + "/" + i;
  task.log('Beast round ' + i + " results: " + outdir, Project.MSG_INFO);
  
  try {
    // disable verbose build logging:
    project.buildListeners.each { listener ->
      if (listener instanceof BuildLogger) {
        listener.messageOutputLevel = Project.MSG_WARN;
      }
    };
    
    new File(outdir).mkdirs();
    
    properties["junit.output.dir"] = outdir;
    
    antcall.setReturn("tests.failed");
    antcall.setTarget("-test");
    antcall.setInheritAll(true);
    antcall.setInheritRefs(true);
    
    antcall.with {

      createParam().with {
        name = "tests.isbeasting";
        value = "true";
      };
      createParam().with {
        name = "tests.timeoutSuite";
        value = "900000";
      };
      createParam().with {
        name = "junit.output.dir";
        value = outdir;
      };

    };
    
    properties["junit.output.dir"] = outdir;

    antcall.execute();

    def antcallResult = project.properties.'tests.failed' as boolean;

    if (antcallResult) {
      failed = true;
      logFailOutput(task, outdir)
    }
    
  } catch (BuildException be) {
    task.log(be.getMessage(), Project.MSG_ERR);
    logFailOutput(task, outdir)
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

// restore junit output dir
properties["junit.output.dir"] = junitOutDir;


if (failed) {
  task.log('Beasting finished with failure.', Project.MSG_INFO);
  throw new BuildException("Beasting Failed!");
} else {
  task.log('Beasting finished Successfully.', Project.MSG_INFO);
}


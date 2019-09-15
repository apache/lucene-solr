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

import org.gradle.api.tasks.StopExecutionException
import org.gradle.api.tasks.diagnostics.DependencyInsightReportTask

/**
 * An alternate to DependencyInsightReportTask that can be run from the root or an
 * aggregate project and will work against all subprojects. DependencyInsightReportTask
 * must be run in a subproject.
 */
class DepInsightReportTask extends DependencyInsightReportTask {

  public void report() {
    
    if (getConfiguration() == null) {
      throw new StopExecutionException()
    }
    
    super.report()
  }

  public void setConfiguration(String configurationName) {
    if (!project.configurations.hasProperty(configurationName)) {
      return
    }
    this.configuration = getProject().getConfigurations().getByName(configurationName);
  }
}



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

/** Task script that is called by Ant's build.xml file:
 * Checks GIT working copy for unversioned or modified files.
 */


import org.gradle.api.DefaultTask
import org.gradle.api.tasks.options.Option;
import org.gradle.api.tasks.TaskAction
import org.apache.commons.configuration2.Configuration
import org.apache.commons.configuration2.FileBasedConfiguration
import org.apache.commons.configuration2.builder.FileBasedConfigurationBuilder
import org.apache.commons.configuration2.builder.fluent.Parameters
import org.apache.commons.configuration2.convert.DefaultListDelimiterHandler
import org.apache.commons.configuration2.PropertiesConfiguration

class SetDefaultUserConfig extends DefaultTask {

  private String style

  @TaskAction
  void config() {

    int cores = Runtime.getRuntime().availableProcessors()

    File propFile = project.file(System.getProperty("user.home") + "/.gradle/gradle.properties")
    
    char delim = ','
    
    Parameters params = new Parameters();
    FileBasedConfigurationBuilder<FileBasedConfiguration> builder = new FileBasedConfigurationBuilder<FileBasedConfiguration>(PropertiesConfiguration.class)
    builder.configure(params.properties()
    .setFile(propFile)
    .setListDelimiterHandler(new DefaultListDelimiterHandler(delim)))
    
    Configuration config = builder.getConfiguration()
    
    if (style != null && style.equals("aggressive")) {
      config.setProperty("org.gradle.workers.max", (round(cores/2)).toString())
      config.setProperty("tests_jvms", (round(cores/2)).toString())
      config.setProperty("org.gradle.caching", "true")
    } else {
      config.setProperty("org.gradle.workers.max", (round(cores/3)).toString())
      config.setProperty("tests_jvms", (round(cores/3)).toString())
      config.setProperty("org.gradle.caching", "true")
    }
    
    builder.save();
  }
  
  @Option(option = "style", description = "Whether defaults should be aggressive")
  public void setStyle(String style) {
    this.style = style
  }
  
  private int round(BigDecimal d) {
    return Math.max(1, Math.round(d))
  }
}




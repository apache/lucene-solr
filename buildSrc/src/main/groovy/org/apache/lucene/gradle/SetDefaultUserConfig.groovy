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
import groovy.swing.SwingBuilder
import java.awt.HeadlessException
import org.apache.commons.configuration2.Configuration
import org.apache.commons.configuration2.FileBasedConfiguration
import org.apache.commons.configuration2.builder.FileBasedConfigurationBuilder
import org.apache.commons.configuration2.builder.fluent.Parameters
import org.apache.commons.configuration2.convert.DefaultListDelimiterHandler
import org.apache.commons.configuration2.PropertiesConfiguration

// tries to safely adjust a users gradle.properties jar to recommended defaults for the project
// for different levels of resource usage and performance
class SetDefaultUserConfig extends DefaultTask {

  private String style

  @TaskAction
  void config() {

    if (checkWithUser()) {
      
      File gradleConfigFile = project.file(System.getProperty("user.home") + "/.gradle/gradle.properties")
      project.copy {
        from gradleConfigFile
        into project.file(System.getProperty("user.home") + "/.gradle")
        rename { String fileName ->
          fileName.replace("gradle.properties", "gradle.properties.bak")
        }
      }

      int cores = Runtime.getRuntime().availableProcessors()


      char delim = ','

      Parameters params = new Parameters();
      FileBasedConfigurationBuilder<FileBasedConfiguration> builder = new FileBasedConfigurationBuilder<FileBasedConfiguration>(PropertiesConfiguration.class)
      builder.configure(params.properties()
          .setFile(gradleConfigFile)
          .setListDelimiterHandler(new DefaultListDelimiterHandler(delim)))

      Configuration config = builder.getConfiguration()

      if (style != null && style.equals("aggressive")) {
        config.setProperty("org.gradle.workers.max", (round(cores/2)).toString())
        config.setProperty("tests_jvms", (round(cores/2)).toString())
        config.setProperty("org.gradle.caching", "true")
        config.setProperty("org.gradle.jvmargs", "-Xmx1g")
      } else {
        config.setProperty("org.gradle.workers.max", (round(cores/3)).toString())
        config.setProperty("tests_jvms", (round(cores/3)).toString())
        config.setProperty("org.gradle.caching", "false")
        config.setProperty("org.gradle.jvmargs", "-Xmx512m")
      }

      // must be false for our project
      config.setProperty("org.gradle.configureondemand", "false")

      builder.save();
    }
  }

  @Option(option = "style", description = "Whether defaults should be aggressive")
  public void setStyle(String style) {
    this.style = style
  }

  private int round(BigDecimal d) {
    return Math.max(1, Math.round(d))
  }

  private boolean checkWithUser() {
    def answer = ''

    // if using an ide or the gradle daemon, this will be null, in recent versions of gradle even without the daemon
    try {
      new SwingBuilder().edt {
        dialog(modal: true, title: '(y/n)', alwaysOnTop: true, resizable: false, locationRelativeTo: null, pack: true, show: true) {
          vbox {
            label(text: "Are you sure? This will modify your ~/.gradle/gradle.properties file!")
            input = textField()

            button(defaultButton: true, text: 'OK', actionPerformed: {
              answer = input.text;
              dispose();
            })

          }
        }
      }
    } catch (HeadlessException e) {
      answer = System.console().readPassword("\nAre you sure? This will modify your ~/.gradle/gradle.properties file! (Y/N):")
      answer = new String(answer)

      ant.input(message: 'Are you sure? This will modify your ~/.gradle/gradle.properties file! (y/n):', validargs: 'y,n', addproperty: 'modifyGradleHomeConf')

      answer = ant.modifyGradleHomeConf
    }

    if (answer.equalsIgnoreCase("y")) {
      return true
    }
    return false
  }
}




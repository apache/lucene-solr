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
 * Runs maven build from within Ant after creating POMs.
 */

import groovy.xml.NamespaceBuilder;
import org.apache.tools.ant.Project;

def userHome = properties['user.home'], commonDir = properties['common.dir'];
def propPrefix = '-mvn.inject.'; int propPrefixLen = propPrefix.length();

def subProject = project.createSubProject();
project.copyUserProperties(subProject);
subProject.initProperties();
new AntBuilder(subProject).sequential{
  property(file: userHome+'/lucene.build.properties', prefix: propPrefix);
  property(file: userHome+'/build.properties', prefix: propPrefix);
  property(file: commonDir+'/build.properties', prefix: propPrefix);
};

def cmdlineProps = subProject.properties
  .findAll{ k, v -> k.startsWith(propPrefix) }
  .collectEntries{ k, v -> [k.substring(propPrefixLen), v] };
cmdlineProps << project.userProperties.findAll{ k, v -> !k.startsWith('ant.') };

def artifact = NamespaceBuilder.newInstance(ant, 'antlib:org.apache.maven.artifact.ant');

task.log('Running Maven with props: ' + cmdlineProps.toString(), Project.MSG_INFO);
artifact.mvn(pom: properties['maven-build-dir']+'/pom.xml', mavenVersion: properties['maven-version'], failonerror: true, fork: true) {
  cmdlineProps.each{ k, v -> arg(value: '-D' + k + '=' + v) };
  arg(value: '-fae');
  arg(value: 'install');
};

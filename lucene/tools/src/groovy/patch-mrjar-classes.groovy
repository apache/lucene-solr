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
 * Patches Java 8 class files to replace method signatures by
 * native Java 9 optimized ones (to be placed in MR-JAR).
 */

import org.apache.tools.ant.Project;

import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.commons.ClassRemapper;
import org.objectweb.asm.commons.Remapper;

def mappings = [
  'org/apache/lucene/util/FutureObjects': 'java/util/Objects',
  'org/apache/lucene/util/FutureArrays': 'java/util/Arrays',
];

if (properties['run.clover'] != null) {
  task.log("Disabled class file remapping for Java 9, because Clover code coverage is enabled.", Project.MSG_INFO);
  return;
}

File inputDir = new File(properties['build.dir'], 'classes/java');
File outputDir = new File(properties['build.dir'], 'classes/java9');

outputDir.mkdirs();

def scanner = ant.fileScanner {
  fileset(dir:inputDir) {
    include(name:"**/*.class")
  }
}

int count = 0;
for (f in scanner) {
  ClassReader reader = new ClassReader(f.getBytes());
  if (mappings.containsKey(reader.className)) {
    // we do not remap our replacements! :-)
    continue;
  }

  ClassWriter writer = new ClassWriter(0 /* no recalculations needed */);  
  boolean remapped = false;
  ClassRemapper remapper = new ClassRemapper(writer, new Remapper() {
    @Override
    public String map(String typeName) {
      if (mappings.containsKey(typeName)) {
        remapped = true;
        return mappings.get(typeName);
      }
      return typeName;
    }
  });
  
  reader.accept(remapper, 0 /* keep everything as-is*/);
  
  if (remapped) {
    task.log("Remapped: "+reader.className, Project.MSG_INFO);
    File output = new File(outputDir, reader.className + '.class');
    output.parentFile.mkdirs();
    output.setBytes(writer.toByteArray());
    count++;
  }
}

task.log("Remapped $count class files for Java 9 to: $outputDir", Project.MSG_INFO);

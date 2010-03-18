/**
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

package org.apache.solr.core;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;

import java.io.IOException;
import java.io.File;
import java.util.Map;
import java.util.HashMap;

/**
 * Directory provider for using lucene RAMDirectory
 */
public class RAMDirectoryFactory extends StandardDirectoryFactory {
  private Map<String, Directory> directories = new HashMap<String, Directory>();

  @Override
  public Directory open(String path) throws IOException {
    synchronized (this) {
      Directory directory = directories.get(path);
      if (directory == null) {
        directory = openNew(path);
        directories.put(path, directory);
      }

      return directory;
    }
  }

  /**
   * Non-public for unit-test access only. Do not use directly
   */
  Directory openNew(String path) throws IOException {
    Directory directory;
    File dirFile = new File(path);
    boolean indexExists = dirFile.canRead();
    if (indexExists) {
      Directory dir = super.open(path);
      directory = new RAMDirectory(dir);
    } else {
      directory = new RAMDirectory();
    }
    return directory;
  }
}

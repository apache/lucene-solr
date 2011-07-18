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

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.store.Directory;

/**
 * Directory provider for using lucene RAMDirectory
 */
public class RAMDirectoryFactory extends StandardDirectoryFactory {
  private static Map<String, RefCntRamDirectory> directories = new HashMap<String, RefCntRamDirectory>();

  @Override
  public Directory open(String path) throws IOException {
    synchronized (RAMDirectoryFactory.class) {
      RefCntRamDirectory directory = directories.get(path);
      if (directory == null || !directory.isOpen()) {
        directory = (RefCntRamDirectory) openNew(path);
        directories.put(path, directory);
      } else {
        directory.incRef();
      }

      return directory;
    }
  }
  
  @Override
  public boolean exists(String path) {
    synchronized (RAMDirectoryFactory.class) {
      RefCntRamDirectory directory = directories.get(path);
      if (directory == null || !directory.isOpen()) {
        return false;
      } else {
        return true;
      }
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
      directory = new RefCntRamDirectory(dir);
    } else {
      directory = new RefCntRamDirectory();
    }
    return directory;
  }
}

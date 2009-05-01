package org.apache.lucene.store;

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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Files with the specified extensions are placed in the
 * primary directory; others are placed in the secondary
 * directory.  The provided Set must not change once passed
 * to this class, and must allow multiple threads to call
 * contains at once.
 */

public class FileSwitchDirectory extends Directory {
  private final Directory secondaryDir;
  private final Directory primaryDir;
  private final Set primaryExtensions;
  private boolean doClose;

  public FileSwitchDirectory(Set primaryExtensions, Directory primaryDir, Directory secondaryDir, boolean doClose) {
    this.primaryExtensions = primaryExtensions;
    this.primaryDir = primaryDir;
    this.secondaryDir = secondaryDir;
    this.doClose = doClose;
    this.lockFactory = primaryDir.getLockFactory();
  }
  
  public void close() throws IOException {
    if (doClose) {
      try {
        secondaryDir.close();
      } finally { 
        primaryDir.close();
      }
      doClose = false;
    }
  }
  
  public String[] listAll() throws IOException {
    List list = new ArrayList();
    String[] ramFiles = secondaryDir.listAll();
    for (int x = 0; x < ramFiles.length; x++) {
      list.add(ramFiles[x]);
    }
    String[] fsFiles = primaryDir.listAll();
    for (int x = 0; x < fsFiles.length; x++) {
      list.add(fsFiles[x]);
    }
    return (String[]) list.toArray(new String[0]);
  }
  
  public String[] list() throws IOException {
    return listAll();
  }

  public static String getExtension(String name) {
    int i = name.lastIndexOf('.');
    if (i == -1) {
      return "";
    }
    return name.substring(i+1, name.length());
  }

  private Directory getDirectory(String name) {
    String ext = getExtension(name);
    if (primaryExtensions.contains(ext)) {
      return primaryDir;
    } else {
      return secondaryDir;
    }
  }

  public boolean fileExists(String name) throws IOException {
    return getDirectory(name).fileExists(name);
  }

  public long fileModified(String name) throws IOException {
    return getDirectory(name).fileModified(name);
  }

  public void touchFile(String name) throws IOException {
    getDirectory(name).touchFile(name);
  }

  public void deleteFile(String name) throws IOException {
    getDirectory(name).deleteFile(name);
  }

  public void renameFile(String from, String to) throws IOException {
    getDirectory(from).renameFile(from, to);
  }

  public long fileLength(String name) throws IOException {
    return getDirectory(name).fileLength(name);
  }

  public IndexOutput createOutput(String name) throws IOException {
    return getDirectory(name).createOutput(name);
  }

  public void sync(String name) throws IOException {
    getDirectory(name).sync(name);
  }

  public IndexInput openInput(String name) throws IOException {
    return getDirectory(name).openInput(name);
  }
}

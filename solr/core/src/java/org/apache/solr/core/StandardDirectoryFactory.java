package org.apache.solr.core;
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

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IOContext;

/**
 * Directory provider which mimics original Solr 
 * {@link org.apache.lucene.store.FSDirectory} based behavior.
 * 
 * File based DirectoryFactory implementations generally extend
 * this class.
 * 
 */
public class StandardDirectoryFactory extends CachingDirectoryFactory {

  @Override
  protected Directory create(String path, DirContext dirContext) throws IOException {
    return FSDirectory.open(new File(path));
  }
  
  @Override
  public String normalize(String path) throws IOException {
    return new File(path).getCanonicalPath();
  }
  
  public boolean isPersistent() {
    return true;
  }
  
  @Override
  public void remove(Directory dir) throws IOException {
    CacheValue val = byDirectoryCache.get(dir);
    if (val == null) {
      throw new IllegalArgumentException("Unknown directory " + dir);
    }
    File dirFile = new File(val.path);
    FileUtils.deleteDirectory(dirFile);
  }
  

  @Override
  public void remove(String path) throws IOException {
    String fullPath = new File(path).getAbsolutePath();
    File dirFile = new File(fullPath);
    FileUtils.deleteDirectory(dirFile);
  }
  
  /**
   * Override for more efficient moves.
   * 
   * @throws IOException
   *           If there is a low-level I/O error.
   */
  @Override
  public void move(Directory fromDir, Directory toDir, String fileName, IOContext ioContext)
      throws IOException {
    if (fromDir instanceof FSDirectory && toDir instanceof FSDirectory) {
      File dir1 = ((FSDirectory) fromDir).getDirectory();
      File dir2 = ((FSDirectory) toDir).getDirectory();
      File indexFileInTmpDir = new File(dir1, fileName);
      File indexFileInIndex = new File(dir2, fileName);
      boolean success = indexFileInTmpDir.renameTo(indexFileInIndex);
      if (success) {
        return;
      }
    }

    super.move(fromDir, toDir, fileName, ioContext);
  }

}

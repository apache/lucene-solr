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

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MockDirectoryWrapper;
import org.apache.lucene.util.LuceneTestCase;

/**
 * Opens a directory with {@link LuceneTestCase#newDirectory()}
 */
public class MockDirectoryFactory extends CachingDirectoryFactory {

  @Override
  protected Directory create(String path) throws IOException {
    Directory dir = LuceneTestCase.newDirectory();
    // we can't currently do this check because of how
    // Solr has to reboot a new Directory sometimes when replicating
    // or rolling back - the old directory is closed and the following
    // test assumes it can open an IndexWriter when that happens - we
    // have a new Directory for the same dir and still an open IW at 
    // this point
    if (dir instanceof MockDirectoryWrapper) {
      ((MockDirectoryWrapper)dir).setAssertNoUnrefencedFilesOnClose(false);
    }
    return dir;
  }
  
  @Override
  public boolean exists(String path) {
    String fullPath = new File(path).getAbsolutePath();
    synchronized (DirectoryFactory.class) {
      CacheValue cacheValue = byPathCache.get(fullPath);
      Directory directory = null;
      if (cacheValue != null) {
        directory = cacheValue.directory;
      }
      if (directory == null) {
        return false;
      } else {
        return true;
      }
    }
  }
}

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
package org.apache.solr.core;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.LockFactory;
import org.apache.lucene.store.MockDirectoryWrapper;
import org.apache.lucene.store.NRTCachingDirectory;
import org.apache.lucene.store.TrackingDirectoryWrapper;
import org.apache.lucene.util.LuceneTestCase;

/**
 * Opens a directory with {@link LuceneTestCase#newFSDirectory(Path)}
 */
public class MockFSDirectoryFactory extends StandardDirectoryFactory {

  @Override
  public Directory create(String path, LockFactory lockFactory, DirContext dirContext) throws IOException {
    // we pass NoLockFactory, because the real lock factory is set later by injectLockFactory:
    Directory dir = LuceneTestCase.newFSDirectory(new File(path).toPath(), lockFactory);
    // we can't currently do this check because of how
    // Solr has to reboot a new Directory sometimes when replicating
    // or rolling back - the old directory is closed and the following
    // test assumes it can open an IndexWriter when that happens - we
    // have a new Directory for the same dir and still an open IW at 
    // this point
    
    Directory cdir = reduce(dir);
    cdir = reduce(cdir);
    cdir = reduce(cdir);
    
    if (cdir instanceof MockDirectoryWrapper) {
      ((MockDirectoryWrapper)cdir).setAssertNoUnrefencedFilesOnClose(false);
    }
    return dir;
  }
  
  @Override
  public boolean isAbsolute(String path) {
    // TODO: kind of a hack - we don't know what the delegate is, so
    // we treat it as file based since this works on most ephem impls
    return new File(path).isAbsolute();
  }
  
  private Directory reduce(Directory dir) {
    Directory cdir = dir;
    if (dir instanceof NRTCachingDirectory) {
      cdir = ((NRTCachingDirectory)dir).getDelegate();
    }
    if (cdir instanceof TrackingDirectoryWrapper) {
      cdir = ((TrackingDirectoryWrapper)dir).getDelegate();
    }
    return cdir;
  }
}

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
import org.apache.lucene.store.LockFactory;
import org.apache.lucene.store.MockDirectoryWrapper;
import org.apache.lucene.store.NRTCachingDirectory;
import org.apache.lucene.store.NoLockFactory;
import org.apache.lucene.store.RateLimitedDirectoryWrapper;
import org.apache.lucene.store.TrackingDirectoryWrapper;
import org.apache.lucene.util.LuceneTestCase;

/**
 * Opens a directory with {@link LuceneTestCase#newDirectory()}
 */
public class MockDirectoryFactory extends EphemeralDirectoryFactory {
  
  public static final String SOLR_TESTS_ALLOW_READING_FILES_STILL_OPEN_FOR_WRITE = "solr.tests.allow_reading_files_still_open_for_write";
  private boolean allowReadingFilesStillOpenForWrite = Boolean.getBoolean(SOLR_TESTS_ALLOW_READING_FILES_STILL_OPEN_FOR_WRITE);

  @Override
  protected LockFactory createLockFactory(String lockPath, String rawLockType) throws IOException {
    return NoLockFactory.getNoLockFactory(); // dummy, actually unused
  }

  @Override
  protected Directory create(String path, LockFactory lockFactory, DirContext dirContext) throws IOException {
    Directory dir = LuceneTestCase.newDirectory(); // we ignore the given lock factory
    
    Directory cdir = reduce(dir);
    cdir = reduce(cdir);
    cdir = reduce(cdir);
    
    if (cdir instanceof MockDirectoryWrapper) {
      MockDirectoryWrapper mockDirWrapper = (MockDirectoryWrapper) cdir;
      
      // we can't currently do this check because of how
      // Solr has to reboot a new Directory sometimes when replicating
      // or rolling back - the old directory is closed and the following
      // test assumes it can open an IndexWriter when that happens - we
      // have a new Directory for the same dir and still an open IW at 
      // this point
      mockDirWrapper.setAssertNoUnrefencedFilesOnClose(false);
      
      // ram dirs in cores that are restarted end up empty
      // and check index fails
      mockDirWrapper.setCheckIndexOnClose(false);
      
      // if we enable this, TestReplicationHandler fails when it
      // tries to write to index.properties after the file has
      // already been created.
      mockDirWrapper.setPreventDoubleWrite(false);
      
      // snappuller & co don't seem ready for this:
      mockDirWrapper.setEnableVirusScanner(false);
      
      if (allowReadingFilesStillOpenForWrite) {
        mockDirWrapper.setAllowReadingFilesStillOpenForWrite(true);
      }
    }
    
    return dir;
  }

  private Directory reduce(Directory dir) {
    Directory cdir = dir;
    if (dir instanceof NRTCachingDirectory) {
      cdir = ((NRTCachingDirectory)dir).getDelegate();
    }
    if (cdir instanceof RateLimitedDirectoryWrapper) {
      cdir = ((RateLimitedDirectoryWrapper)dir).getDelegate();
    }
    if (cdir instanceof TrackingDirectoryWrapper) {
      cdir = ((TrackingDirectoryWrapper)dir).getDelegate();
    }
    return cdir;
  }
  
  @Override
  public boolean isAbsolute(String path) {
    // TODO: kind of a hack - we don't know what the delegate is, so
    // we treat it as file based since this works on most ephem impls
    return new File(path).isAbsolute();
  }

}

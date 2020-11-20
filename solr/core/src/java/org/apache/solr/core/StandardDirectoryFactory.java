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
import java.lang.invoke.MethodHandles;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Locale;

import org.apache.commons.io.FileUtils;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.LockFactory;
import org.apache.lucene.store.NativeFSLockFactory;
import org.apache.lucene.store.NoLockFactory;
import org.apache.lucene.store.SimpleFSLockFactory;
import org.apache.lucene.store.SingleInstanceLockFactory;
import org.apache.solr.common.SolrException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Directory provider which mimics original Solr 
 * {@link org.apache.lucene.store.FSDirectory} based behavior.
 * 
 * File based DirectoryFactory implementations generally extend
 * this class.
 * 
 */
public class StandardDirectoryFactory extends CachingDirectoryFactory {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @Override
  protected Directory create(String path, LockFactory lockFactory, DirContext dirContext) throws IOException {
    // we pass NoLockFactory, because the real lock factory is set later by injectLockFactory:
    return FSDirectory.open(new File(path).toPath(), lockFactory);
  }
  
  @Override
  protected LockFactory createLockFactory(String rawLockType) throws IOException {
    if (null == rawLockType) {
      rawLockType = DirectoryFactory.LOCK_TYPE_NATIVE;
      log.warn("No lockType configured, assuming '{}'.", rawLockType);
    }
    final String lockType = rawLockType.toLowerCase(Locale.ROOT).trim();
    switch (lockType) {
      case DirectoryFactory.LOCK_TYPE_SIMPLE:
        return SimpleFSLockFactory.INSTANCE;
      case DirectoryFactory.LOCK_TYPE_NATIVE:
        return NativeFSLockFactory.INSTANCE;
      case DirectoryFactory.LOCK_TYPE_SINGLE:
        return new SingleInstanceLockFactory();
      case DirectoryFactory.LOCK_TYPE_NONE:
        return NoLockFactory.INSTANCE;
      default:
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
            "Unrecognized lockType: " + rawLockType);
    }
  }
  
  @Override
  public String normalize(String path) throws IOException {
    String cpath = new File(path).getCanonicalPath();
    
    return super.normalize(cpath);
  }

  public boolean isPersistent() {
    return true;
  }

  @Override
  protected void removeDirectory(CacheValue cacheValue) throws IOException {
    File dirFile = new File(cacheValue.path);
    FileUtils.deleteDirectory(dirFile);
  }
  
  /**
   * Override for more efficient moves.
   * 
   * Intended for use with replication - use
   * carefully - some Directory wrappers will
   * cache files for example.
   * 
   * You should first {@link Directory#sync(java.util.Collection)} any file that will be 
   * moved or avoid cached files through settings.
   * 
   * @throws IOException
   *           If there is a low-level I/O error.
   */
  @Override
  public void move(Directory fromDir, Directory toDir, String fileName, IOContext ioContext)
      throws IOException {
    
    Directory baseFromDir = getBaseDir(fromDir);
    Directory baseToDir = getBaseDir(toDir);
    
    if (baseFromDir instanceof FSDirectory && baseToDir instanceof FSDirectory) {
  
      Path path1 = ((FSDirectory) baseFromDir).getDirectory().toAbsolutePath();
      Path path2 = ((FSDirectory) baseToDir).getDirectory().toAbsolutePath();
      
      try {
        Files.move(path1.resolve(fileName), path2.resolve(fileName), StandardCopyOption.ATOMIC_MOVE);
      } catch (AtomicMoveNotSupportedException e) {
        Files.move(path1.resolve(fileName), path2.resolve(fileName));
      }
      return;
    }

    super.move(fromDir, toDir, fileName, ioContext);
  }

  // perform an atomic rename if possible
  public void renameWithOverwrite(Directory dir, String fileName, String toName) throws IOException {
    Directory baseDir = getBaseDir(dir);
    if (baseDir instanceof FSDirectory) {
      Path path = ((FSDirectory) baseDir).getDirectory().toAbsolutePath();
      try {
        Files.move(path.resolve(fileName),
            path.resolve(toName), StandardCopyOption.ATOMIC_MOVE,
            StandardCopyOption.REPLACE_EXISTING);
      } catch (AtomicMoveNotSupportedException e) {
        Files.move(FileSystems.getDefault().getPath(path.toString(), fileName),
            FileSystems.getDefault().getPath(path.toString(), toName), StandardCopyOption.REPLACE_EXISTING);
      }
    } else {
      super.renameWithOverwrite(dir, fileName, toName);
    }
  }

}

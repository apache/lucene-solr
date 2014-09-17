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
import java.util.Locale;

import org.apache.commons.io.FileUtils;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.LockFactory;
import org.apache.lucene.store.NRTCachingDirectory;
import org.apache.lucene.store.NativeFSLockFactory;
import org.apache.lucene.store.NoLockFactory;
import org.apache.lucene.store.RateLimitedDirectoryWrapper;
import org.apache.lucene.store.SimpleFSLockFactory;
import org.apache.lucene.store.SingleInstanceLockFactory;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
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

  private static final Logger log = LoggerFactory.getLogger(StandardDirectoryFactory.class);

  @Override
  protected Directory create(String path, LockFactory lockFactory, DirContext dirContext) throws IOException {
    // we pass NoLockFactory, because the real lock factory is set later by injectLockFactory:
    return FSDirectory.open(new File(path).toPath(), lockFactory);
  }
  
  @Override
  protected LockFactory createLockFactory(String lockPath, String rawLockType) throws IOException {
    if (null == rawLockType) {
      // we default to "native"
      log.warn("No lockType configured, assuming 'native'.");
      rawLockType = "native";
    }
    final String lockType = rawLockType.toLowerCase(Locale.ROOT).trim();
    switch (lockType) {
      case "simple":
        return new SimpleFSLockFactory(new File(lockPath).toPath());
      case "native":
        return new NativeFSLockFactory(new File(lockPath).toPath());
      case "single":
        return new SingleInstanceLockFactory();
      case "none":
        return NoLockFactory.getNoLockFactory();
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
  
  @Override
  public boolean exists(String path) throws IOException {
    // we go by the persistent storage ... 
    File dirFile = new File(path);
    return dirFile.canRead() && dirFile.list().length > 0;
  }
  
  public boolean isPersistent() {
    return true;
  }
  
  @Override
  public boolean isAbsolute(String path) {
    // back compat
    return new File(path).isAbsolute();
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
   * This implementation works with two wrappers:
   * NRTCachingDirectory and RateLimitedDirectoryWrapper.
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
      File dir1 = ((FSDirectory) baseFromDir).getDirectory().toFile();
      File dir2 = ((FSDirectory) baseToDir).getDirectory().toFile();
      File indexFileInTmpDir = new File(dir1, fileName);
      File indexFileInIndex = new File(dir2, fileName);
      boolean success = indexFileInTmpDir.renameTo(indexFileInIndex);
      if (success) {
        return;
      }
    }

    super.move(fromDir, toDir, fileName, ioContext);
  }

  // special hack to work with NRTCachingDirectory and RateLimitedDirectoryWrapper
  private Directory getBaseDir(Directory dir) {
    Directory baseDir;
    if (dir instanceof NRTCachingDirectory) {
      baseDir = ((NRTCachingDirectory)dir).getDelegate();
    } else if (dir instanceof RateLimitedDirectoryWrapper) {
      baseDir = ((RateLimitedDirectoryWrapper)dir).getDelegate();
    } else {
      baseDir = dir;
    }
    
    return baseDir;
  }

}

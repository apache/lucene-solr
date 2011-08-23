package org.apache.solr.core;

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

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.NativeFSLockFactory;
import org.apache.lucene.store.NoLockFactory;
import org.apache.lucene.store.SimpleFSLockFactory;
import org.apache.lucene.store.SingleInstanceLockFactory;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.NamedList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link DirectoryFactory} impl base class for caching Directory instances
 * per path. Most DirectoryFactory implementations will want to extend this
 * class and simply implement {@link DirectoryFactory#create(String)}.
 * 
 */
public abstract class CachingDirectoryFactory extends DirectoryFactory {
  class CacheValue {
    Directory directory;
    int refCnt = 1;
    public String path;
    public boolean doneWithDir = false;
  }
  
  private static Logger log = LoggerFactory
      .getLogger(CachingDirectoryFactory.class);
  
  protected Map<String,CacheValue> byPathCache = new HashMap<String,CacheValue>();
  
  protected Map<Directory,CacheValue> byDirectoryCache = new HashMap<Directory,CacheValue>();
  
  /*
   * (non-Javadoc)
   * 
   * @see org.apache.solr.core.DirectoryFactory#close()
   */
  @Override
  public void close() throws IOException {
    synchronized (this) {
      for (CacheValue val : byDirectoryCache.values()) {
        val.directory.close();
      }
      byDirectoryCache.clear();
      byPathCache.clear();
    }
  }
  
  private void close(Directory directory) throws IOException {
    synchronized (this) {
      CacheValue cacheValue = byDirectoryCache.get(directory);
      if (cacheValue == null) {
        throw new IllegalArgumentException("Unknown directory: " + directory
            + " " + byDirectoryCache);
      }
      cacheValue.refCnt--;
      if (cacheValue.refCnt == 0 && cacheValue.doneWithDir) {
        directory.close();
        byDirectoryCache.remove(directory);
        byPathCache.remove(cacheValue.path);
      }
    }
  }
  
  protected abstract Directory create(String path) throws IOException;
  
  @Override
  public boolean exists(String path) {
    // back compat behavior
    File dirFile = new File(path);
    return dirFile.canRead() && dirFile.list().length > 0;
  }
  
  /*
   * (non-Javadoc)
   * 
   * @see org.apache.solr.core.DirectoryFactory#get(java.lang.String,
   * java.lang.String)
   */
  @Override
  public final Directory get(String path, String rawLockType)
      throws IOException {
    return get(path, rawLockType, false);
  }
  
  /*
   * (non-Javadoc)
   * 
   * @see org.apache.solr.core.DirectoryFactory#get(java.lang.String,
   * java.lang.String, boolean)
   */
  @Override
  public final Directory get(String path, String rawLockType, boolean forceNew)
      throws IOException {
    String fullPath = new File(path).getAbsolutePath();
    synchronized (this) {
      CacheValue cacheValue = byPathCache.get(fullPath);
      Directory directory = null;
      if (cacheValue != null) {
        directory = cacheValue.directory;
        if (forceNew) {
          cacheValue.doneWithDir = true;
          if (cacheValue.refCnt == 0) {
            close(cacheValue.directory);
          }
        }
      }
      
      if (directory == null || forceNew) {
        directory = create(fullPath);
        
        CacheValue newCacheValue = new CacheValue();
        newCacheValue.directory = directory;
        newCacheValue.path = fullPath;
        
        injectLockFactory(directory, path, rawLockType);
        
        byDirectoryCache.put(directory, newCacheValue);
        byPathCache.put(fullPath, newCacheValue);
      } else {
        cacheValue.refCnt++;
      }
      
      return directory;
    }
  }
  
  /*
   * (non-Javadoc)
   * 
   * @see
   * org.apache.solr.core.DirectoryFactory#incRef(org.apache.lucene.store.Directory
   * )
   */
  public void incRef(Directory directory) {
    synchronized (this) {
      CacheValue cacheValue = byDirectoryCache.get(directory);
      if (cacheValue == null) {
        throw new IllegalArgumentException("Unknown directory: " + directory);
      }
      
      cacheValue.refCnt++;
    }
  }
  
  public void init(NamedList args) {}
  
  /*
   * (non-Javadoc)
   * 
   * @see
   * org.apache.solr.core.DirectoryFactory#release(org.apache.lucene.store.Directory
   * )
   */
  @Override
  public void release(Directory directory) throws IOException {
    if (directory == null) {
      throw new NullPointerException();
    }
    close(directory);
  }
  
  /**
   * @param dir
   * @param lockPath
   * @param rawLockType
   * @return
   * @throws IOException
   */
  private static Directory injectLockFactory(Directory dir, String lockPath,
      String rawLockType) throws IOException {
    if (null == rawLockType) {
      // we default to "simple" for backwards compatibility
      log.warn("No lockType configured for " + dir + " assuming 'simple'");
      rawLockType = "simple";
    }
    final String lockType = rawLockType.toLowerCase(Locale.ENGLISH).trim();
    
    if ("simple".equals(lockType)) {
      // multiple SimpleFSLockFactory instances should be OK
      dir.setLockFactory(new SimpleFSLockFactory(lockPath));
    } else if ("native".equals(lockType)) {
      dir.setLockFactory(new NativeFSLockFactory(lockPath));
    } else if ("single".equals(lockType)) {
      if (!(dir.getLockFactory() instanceof SingleInstanceLockFactory)) dir
          .setLockFactory(new SingleInstanceLockFactory());
    } else if ("none".equals(lockType)) {
      // Recipe for disaster
      log.error("CONFIGURATION WARNING: locks are disabled on " + dir);
      dir.setLockFactory(NoLockFactory.getNoLockFactory());
    } else {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
          "Unrecognized lockType: " + rawLockType);
    }
    return dir;
  }
}

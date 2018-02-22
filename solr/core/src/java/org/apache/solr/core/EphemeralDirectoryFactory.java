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
import java.io.IOException;
import java.lang.invoke.MethodHandles;

import org.apache.lucene.store.Directory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Directory provider for implementations that do not persist over reboots.
 * 
 */
public abstract class EphemeralDirectoryFactory extends CachingDirectoryFactory {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  
  @Override
  public boolean exists(String path) throws IOException {
    String fullPath = normalize(path);
    synchronized (this) {
      final CacheValue cacheValue = byPathCache.get(fullPath);
      if (null == cacheValue) {
        return false;
      }
      final Directory directory = cacheValue.directory;
      if (null == directory) {
        return false;
      }
      if (0 < directory.listAll().length) {
        return true;
      } 
      return false;
    }
  }
  
  public boolean isPersistent() {
    return false;
  }
  
  @Override
  public boolean isAbsolute(String path) {
    return true;
  }
  
  
  @Override
  public void remove(Directory dir) throws IOException {
    // ram dir does not persist its dir anywhere
  }
  
  @Override
  public void remove(String path) throws IOException {
    // ram dir does not persist its dir anywhere
  }
  
  public void cleanupOldIndexDirectories(final String dataDirPath, final String currentIndexDirPath, boolean reload) {
    // currently a no-op
  }

}

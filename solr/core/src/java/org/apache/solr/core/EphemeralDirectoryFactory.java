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

/**
 * Directory provider for implementations that do not persist over reboots.
 * 
 */
public abstract class EphemeralDirectoryFactory extends CachingDirectoryFactory {
  
  @Override
  public boolean exists(String path) {
    String fullPath = new File(path).getAbsolutePath();
    synchronized (this) {
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
  
  @Override
  public void remove(Directory dir) throws IOException {
    // ram dir does not persist its dir anywhere
  }
  
  @Override
  public void remove(String path) throws IOException {
    // ram dir does not persist its dir anywhere
  }
  
  @Override
  public String normalize(String path) throws IOException {
    return path;
  }
}

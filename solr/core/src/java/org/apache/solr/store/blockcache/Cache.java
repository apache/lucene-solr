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
package org.apache.solr.store.blockcache;

/**
 * @lucene.experimental
 */
public interface Cache {
  
  /**
   * Remove a file from the cache.
   * 
   * @param name
   *          cache file name
   */
  void delete(String name);
  
  /**
   * Update the content of the specified cache file. Creates cache entry if
   * necessary.
   * 
   */
  void update(String name, long blockId, int blockOffset, byte[] buffer,
      int offset, int length);
  
  /**
   * Fetch the specified cache file content.
   * 
   * @return true if cached content found, otherwise return false
   */
  boolean fetch(String name, long blockId, int blockOffset, byte[] b, int off,
      int lengthToReadInBlock);
  
  /**
   * Number of entries in the cache.
   */
  long size();
  
  /**
   * Expert: Rename the specified file in the cache. Allows a file to be moved
   * without invalidating the cache.
   * 
   * @param source
   *          original name
   * @param dest
   *          final name
   */
  void renameCacheFile(String source, String dest);

  /**
   * Release any resources associated with the cache.
   */
  void releaseResources();
  
}

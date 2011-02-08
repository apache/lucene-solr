package org.apache.lucene.search.cache;

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

import java.io.IOException;

import org.apache.lucene.index.IndexReader;

/**
 * Create Cached Values for a given key
 * 
 * @lucene.experimental
 */
public abstract class EntryCreator<T>
{
  public abstract T create( IndexReader reader ) throws IOException;
  public abstract T validate( T entry, IndexReader reader ) throws IOException;

  /**
   * Indicate if a cached cached value should be checked before usage.
   * This is useful if an application wants to support subsequent calls
   * to the same cached object that may alter the cached object.  If
   * an application wants to avoid this (synchronized) check, it should
   * return 'false'
   *
   * @return 'true' if the Cache should call 'validate' before returning a cached object
   */
  public boolean shouldValidate() {
    return true;
  }

  /**
   * @return A key to identify valid cache entries for subsequent requests
   */
  public abstract EntryKey getCacheKey();
  

  //------------------------------------------------------------------------
  // The Following code is a hack to make things work while the 
  // EntryCreator is stored in in the FieldCache.  
  // When the FieldCache is replaced with a simpler map LUCENE-2665
  // This can be removed
  //------------------------------------------------------------------------

  @Override
  public boolean equals(Object obj) {
    if( obj instanceof EntryCreator ) {
      return getCacheKey().equals( ((EntryCreator)obj).getCacheKey() );
    }
    return false;
  }

  @Override
  public int hashCode() {
    return getCacheKey().hashCode();
  }
}

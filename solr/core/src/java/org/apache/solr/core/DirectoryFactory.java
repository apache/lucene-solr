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

import java.io.Closeable;
import java.io.IOException;

import org.apache.lucene.store.Directory;
import org.apache.solr.util.plugin.NamedListInitializedPlugin;

/**
 * Provides access to a Directory implementation. You must release every
 * Directory that you get.
 */
public abstract class DirectoryFactory implements NamedListInitializedPlugin,
    Closeable {
  
  /**
   * Close the this and all of the Directories it contains.
   * 
   * @throws IOException
   */
  public abstract void close() throws IOException;
  
  /**
   * Creates a new Directory for a given path.
   * 
   * @throws IOException
   */
  protected abstract Directory create(String path) throws IOException;
  
  /**
   * Returns true if a Directory exists for a given path.
   * 
   */
  public abstract boolean exists(String path);
  
  /**
   * Returns the Directory for a given path, using the specified rawLockType.
   * Will return the same Directory instance for the same path.
   * 
   * @throws IOException
   */
  public abstract Directory get(String path, String rawLockType)
      throws IOException;
  
  /**
   * Returns the Directory for a given path, using the specified rawLockType.
   * Will return the same Directory instance for the same path unless forceNew,
   * in which case a new Directory is returned.
   * 
   * @throws IOException
   */
  public abstract Directory get(String path, String rawLockType,
      boolean forceNew) throws IOException;
  
  /**
   * Increment the number of references to the given Directory. You must call
   * release for every call to this method.
   * 
   */
  public abstract void incRef(Directory directory);
  
  /**
   * Releases the Directory so that it may be closed when it is no longer
   * referenced.
   * 
   * @throws IOException
   */
  public abstract void release(Directory directory) throws IOException;
  
}

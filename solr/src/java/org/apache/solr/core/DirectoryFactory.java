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

import org.apache.lucene.store.Directory;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.util.plugin.NamedListInitializedPlugin;

/**
 * Provides access to a Directory implementation. 
 * 
 */
public abstract class DirectoryFactory implements NamedListInitializedPlugin {

  /**
   * Opens a Lucene directory
   * 
   * @throws IOException
   */
  public abstract Directory open(String path) throws IOException;
  
  public boolean exists(String path) {
    // back compat behavior
    File dirFile = new File(path);
    return dirFile.canRead();
  }

  
  public void init(NamedList args) {
  }
}

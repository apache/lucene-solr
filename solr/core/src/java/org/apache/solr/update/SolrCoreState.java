package org.apache.solr.update;

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

import org.apache.lucene.index.IndexWriter;
import org.apache.solr.core.DirectoryFactory;
import org.apache.solr.core.SolrCore;

/**
 * The state in this class can be easily shared between SolrCores across
 * SolrCore reloads.
 * 
 */
public abstract class SolrCoreState {
  
  /**
   * Force the creation of a new IndexWriter using the settings from the given
   * SolrCore.
   * 
   * @param core
   * @throws IOException
   */
  public abstract void newIndexWriter(SolrCore core) throws IOException;
  
  /**
   * Get the current IndexWriter. If a new IndexWriter must be created, use the
   * settings from the given {@link SolrCore}.
   * 
   * @param core
   * @return
   * @throws IOException
   */
  public abstract IndexWriter getIndexWriter(SolrCore core) throws IOException;
  
  /**
   * Decrement the number of references to this state. When then number of
   * references hits 0, the state will close.
   * 
   * @throws IOException
   */
  public abstract void decref() throws IOException;
  
  /**
   * Increment the number of references to this state.
   */
  public abstract void incref();
  
  /**
   * Rollback the current IndexWriter. When creating the new IndexWriter use the
   * settings from the given {@link SolrCore}.
   * 
   * @param core
   * @throws IOException
   */
  public abstract void rollbackIndexWriter(SolrCore core) throws IOException;
  
  /**
   * @return the {@link DirectoryFactory} that should be used.
   */
  public abstract DirectoryFactory getDirectoryFactory();
  
}

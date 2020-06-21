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

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.store.Directory;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.util.plugin.NamedListInitializedPlugin;

/**
 * Factory used to build a new IndexReader instance.
 */
public abstract class IndexReaderFactory implements NamedListInitializedPlugin {
  /**
   * init will be called just once, immediately after creation.
   * <p>
   * The args are user-level initialization parameters that may be specified
   * when declaring an indexReaderFactory in solrconfig.xml
   *
   */
  @Override
  public void init(@SuppressWarnings("rawtypes") NamedList args) {
   Object v = args.get("setTermIndexDivisor");
   if (v != null) {
     throw new IllegalArgumentException("Illegal parameter 'setTermIndexDivisor'");
   }
  }

  /**
   * Creates a new IndexReader instance using the given Directory.
   * 
   * @param indexDir indexDir index location
   * @param core {@link SolrCore} instance where this reader will be used. NOTE:
   * this SolrCore instance may not be fully configured yet, but basic things like
   * {@link SolrCore#getCoreDescriptor()}, {@link SolrCore#getLatestSchema()} and
   * {@link SolrCore#getSolrConfig()} are valid.
   * @return An IndexReader instance
   * @throws IOException If there is a low-level I/O error.
   */
  public abstract DirectoryReader newReader(Directory indexDir, SolrCore core)
      throws IOException;
  
  /**
   * Creates a new IndexReader instance using the given IndexWriter.
   * <p>
   * This is used for opening the initial reader in NRT mode
   *
   * @param writer IndexWriter
   * @param core {@link SolrCore} instance where this reader will be used. NOTE:
   * this SolrCore instance may not be fully configured yet, but basic things like
   * {@link SolrCore#getCoreDescriptor()}, {@link SolrCore#getLatestSchema()} and
   * {@link SolrCore#getSolrConfig()} are valid.
   * @return An IndexReader instance
   * @throws IOException If there is a low-level I/O error.
   */
  public abstract DirectoryReader newReader(IndexWriter writer, SolrCore core)
      throws IOException;
}

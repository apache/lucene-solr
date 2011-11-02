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

import java.io.IOException;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.store.Directory;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.util.plugin.NamedListInitializedPlugin;

/**
 * Factory used to build a new IndexReader instance.
 */
public abstract class IndexReaderFactory implements NamedListInitializedPlugin {
  protected int termInfosIndexDivisor = 1;//IndexReader.DEFAULT_TERMS_INDEX_DIVISOR;  Set this once Lucene makes this public.
  /**
   * Potentially initializes {@link #termInfosIndexDivisor}.  Overriding classes should call super.init() in order
   * to make sure termInfosIndexDivisor is set.
   * <p>
   * <code>init</code> will be called just once, immediately after creation.
   * <p>
   * The args are user-level initialization parameters that may be specified
   * when declaring an indexReaderFactory in solrconfig.xml
   *
   */
  public void init(NamedList args) {
    Integer v = (Integer)args.get("setTermIndexDivisor");
    if (v != null) {
      termInfosIndexDivisor = v.intValue();
    }
  }

  /**
   *
   * @return The setting of {@link #termInfosIndexDivisor} 
   */
  public int getTermInfosIndexDivisor() {
    return termInfosIndexDivisor;
  }

  /**
   * Creates a new IndexReader instance using the given Directory.
   * 
   * @param indexDir indexDir index location
   * @param readOnly return readOnly IndexReader
   * @return An IndexReader instance
   * @throws IOException
   */
  public abstract IndexReader newReader(Directory indexDir, boolean readOnly)
      throws IOException;
}

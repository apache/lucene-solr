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

package org.apache.solr.search;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.util.Bits;

import java.util.Map;
import java.io.IOException;


/** A SolrFilter extends the Lucene Filter and adds extra semantics such as passing on
 * weight context info for function queries.
 *
 * Experimental and subject to change.
 */
public abstract class SolrFilter extends Filter {

  /** Implementations should propagate createWeight to sub-ValueSources which can store weight info in the context.
   * The context object will be passed to getDocIdSet() where this info can be retrieved. */
  public abstract void createWeight(Map context, IndexSearcher searcher) throws IOException;
  
  public abstract DocIdSet getDocIdSet(Map context, LeafReaderContext readerContext, Bits acceptDocs) throws IOException;

  @Override
  public DocIdSet getDocIdSet(LeafReaderContext context, Bits acceptDocs) throws IOException {
    return getDocIdSet(null, context, acceptDocs);
  }
}

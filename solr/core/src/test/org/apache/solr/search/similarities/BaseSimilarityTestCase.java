package org.apache.solr.search.similarities;

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

import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.search.similarities.SimilarityProvider;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.core.SolrCore;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.util.RefCounted;

public abstract class BaseSimilarityTestCase extends SolrTestCaseJ4 {

  /** returns the similarity in use for the field */
  protected Similarity getSimilarity(String field) {
    SolrCore core = h.getCore();
    RefCounted<SolrIndexSearcher> searcher = core.getSearcher();
    Similarity sim = searcher.get().getSimilarityProvider().get(field);
    searcher.decref();
    return sim;
  }
  
  /** returns the (Solr)SimilarityProvider */
  protected SimilarityProvider getSimilarityProvider() {
    SolrCore core = h.getCore();
    RefCounted<SolrIndexSearcher> searcher = core.getSearcher();
    SimilarityProvider prov = searcher.get().getSimilarityProvider();
    searcher.decref();
    return prov;
  }
}

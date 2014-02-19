package org.apache.solr.handler.clustering;

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

import java.util.Map;
import java.util.Set;

import org.apache.lucene.search.Query;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;

/**
 * Base class for clustering engines performing cluster analysis on search
 * results.
 * 
 * @lucene.experimental
 */
public abstract class SearchClusteringEngine extends ClusteringEngine {
  /**
   * Do the clustering, return a clusters structure to be appended to
   * {@link SolrQueryResponse}.
   */
  public abstract Object cluster(Query query, SolrDocumentList solrDocumentList,
      Map<SolrDocument,Integer> docIds, SolrQueryRequest sreq);

  /**
   * Returns the set of field names to load.
   * Concrete classes can override this method if needed.
   * Default implementation returns null, that is, all stored fields are loaded.
   * 
   * @return The set of field names to load.
   */
  protected Set<String> getFieldsToLoad(SolrQueryRequest sreq){
    return null;
  }
}

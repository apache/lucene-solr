package org.apache.solr.handler.clustering;
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

import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.search.DocSet;


/**
 * Experimental.  Subject to change before the next release.
 *
 **/
public abstract class DocumentClusteringEngine extends ClusteringEngine {

  /**
   * Experimental.  Subject to change before the next release
   *
   * Cluster all the documents in the index.  Clustering is often an expensive task that can take a long time.
   * @param solrParams The params controlling clustering
   * @return The clustering results
   */
  public abstract NamedList cluster(SolrParams solrParams);

  /**
   *  Experimental.  Subject to change before the next release
   *
   *
   * Cluster the set of docs.  Clustering of documents is often an expensive task that can take a long time.
   * @param docs The docs to cluster.  If null, cluster all docs as in {@link #cluster(org.apache.solr.common.params.SolrParams)}
   * @param solrParams The params controlling the clustering
   * @return The results.
   */
  public abstract NamedList cluster(DocSet docs, SolrParams solrParams);


}

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

package org.apache.solr.cluster.api;

import org.apache.solr.common.SolrException;

/** Represents a Solr cluster */

public interface SolrCluster {

  /** collections in the cluster */
  SimpleMap<SolrCollection> collections() throws SolrException;

  /** collections in the cluster and aliases */
  SimpleMap<SolrCollection> collections(boolean includeAlias) throws SolrException;

  /** nodes in the cluster */
  SimpleMap<SolrNode> nodes() throws SolrException;


  /** Config sets in the cluster*/
  SimpleMap<CollectionConfig> configs() throws SolrException;

  /** Name of the node in which the overseer is running */
  String overseerNode() throws SolrException;

  /**
   * The name of the node in which this method is invoked from. returns null, if this is not invoked from a
   * Solr node
   */
  String thisNode();

}

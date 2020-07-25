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

package org.apache.solr.common.cloud.sdk;

import org.apache.solr.common.util.SimpleMap;

/**
 * Represents a Solr cluster
 */
public interface SolrCluster {
  /** collections in the cluster */
  SimpleMap<SolrCollection> collections();

  /** nodes in the cluster */
  SimpleMap<SolrNode> nodes();

  /** Get a {@link SolrNode} by name. returns null if no such node exists */
  SolrNode getNode(String node);

  /**
   * Name of the node in which the overseer is running
   */
  String overseerNode();

  /**
   * The name of the node in which this method is invoked from. returns null, if this is not invoked from a
   * Solr node
   */
  String thisNode();

}

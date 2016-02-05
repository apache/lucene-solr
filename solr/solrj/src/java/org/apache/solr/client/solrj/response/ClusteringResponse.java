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
package org.apache.solr.client.solrj.response;
import java.util.LinkedList;
import java.util.List;

import org.apache.solr.common.util.NamedList;

/**
 * Encapsulates responses from ClusteringComponent
 */
public class ClusteringResponse {

  private static final String LABELS_NODE = "labels";
  private static final String DOCS_NODE = "docs";
  private static final String SCORE_NODE = "score";
  private List<Cluster> clusters = new LinkedList<Cluster>();

  public ClusteringResponse(List<NamedList<Object>> clusterInfo) {
    for (NamedList<Object> clusterNode : clusterInfo) {
      List<String> labelList;
      List<String> docIdList;
      labelList = (List<String>) clusterNode.get(LABELS_NODE);
      double score = (double) clusterNode.get(SCORE_NODE);
      docIdList = (List<String>) clusterNode.get(DOCS_NODE);
      Cluster currentCluster = new Cluster(labelList, score, docIdList);
      clusters.add(currentCluster);
    }
  }

  public List<Cluster> getClusters() {
    return clusters;
  }

}
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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.solr.common.util.NamedList;

/**
 * Encapsulates responses from ClusteringComponent
 */
public class ClusteringResponse {
  private static final String CLUSTERS_NODE = "clusters";
  private static final String LABELS_NODE = "labels";
  private static final String DOCS_NODE = "docs";
  private static final String SCORE_NODE = "score";
  private static final String IS_OTHER_TOPICS = "other-topics";
  private List<Cluster> clusters;

  @SuppressWarnings("unchecked")
  public ClusteringResponse(List<NamedList<Object>> clusterInfo) {
    clusters = new ArrayList<Cluster>();
    for (NamedList<Object> clusterNode : clusterInfo) {
      List<String> labelList, docIdList;
      List<Cluster> subclusters = Collections.emptyList();
      labelList = docIdList = Collections.emptyList();
      Double score = 0d;
      boolean otherTopics = false;
      for (Map.Entry<String, ?> e : clusterNode) {
        switch (e.getKey()) {
          case LABELS_NODE:
            labelList = (List<String>) e.getValue(); 
            break;

          case DOCS_NODE:
            docIdList = (List<String>) e.getValue(); 
            break;
            
          case SCORE_NODE:
            score = (Double) e.getValue();
            break;

          case CLUSTERS_NODE:
            subclusters = new ClusteringResponse((List<NamedList<Object>>) e.getValue()).getClusters();
            break;
            
          case IS_OTHER_TOPICS:
            otherTopics = (Boolean) e.getValue();
            break;
        }
      }

      clusters.add(new Cluster(labelList, score, docIdList, subclusters, otherTopics));
    }
  }

  public List<Cluster> getClusters() {
    return clusters;
  }

}
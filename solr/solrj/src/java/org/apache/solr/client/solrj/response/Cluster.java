package org.apache.solr.client.solrj.response;

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

import java.util.List;

/**
 * This class represents a cluster of Solr Docs .
 * The cluster is produced from a set of Solr documents from the results.
 * It is a direct mapping for the Json object Solr is returning.
 */
public class Cluster {

  private List<String> labels;
  private double score;
  private List<String> docIds;

  /**
   * @param labels the list of human readable labels associated to the cluster
   * @param score  the score produced by the clustering algorithm for the current cluster
   * @param docIds   the list of document Ids belonging to the cluster
   */
  public Cluster(List<String> labels, double score, List<String> docIds) {
    this.labels = labels;
    this.score = score;
    this.docIds = docIds;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof Cluster)) return false;

    Cluster cluster = (Cluster) o;

    if (Double.compare(cluster.score, score) != 0) return false;
    if (!docIds.equals(cluster.docIds)) return false;
    if (!labels.equals(cluster.labels)) return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result;
    long temp;
    result = labels.hashCode();
    temp = Double.doubleToLongBits(score);
    result = 31 * result + (int) (temp ^ (temp >>> 32));
    result = 31 * result + docIds.hashCode();
    return result;
  }

  public List<String> getLabels() {
    return labels;
  }

  public void setLabels(List<String> labels) {
    this.labels = labels;
  }

  public double getScore() {
    return score;
  }

  public void setScore(double score) {
    this.score = score;
  }

  public List<String> getDocs() {
    return docIds;
  }

  public void setDocs(List<String> docIds) {
    this.docIds = docIds;
  }


}
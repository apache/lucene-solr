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

import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * This class represents a cluster of Solr Docs .
 */
public class Cluster {

  private List<String> labels;
  private double score;
  private List<String> docIds;
  private List<Cluster> subclusters;
  private boolean otherTopics;

  public Cluster(List<String> labels, double score, List<String> docIds) {
    this(labels, score, docIds, Collections.emptyList(), false);
  }

  /**
   * @param labels the list of human readable labels associated to the cluster
   * @param score  the score produced by the clustering algorithm for the current cluster
   * @param docIds   the list of document Ids belonging to the cluster
   */
  public Cluster(List<String> labels, double score, List<String> docIds, List<Cluster> subclusters, boolean otherTopics) {
    this.labels = labels;
    this.score = score;
    this.docIds = docIds;
    this.subclusters = subclusters;
    this.otherTopics = otherTopics;
  }

  @Override
  public boolean equals(Object o) {
    return o != null &&
           this.getClass().isInstance(o) &&
           equalsTo((Cluster) o);
  }

  private boolean equalsTo(Cluster o) {
    return Double.compare(o.score, score) == 0 &&
           Objects.equals(o.docIds, docIds) &&
           Objects.equals(o.labels, labels) &&
           Objects.equals(o.subclusters, subclusters);
  }

  @Override
  public int hashCode() {
    return Objects.hash(subclusters, docIds, labels, score);
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

  public List<Cluster> getSubclusters() {
    return subclusters;
  }

  /**
   * @return If <code>true</code>, the cluster contains references to documents that are not semantically associated
   * and form a group of documents not related to any other cluster (or themselves).
   */
  public boolean isOtherTopics() {
    return otherTopics;
  }
}
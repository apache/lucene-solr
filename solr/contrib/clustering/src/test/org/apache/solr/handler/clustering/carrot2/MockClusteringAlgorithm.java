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
package org.apache.solr.handler.clustering.carrot2;
import org.carrot2.core.*;
import org.carrot2.core.attribute.AttributeNames;
import org.carrot2.core.attribute.Processing;
import org.carrot2.util.attribute.*;
import org.carrot2.util.attribute.constraint.IntRange;

import java.util.ArrayList;
import java.util.List;

@Bindable(prefix = "MockClusteringAlgorithm")
public class MockClusteringAlgorithm extends ProcessingComponentBase implements
        IClusteringAlgorithm {
  @Input
  @Processing
  @Attribute(key = AttributeNames.DOCUMENTS)
  public List<Document> documents;

  @Output
  @Processing
  @Attribute(key = AttributeNames.CLUSTERS)
  public List<Cluster> clusters;

  @Input
  @Processing
  @Attribute
  @IntRange(min = 1, max = 5)
  public int depth = 2;

  @Input
  @Processing
  @Attribute
  @IntRange(min = 1, max = 5)
  public int labels = 1;

  @Input
  @Processing
  @Attribute
  @IntRange(min = 0)
  public int maxClusters = 0;

  @Input
  @Processing
  @Attribute
  public int otherTopicsModulo = 0;

  @Override
  public void process() throws ProcessingException {
    clusters = new ArrayList<>();
    if (documents == null) {
      return;
    }

    if (maxClusters > 0) {
      documents = documents.subList(0, maxClusters);
    }

    int documentIndex = 1;
    for (Document document : documents) {
      StringBuilder label = new StringBuilder("Cluster " + documentIndex);
      Cluster cluster = createCluster(label.toString(), documentIndex, document);
      clusters.add(cluster);
      for (int i = 1; i <= depth; i++) {
        label.append(".");
        label.append(i);
        Cluster newCluster = createCluster(label.toString(), documentIndex, document);
        cluster.addSubclusters(createCluster(label.toString(), documentIndex, document), newCluster);
        cluster = newCluster;
      }
      documentIndex++;
    }
  }

  private Cluster createCluster(String labelBase, int documentIndex, Document... documents) {
    Cluster cluster = new Cluster();
    cluster.setScore(documentIndex * 0.25);
    if (otherTopicsModulo != 0 && documentIndex % otherTopicsModulo == 0)
    {
      cluster.setOtherTopics(true);
    }
    for (int i = 0; i < labels; i++) {
      cluster.addPhrases(labelBase + "#" + (i + 1));
    }
    cluster.addDocuments(documents);
    return cluster;
  }
}

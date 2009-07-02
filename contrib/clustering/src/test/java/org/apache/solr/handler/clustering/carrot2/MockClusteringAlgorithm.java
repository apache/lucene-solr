package org.apache.solr.handler.clustering.carrot2;

import com.google.common.collect.Lists;
import org.carrot2.core.*;
import org.carrot2.core.attribute.AttributeNames;
import org.carrot2.core.attribute.Processing;
import org.carrot2.util.attribute.*;
import org.carrot2.util.attribute.constraint.IntRange;

import java.util.List;

@Bindable(prefix = "MockClusteringAlgorithm")
public class MockClusteringAlgorithm extends ProcessingComponentBase implements
        IClusteringAlgorithm {
  @Input
  @Processing
  @Attribute(key = AttributeNames.DOCUMENTS)
  private List<Document> documents;

  @Output
  @Processing
  @Attribute(key = AttributeNames.CLUSTERS)
  private List<Cluster> clusters;

  @Input
  @Processing
  @Attribute
  @IntRange(min = 1, max = 5)
  private int depth = 2;

  @Input
  @Processing
  @Attribute
  @IntRange(min = 1, max = 5)
  private int labels = 1;

  @Override
  public void process() throws ProcessingException {
    clusters = Lists.newArrayList();
    if (documents == null) {
      return;
    }

    int documentIndex = 1;
    for (Document document : documents) {
      StringBuilder label = new StringBuilder("Cluster " + documentIndex);
      Cluster cluster = createCluster(label.toString(), document);
      clusters.add(cluster);
      for (int i = 1; i <= depth; i++) {
        label.append(".");
        label.append(i);
        Cluster newCluster = createCluster(label.toString(), document);
        cluster.addSubclusters(createCluster(label.toString(), document), newCluster);
        cluster = newCluster;
      }
      documentIndex++;
    }
  }

  private Cluster createCluster(String labelBase, Document... documents) {
    Cluster cluster = new Cluster();
    for (int i = 0; i < labels; i++) {
      cluster.addPhrases(labelBase + "#" + (i + 1));
    }
    cluster.addDocuments(documents);
    return cluster;
  }
}

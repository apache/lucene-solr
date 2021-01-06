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
package org.apache.solr.handler.clustering;

import org.carrot2.attrs.AttrComposite;
import org.carrot2.attrs.AttrInteger;
import org.carrot2.clustering.Cluster;
import org.carrot2.clustering.ClusteringAlgorithm;
import org.carrot2.clustering.Document;
import org.carrot2.language.LanguageComponents;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Creates a stable set of synthetic clusters based on the provided parameters.
 */
public class MockClusteringAlgorithm extends AttrComposite implements ClusteringAlgorithm {
  public AttrInteger docsInCluster =
      attributes.register(
          "docsInCluster",
          AttrInteger.builder().label("Number of documents in each cluster.")
              .min(1)
              .max(5)
              .defaultValue(3));

  public AttrInteger hierarchyDepth =
      attributes.register(
          "hierarchyDepth",
          AttrInteger.builder().label("Levels of clusters hierarchy.")
              .min(1)
              .max(3)
              .defaultValue(2));

  public AttrInteger maxClusters =
      attributes.register(
          "maxClusters",
          AttrInteger.builder().label("Maximum number of clusters at each hierarchy level.")
              .min(2)
              .max(100)
              .defaultValue(3));

  public AttrInteger labelsPerCluster =
      attributes.register(
          "labelsPerCluster",
          AttrInteger.builder().label("Number of labels generated for each cluster.")
              .min(1)
              .max(5)
              .defaultValue(1));

  @Override
  public boolean supports(LanguageComponents languageComponents) {
    return true;
  }

  @Override
  public Set<Class<?>> requiredLanguageComponents() {
    return Collections.emptySet();
  }

  @Override
  public <T extends Document> List<Cluster<T>> cluster(Stream<? extends T> documentStream,
                                                       LanguageComponents languageComponents) {
    List<T> documents = documentStream.collect(Collectors.toList());
    if (docsInCluster.get() > documents.size()) {
      throw new AssertionError();
    }

    Supplier<T> docSupplier = new Supplier<>() {
      Iterator<T> i = documents.iterator();

      @Override
      public T get() {
        if (!i.hasNext()) {
          i = documents.iterator();
        }
        return i.next();
      }
    };

    return createClusters(hierarchyDepth.get(), "Cluster ", docSupplier);
  }

  private <T extends Document> List<Cluster<T>> createClusters(int level, String prefix,
                                                               Supplier<T> docSupplier) {
    ArrayList<Cluster<T>> clusters = new ArrayList<>();
    for (int count = maxClusters.get(), idx = 1; count > 0; count--, idx++) {
      String label = prefix + (prefix.endsWith(" ") ? "" : ".") + idx;

      Cluster<T> c = new Cluster<>();
      c.addLabel(label);
      for (int cnt = 1, max = labelsPerCluster.get(); cnt < max; cnt++) {
        c.addLabel("Label " + cnt);
      }
      c.setScore(level * count * 0.01);

      if (level == 1) {
        for (int j = docsInCluster.get(); j > 0; j--) {
          c.addDocument(docSupplier.get());
        }
      } else {
        createClusters(level - 1, label, docSupplier).forEach(c::addCluster);
      }

      clusters.add(c);
    }
    return clusters;
  }
}

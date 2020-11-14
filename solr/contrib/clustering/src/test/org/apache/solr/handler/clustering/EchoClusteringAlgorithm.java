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
import org.carrot2.clustering.Cluster;
import org.carrot2.clustering.ClusteringAlgorithm;
import org.carrot2.clustering.Document;
import org.carrot2.language.LanguageComponents;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

/**
 * Test-only pseudo clustering algorithm that creates
 * a cluster for each input document and sets the labels
 * of this cluster to the full content of clustered input
 * fields.
 */
public class EchoClusteringAlgorithm extends AttrComposite implements ClusteringAlgorithm {
  @Override
  public boolean supports(LanguageComponents languageComponents) {
    return true;
  }

  @Override
  public Set<Class<?>> requiredLanguageComponents() {
    return Collections.emptySet();
  }

  @Override
  public <T extends Document> List<Cluster<T>> cluster(Stream<? extends T> documentStream, LanguageComponents languageComponents) {
    List<Cluster<T>> clusters = new ArrayList<>();
    documentStream.forEach(document -> {
      final Cluster<T> cluster = new Cluster<>();
      cluster.addDocument(document);
      document.visitFields((field, value) -> {
        cluster.addLabel(field + ":" + value);
      });
      clusters.add(cluster);
    });

    return clusters;
  }
}

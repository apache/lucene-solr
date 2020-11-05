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
import org.carrot2.attrs.AttrString;
import org.carrot2.clustering.Cluster;
import org.carrot2.clustering.ClusteringAlgorithm;
import org.carrot2.clustering.Document;
import org.carrot2.language.LanguageComponents;
import org.carrot2.language.LexicalData;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Creates synthetic clusters with diagnostics of
 * {@link LanguageComponents} passed to the clustering method.
 */
class ResourceCheckAlgorithm extends AttrComposite implements ClusteringAlgorithm {
  public AttrString text =
      attributes.register(
          "text",
          AttrString.builder().label("Input text to analyze.")
              .defaultValue(null));

  @Override
  public Set<Class<?>> requiredLanguageComponents() {
    return Set.of(LexicalData.class);
  }

  @Override
  public <T extends Document> List<Cluster<T>> cluster(Stream<? extends T> documentStream,
                                                       LanguageComponents languageComponents) {
    ArrayList<Cluster<T>> clusters = new ArrayList<>();

    Cluster<T> cluster = new Cluster<>();
    cluster.addLabel("Lang: " + languageComponents.language());
    clusters.add(cluster);

    cluster = new Cluster<>();
    clusters.add(cluster);

    LexicalData lexicalData = languageComponents.get(LexicalData.class);
    cluster.addLabel(Arrays.stream(text.get().trim().split("[\\s]+"))
        .map(term -> String.format(Locale.ROOT,
            "%s[%s, %s]",
            term,
            lexicalData.ignoreWord(term) ? "ignoredWord" : "-",
            lexicalData.ignoreLabel(term) ? "ignoredLabel" : "-"))
        .collect(Collectors.joining(" ")));

    return clusters;
  }
}

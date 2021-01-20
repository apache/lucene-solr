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

import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.core.SolrCore;
import org.carrot2.clustering.Cluster;
import org.carrot2.clustering.ClusteringAlgorithm;
import org.carrot2.language.LanguageComponents;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Search results clustering engine based on Carrot2 clustering algorithms.
 *
 * @lucene.experimental
 * @see "https://project.carrot2.org"
 */
final class Engine {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  /**
   * All resources required for the clustering engine.
   */
  private EngineContext engineContext;

  boolean init(String engineName, SolrCore core, EngineParameters defaultParams) {
    log.info("Initializing clustering engine: {}", engineName);

    this.engineContext = new EngineContext(defaultParams.resources(), core);

    {
      ClusteringAlgorithm defaultAlgorithm = engineContext.getAlgorithm(defaultParams.algorithmName());
      LanguageComponents defaultLanguage = engineContext.getLanguage(defaultParams.language());

      if (defaultAlgorithm == null) {
        log.warn("The default clustering algorithm for engine '{}' is not available: {}",
            engineName, defaultParams.algorithmName());
      }

      if (defaultLanguage == null) {
        log.warn("The default language for engine {} is not available: {}",
            engineName, defaultParams.language());
      }

      return (defaultAlgorithm != null && defaultLanguage != null);
    }
  }

  List<Cluster<InputDocument>> cluster(EngineParameters parameters, Query query, List<InputDocument> documents) {
    try {
      checkParameters(parameters);

      ClusteringAlgorithm algorithm = engineContext.getAlgorithm(parameters.algorithmName());
      populateAlgorithmParameters(query, parameters, algorithm);

      // Sort documents by ID so that results are not order-sensitive.
      documents.sort(Comparator.comparing(a -> a.getId().toString()));

      // Split documents into language groups.
      String defaultLanguage = parameters.language();
      Map<String, List<InputDocument>> documentsByLanguage =
          documents.stream()
              .collect(
                  Collectors.groupingBy(
                      doc -> {
                        String lang = doc.language();
                        return lang == null ? defaultLanguage : lang;
                      }));

      // Cluster documents within each language group.
      HashSet<String> warnOnce = new HashSet<>();
      LinkedHashMap<String, List<Cluster<InputDocument>>> clustersByLanguage =
          new LinkedHashMap<>();
      for (Map.Entry<String, List<InputDocument>> e : documentsByLanguage.entrySet()) {
        String lang = e.getKey();
        if (!engineContext.isLanguageSupported(lang)) {
          if (warnOnce.add(lang)) {
            log.warn(
                "Language '{}' is not supported, documents in this "
                    + "language will not be clustered.", lang);
          }
        } else {
          LanguageComponents langComponents = engineContext.getLanguage(lang);
          if (!algorithm.supports(langComponents)) {
            if (warnOnce.add(lang)) {
              log.warn(
                  "Language '{}' is not supported by algorithm '{}', documents in this "
                      + "language will not be clustered.", lang, parameters.algorithmName());
            }
          } else {
            clustersByLanguage.put(
                lang, algorithm.cluster(e.getValue().stream(), langComponents));
          }
        }
      }

      List<Cluster<InputDocument>> clusters;
      if (clustersByLanguage.size() == 1) {
        clusters = clustersByLanguage.values().iterator().next();
      } else {
        clusters = clustersByLanguage.entrySet().stream()
            .map(e -> {
              Cluster<InputDocument> cluster = new Cluster<>();
              cluster.addLabel(e.getKey());
              e.getValue().forEach(cluster::addCluster);
              return cluster;
            })
            .collect(Collectors.toList());
      }

      return clusters;
    } catch (Exception e) {
      log.error("Clustering request failed.", e);
      throw new SolrException(ErrorCode.SERVER_ERROR, "Carrot2 clustering failed", e);
    }
  }

  private void populateAlgorithmParameters(Query query, EngineParameters requestParameters, ClusteringAlgorithm algorithm) {
    LinkedHashMap<String, String> attrs = requestParameters.otherParameters();
    // Set the optional query hint. We extract just the terms
    if (!attrs.containsKey("queryHint")) {
      Set<String> termSet = new LinkedHashSet<>();
      query.visit(new QueryVisitor() {
        @Override
        public void consumeTerms(Query query, Term... terms) {
          for (Term t : terms) {
            termSet.add(t.text());
          }
        }
      });
      attrs.put("queryHint", String.join(" ", termSet));
    }
    algorithm.accept(new FlatKeysAttrVisitor(attrs));
  }

  private void checkParameters(EngineParameters parameters) {
    ClusteringAlgorithm algorithm = engineContext.getAlgorithm(parameters.algorithmName());
    if (algorithm == null) {
      throw new SolrException(ErrorCode.BAD_REQUEST, String.format(Locale.ROOT,
          "Algorithm '%s' not found.",
          parameters.algorithmName()));
    }

    String defaultLanguage = parameters.language();
    LanguageComponents languageComponents = engineContext.getLanguage(defaultLanguage);
    if (languageComponents == null) {
      throw new SolrException(ErrorCode.BAD_REQUEST, String.format(Locale.ROOT,
          "Language '%s' is not supported.",
          defaultLanguage));
    }

    if (!algorithm.supports(languageComponents)) {
      throw new SolrException(ErrorCode.BAD_REQUEST, String.format(Locale.ROOT,
          "Language '%s' is not supported by algorithm '%s'.",
          defaultLanguage,
          parameters.algorithmName()));
    }

    if (parameters.fields().isEmpty()) {
      throw new SolrException(ErrorCode.BAD_REQUEST, String.format(Locale.ROOT,
          "At least one field name specifying content for clustering is required in parameter '%s'.",
          EngineParameters.PARAM_FIELDS));
    }
  }
}

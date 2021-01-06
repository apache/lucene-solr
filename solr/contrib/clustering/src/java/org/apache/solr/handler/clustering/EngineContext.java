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

import org.apache.solr.core.SolrCore;
import org.carrot2.clustering.ClusteringAlgorithm;
import org.carrot2.clustering.ClusteringAlgorithmProvider;
import org.carrot2.clustering.kmeans.BisectingKMeansClusteringAlgorithm;
import org.carrot2.clustering.lingo.LingoClusteringAlgorithm;
import org.carrot2.clustering.stc.STCClusteringAlgorithm;
import org.carrot2.language.LanguageComponents;
import org.carrot2.language.LanguageComponentsLoader;
import org.carrot2.language.LoadedLanguages;
import org.carrot2.util.ChainedResourceLookup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.invoke.MethodHandles;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Clustering engine context: algorithms, preloaded language
 * resources and initial validation.
 */
final class EngineContext {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final LinkedHashMap<String, LanguageComponents> languages;
  private final Map<String, ClusteringAlgorithmProvider> algorithmProviders;

  private final static Map<String, String> aliasedNames;

  static {
    aliasedNames = new HashMap<>();
    aliasedNames.put(LingoClusteringAlgorithm.class.getName(), LingoClusteringAlgorithm.NAME);
    aliasedNames.put(STCClusteringAlgorithm.class.getName(), STCClusteringAlgorithm.NAME);
    aliasedNames.put(BisectingKMeansClusteringAlgorithm.class.getName(), BisectingKMeansClusteringAlgorithm.NAME);
  }

  EngineContext(String resourcesPath, SolrCore core) {
    LanguageComponentsLoader loader = LanguageComponents.loader();

    List<Path> resourceLocations = new ArrayList<>();

    Path configDir = Paths.get(core.getResourceLoader().getConfigDir());
    if (resourcesPath != null && !resourcesPath.trim().isEmpty()) {
      configDir = configDir.resolve(resourcesPath);
      resourceLocations.add(configDir);
    }

    if (!resourceLocations.isEmpty()) {
      log.info(
          "Clustering algorithm resources first looked up relative to: {}", resourceLocations);

      loader.withResourceLookup(
          (provider) ->
              new ChainedResourceLookup(
                  Arrays.asList(
                      new PathResourceLookup(resourceLocations),
                      provider.defaultResourceLookup())));
    } else {
      log.info("Resources read from defaults (JARs).");
    }

    ClassLoader classLoader = getClass().getClassLoader();
    algorithmProviders =
        ServiceLoader.load(ClusteringAlgorithmProvider.class, classLoader)
            .stream()
            .map(ServiceLoader.Provider::get)
            .collect(Collectors.toMap(ClusteringAlgorithmProvider::name, e -> e));

    // Only load the resources of algorithms we're interested in.
    loader.limitToAlgorithms(
        algorithmProviders.values().stream()
            .map(Supplier::get)
            .toArray(ClusteringAlgorithm[]::new));

    languages = new LinkedHashMap<>();
    try {
      LoadedLanguages loadedLanguages = loader.load();
      for (String lang : loadedLanguages.languages()) {
        languages.put(lang, loadedLanguages.language(lang));
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }

    // Debug info about loaded languages.
    for (String lang : languages.keySet()) {
      if (log.isTraceEnabled()) {
        log.trace(
            "Loaded language '{}' with components:\n  - {}",
            lang,
            languages.get(lang).components().stream()
                .map(Class::getSimpleName)
                .collect(Collectors.joining("\n  - ")));
      }
    }

    // Remove algorithms for which there are no languages that are supported.
    algorithmProviders
        .entrySet()
        .removeIf(e -> !isAlgorithmAvailable(e.getValue(), languages.values()));

    algorithmProviders.forEach(
        (name, prov) -> {
          String supportedLanguages =
              languages.values().stream()
                  .filter(lc -> prov.get().supports(lc))
                  .map(LanguageComponents::language)
                  .collect(Collectors.joining(", "));

          log.info(
              "Clustering algorithm {} loaded with support for the following languages: {}",
              name,
              supportedLanguages);
        });
  }

  ClusteringAlgorithm getAlgorithm(String algorithmName) {
    if (!algorithmProviders.containsKey(algorithmName)
        && aliasedNames.containsKey(algorithmName)) {
      algorithmName = aliasedNames.get(algorithmName);
    }

    ClusteringAlgorithmProvider provider = algorithmProviders.get(algorithmName);
    return provider == null ? null : provider.get();
  }

  LanguageComponents getLanguage(String language) {
    return languages.get(language);
  }

  boolean isLanguageSupported(String language) {
    return languages.containsKey(language);
  }

  private boolean isAlgorithmAvailable(
      ClusteringAlgorithmProvider provider, Collection<LanguageComponents> languages) {
    ClusteringAlgorithm algorithm = provider.get();
    Optional<LanguageComponents> first = languages.stream().filter(algorithm::supports).findFirst();
    if (first.isEmpty()) {
      log.warn("Algorithm does not support any of the available languages: {}", provider.name());
      return false;
    } else {
      return true;
    }
  }
}
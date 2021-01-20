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

import org.apache.lucene.index.IndexableField;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TotalHits;
import org.apache.solr.client.solrj.response.ClusteringResponse;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.params.HighlightParams;
import org.apache.solr.common.params.ShardParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.component.HighlightComponent;
import org.apache.solr.handler.component.ResponseBuilder;
import org.apache.solr.handler.component.SearchComponent;
import org.apache.solr.handler.component.ShardRequest;
import org.apache.solr.highlight.SolrHighlighter;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.search.DocIterator;
import org.apache.solr.search.DocList;
import org.apache.solr.search.DocSlice;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.util.plugin.SolrCoreAware;
import org.carrot2.clustering.Cluster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * A {@link SearchComponent} for dynamic, unsupervised grouping of
 * search results based on the content of their text fields or contextual
 * snippets around query-matching regions.
 *
 * <p>
 * The default implementation uses clustering algorithms from the
 * <a href="https://project.carrot2.org">Carrot<sup>2</sup> project</a>.
 *
 * @lucene.experimental
 */
public class ClusteringComponent extends SearchComponent implements SolrCoreAware {
  /**
   * Default component name and parameter prefix.
   */
  public static final String COMPONENT_NAME = "clustering";

  /**
   * Request parameter that selects one of the {@link Engine} configurations
   * out of many possibly defined in the component's initialization parameters.
   */
  public static final String REQUEST_PARAM_ENGINE = COMPONENT_NAME + ".engine";

  /**
   * Engine configuration initialization block name.
   */
  public static final String INIT_SECTION_ENGINE = "engine";

  /**
   * Response section name containing output clusters.
   */
  public static final String RESPONSE_SECTION_CLUSTERS = "clusters";

  /**
   * Default log sink.
   */
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  /**
   * An internal request parameter for shard requests used for collecting
   * input documents for clustering.
   */
  private static final String REQUEST_PARAM_COLLECT_INPUTS = COMPONENT_NAME + ".collect-inputs";

  /**
   * Shard request response section name containing partial document inputs.
   */
  private static final String RESPONSE_SECTION_INPUT_DOCUMENTS = "clustering-inputs";

  /**
   * All engines declared in this component's initialization block.
   */
  private final List<EngineEntry> declaredEngines = new ArrayList<>();

  /**
   * Declaration-order list of available search clustering engines.
   */
  private final LinkedHashMap<String, EngineEntry> engines = new LinkedHashMap<>();

  private static boolean isComponentEnabled(ResponseBuilder rb) {
    return rb.req.getParams().getBool(COMPONENT_NAME, false);
  }

  private static List<InputDocument> documentsFromNamedList(List<NamedList<Object>> docList) {
    return docList.stream()
        .map(docProps -> {
          InputDocument doc = new InputDocument(
              docProps.get("id"),
              (String) docProps.get("language"));

          docProps.forEach((fieldName, value) -> {
            doc.addClusteredField(fieldName, (String) value);
          });
          doc.visitFields(docProps::add);
          return doc;
        })
        .collect(Collectors.toList());
  }

  private static List<NamedList<Object>> documentsToNamedList(List<InputDocument> documents) {
    return documents.stream()
        .map(doc -> {
          NamedList<Object> docProps = new SimpleOrderedMap<>();
          docProps.add("id", doc.getId());
          docProps.add("language", doc.language());
          doc.visitFields(docProps::add);
          return docProps;
        })
        .collect(Collectors.toList());
  }

  private static List<NamedList<Object>> clustersToNamedList(List<InputDocument> documents,
                                                             List<Cluster<InputDocument>> clusters,
                                                             EngineParameters params) {
    List<NamedList<Object>> result = new ArrayList<>();
    clustersToNamedListRecursive(clusters, result, params);

    if (params.includeOtherTopics()) {
      LinkedHashSet<InputDocument> clustered = new LinkedHashSet<>();
      clusters.forEach(cluster -> collectUniqueDocuments(cluster, clustered));
      List<InputDocument> unclustered = documents.stream()
          .filter(doc -> !clustered.contains(doc))
          .collect(Collectors.toList());

      if (!unclustered.isEmpty()) {
        NamedList<Object> cluster = new SimpleOrderedMap<>();
        result.add(cluster);
        cluster.add(ClusteringResponse.IS_OTHER_TOPICS, true);
        cluster.add(ClusteringResponse.LABELS_NODE, Collections.singletonList("Other topics"));
        cluster.add(ClusteringResponse.SCORE_NODE, 0d);
        cluster.add(ClusteringResponse.DOCS_NODE, unclustered.stream().map(InputDocument::getId)
            .collect(Collectors.toList()));
      }
    }

    return result;
  }

  private static void clustersToNamedListRecursive(
      List<Cluster<InputDocument>> outputClusters,
      List<NamedList<Object>> parent, EngineParameters params) {
    for (Cluster<InputDocument> cluster : outputClusters) {
      NamedList<Object> converted = new SimpleOrderedMap<>();
      parent.add(converted);

      // Add labels
      List<String> labels = cluster.getLabels();
      if (labels.size() > params.maxLabels()) {
        labels = labels.subList(0, params.maxLabels());
      }
      converted.add(ClusteringResponse.LABELS_NODE, labels);

      // Add cluster score
      final Double score = cluster.getScore();
      if (score != null) {
        converted.add(ClusteringResponse.SCORE_NODE, score);
      }

      List<InputDocument> docs;
      if (params.includeSubclusters()) {
        docs = cluster.getDocuments();
      } else {
        docs = new ArrayList<>(collectUniqueDocuments(cluster, new LinkedHashSet<>()));
      }

      converted.add(ClusteringResponse.DOCS_NODE, docs.stream().map(InputDocument::getId)
          .collect(Collectors.toList()));

      if (params.includeSubclusters() && !cluster.getClusters().isEmpty()) {
        List<NamedList<Object>> subclusters = new ArrayList<>();
        converted.add(ClusteringResponse.CLUSTERS_NODE, subclusters);
        clustersToNamedListRecursive(cluster.getClusters(), subclusters, params);
      }
    }
  }

  private static LinkedHashSet<InputDocument> collectUniqueDocuments(Cluster<InputDocument> cluster, LinkedHashSet<InputDocument> unique) {
    unique.addAll(cluster.getDocuments());
    for (Cluster<InputDocument> sub : cluster.getClusters()) {
      collectUniqueDocuments(sub, unique);
    }
    return unique;
  }

  @Override
  @SuppressWarnings({"rawtypes", "unchecked"})
  public void init(NamedList args) {
    super.init(args);

    if (args != null) {
      @SuppressWarnings("unchecked")
      NamedList<Object> initParams = (NamedList<Object>) args;
      for (Map.Entry<String, Object> entry : initParams) {
        if (!INIT_SECTION_ENGINE.equals(entry.getKey())) {
          throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
              "Unrecognized configuration entry: " + entry.getKey());
        }

        declaredEngines.add(new EngineEntry(((NamedList<Object>) entry.getValue()).toSolrParams()));
      }
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public void inform(SolrCore core) {
    declaredEngines.forEach(engineEntry -> {
      if (!engineEntry.initialize(core)) {
        if (engineEntry.optional) {
          if (log.isInfoEnabled()) {
            log.info("Optional clustering engine is not available: {}", engineEntry.engineName);
          }
        } else {
          throw new SolrException(ErrorCode.SERVER_ERROR,
              "A required clustering engine failed to initialize, check the logs: " + engineEntry.engineName);
        }
      } else {
        if (engines.put(engineEntry.engineName, engineEntry) != null) {
          throw new SolrException(ErrorCode.SERVER_ERROR,
              String.format(Locale.ROOT,
                  "Duplicate clustering engine named '%s'.", engineEntry.engineName));
        }
      }
    });

    if (engines.size() > 0) {
      if (log.isInfoEnabled()) {
        log.info("The following clustering engines are available: {}",
            String.join(", ", engines.keySet()));
      }
    } else {
      log.warn("No clustering engines are available.");
    }
  }

  @Override
  public void prepare(ResponseBuilder rb) {
    // Do nothing.
  }

  /**
   * Entry point for clustering in local server mode (non-distributed).
   *
   * @param rb The {@link ResponseBuilder}.
   * @throws IOException Propagated if an I/O exception occurs.
   */
  @Override
  public void process(ResponseBuilder rb) throws IOException {
    if (!isComponentEnabled(rb)) {
      return;
    }

    EngineEntry engine = getEngine(rb);
    EngineParameters parameters = engine.defaults.derivedFrom(rb.req.getParams());

    List<InputDocument> inputs = getDocuments(rb, parameters);

    if (rb.req.getParams().getBool(ShardParams.IS_SHARD, false) &&
        rb.req.getParams().getBool(REQUEST_PARAM_COLLECT_INPUTS, false)) {
      rb.rsp.add(RESPONSE_SECTION_INPUT_DOCUMENTS, documentsToNamedList(inputs));
    } else {
      doCluster(rb, engine, inputs, parameters);
    }
  }

  @Override
  public void modifyRequest(ResponseBuilder rb, SearchComponent who, ShardRequest sreq) {
    if (!isComponentEnabled(rb)) {
      return;
    }

    // Make sure the component is enabled for shard request.
    assert sreq.params.getBool(COMPONENT_NAME, false) :
        "Shard request should propagate clustering component enabled state?";

    // Piggyback collecting inputs for clustering on top of get fields request.
    if ((sreq.purpose & ShardRequest.PURPOSE_GET_FIELDS) != 0) {
      sreq.params.set(REQUEST_PARAM_COLLECT_INPUTS, true);
    }
  }

  @Override
  public void finishStage(ResponseBuilder rb) {
    if (!isComponentEnabled(rb)) {
      return;
    }

    if (rb.stage == ResponseBuilder.STAGE_GET_FIELDS) {
      List<InputDocument> inputs = new ArrayList<>();
      rb.finished.stream()
          .filter(shardRequest -> (shardRequest.purpose & ShardRequest.PURPOSE_GET_FIELDS) != 0)
          .flatMap(shardRequest -> shardRequest.responses.stream())
          .filter(rsp -> rsp.getException() == null)
          .map(rsp -> rsp.getSolrResponse().getResponse())
          .forEach(response -> {
            @SuppressWarnings("unchecked")
            List<NamedList<Object>> partialInputs = (List<NamedList<Object>>) response.get(RESPONSE_SECTION_INPUT_DOCUMENTS);
            if (partialInputs != null) {
              inputs.addAll(documentsFromNamedList(partialInputs));
            }
          });

      EngineEntry engine = getEngine(rb);
      EngineParameters parameters = engine.defaults.derivedFrom(rb.req.getParams());
      doCluster(rb, engine, inputs, parameters);
    }
  }

  /**
   * Run clustering of input documents and append the result to the response.
   */
  private void doCluster(ResponseBuilder rb, EngineEntry engine, List<InputDocument> inputs, EngineParameters parameters) {
    // log.warn("# CLUSTERING: " + inputs.size() + " document(s), contents:\n - "
    //   + inputs.stream().map(Object::toString).collect(Collectors.joining("\n - ")));
    List<Cluster<InputDocument>> clusters = engine.get().cluster(parameters, rb.getQuery(), inputs);
    rb.rsp.add(RESPONSE_SECTION_CLUSTERS, clustersToNamedList(inputs, clusters, parameters));
  }

  /**
   * Prepares input documents for clustering.
   */
  private List<InputDocument> getDocuments(ResponseBuilder responseBuilder,
                                           EngineParameters requestParameters) throws IOException {

    SolrQueryRequest solrRequest = responseBuilder.req;
    Query query = responseBuilder.getQuery();
    SolrIndexSearcher indexSearcher = responseBuilder.req.getSearcher();
    SolrCore core = solrRequest.getCore();
    String[] fieldsToCluster = requestParameters.fields().toArray(String[]::new);
    IndexSchema schema = indexSearcher.getSchema();

    boolean preferQueryContext = requestParameters.preferQueryContext();
    SolrQueryRequest req = null;
    SolrHighlighter highlighter = null;
    if (preferQueryContext) {
      highlighter = ((HighlightComponent) core.getSearchComponents().get(HighlightComponent.COMPONENT_NAME)).getHighlighter();
      if (highlighter != null) {
        Map<String, Object> args = new HashMap<>();
        args.put(HighlightParams.FIELDS, fieldsToCluster);
        args.put(HighlightParams.HIGHLIGHT, "true");
        // We don't want any highlight marks.
        args.put(HighlightParams.SIMPLE_PRE, "");
        args.put(HighlightParams.SIMPLE_POST, "");
        args.put(HighlightParams.FRAGSIZE, requestParameters.contextSize());
        args.put(HighlightParams.SNIPPETS, requestParameters.contextCount());
        req = new LocalSolrQueryRequest(core, query.toString(), "", 0, 1, args) {
          @Override
          public SolrIndexSearcher getSearcher() {
            return indexSearcher;
          }
        };
      } else {
        log.warn("No highlighter configured, cannot produce summary");
        preferQueryContext = false;
      }
    }

    Map<String, Function<IndexableField, String>> fieldsToLoad = new LinkedHashMap<>();
    for (String fld : requestParameters.getFieldsToLoad()) {
      FieldType type = schema.getField(fld).getType();
      fieldsToLoad.put(fld, (fieldValue) -> type.toObject(fieldValue).toString());
    }

    Function<Map<String, String>, String> docLanguage;
    String languageField = requestParameters.languageField();
    if (languageField != null) {
      docLanguage = (doc) -> doc.getOrDefault(languageField, requestParameters.language());
    } else {
      docLanguage = (doc) -> requestParameters.language();
    }

    List<InputDocument> result = new ArrayList<>();
    DocIterator it = responseBuilder.getResults().docList.iterator();
    while (it.hasNext()) {
      int docId = it.nextDoc();

      Map<String, String> docFieldValues = new LinkedHashMap<>();
      for (IndexableField indexableField : indexSearcher.doc(docId, fieldsToLoad.keySet())) {
        String fieldName = indexableField.name();
        Function<IndexableField, String> toString = fieldsToLoad.get(fieldName);
        if (toString != null) {
          String value = toString.apply(indexableField);
          docFieldValues.compute(fieldName, (k, v) -> {
            if (v == null) {
              return value;
            } else {
              return v + " . " + value;
            }
          });
        }
      }

      InputDocument inputDocument = new InputDocument(
          docFieldValues.get(requestParameters.docIdField()),
          docLanguage.apply(docFieldValues));
      result.add(inputDocument);

      Function<String, String> snippetProvider = (field) -> null;
      if (preferQueryContext) {
        DocList docAsList = new DocSlice(0, 1,
            new int[]{docId},
            new float[]{1.0f},
            1,
            1.0f,
            TotalHits.Relation.EQUAL_TO);

        NamedList<Object> highlights = highlighter.doHighlighting(docAsList, query, req, fieldsToCluster);
        if (highlights != null && highlights.size() == 1) {
          @SuppressWarnings("unchecked")
          NamedList<String[]> tmp = (NamedList<String[]>) highlights.getVal(0);
          snippetProvider = (field) -> {
            String[] values = tmp.get(field);
            if (values == null) {
              return null;
            } else {
              return String.join(" . ", Arrays.asList(values));
            }
          };
        }
      }

      Function<String, String> fullValueProvider = docFieldValues::get;

      for (String field : fieldsToCluster) {
        String values = snippetProvider.apply(field);
        if (values == null) {
          values = fullValueProvider.apply(field);
        }
        if (values != null) {
          inputDocument.addClusteredField(field, values);
        }
      }
    }

    return result;
  }

  private EngineEntry getEngine(ResponseBuilder rb) {
    if (engines.isEmpty()) {
      throw new SolrException(ErrorCode.SERVER_ERROR,
          "No clustering engines are defined or loaded.");
    }

    EngineEntry engine;
    String name = rb.req.getParams().get(REQUEST_PARAM_ENGINE, null);
    if (name != null) {
      engine = engines.get(name);
      if (engine == null) {
        throw new SolrException(ErrorCode.SERVER_ERROR,
            "Clustering engine unknown or not loaded: " + name);
      }
    } else {
      engine = engines.values().iterator().next();
    }
    return engine;
  }

  /**
   * @return A map of initialized clustering engines, exposed for tests only.
   */
  Set<String> getEngineNames() {
    return engines.keySet();
  }

  @Override
  public String getDescription() {
    return "Search results clustering component";
  }
}

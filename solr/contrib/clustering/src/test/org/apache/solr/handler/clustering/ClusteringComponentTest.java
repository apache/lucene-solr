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

import com.carrotsearch.randomizedtesting.RandomizedContext;
import org.apache.commons.io.FileUtils;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.response.ClusteringResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.component.SearchHandler;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.ResultContext;
import org.apache.solr.response.SolrQueryResponse;
import org.carrot2.clustering.Cluster;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Tests {@link Engine}.
 */
public class ClusteringComponentTest extends SolrTestCaseJ4 {
  private final static String QUERY_TESTSET_SAMPLE_DOCUMENTS = "testSet:sampleDocs";

  @BeforeClass
  public static void beforeClass() throws Exception {
    File testHome = createTempDir().toFile();
    FileUtils.copyDirectory(getFile("clustering/solr"), testHome);
    initCore("solrconfig.xml", "schema.xml", testHome.getAbsolutePath());

    String[] languages = {
        "English",
        "French",
        "German",
        "Unknown",
    };

    int docId = 0;
    for (String[] doc : SampleData.SAMPLE_DOCUMENTS) {
      assertNull(h.validateUpdate(adoc(
          "id", Integer.toString(docId),
          "title", doc[0],
          "snippet", doc[1],
          "testSet", "sampleDocs",
          "lang", languages[docId % languages.length])));
      docId++;
    }

    assertNull(h.validateUpdate(commit()));
  }

  @Test
  public void testLingoAlgorithm() throws Exception {
    compareToExpected(clusters("lingo", QUERY_TESTSET_SAMPLE_DOCUMENTS));
  }

  @Test
  public void testStcAlgorithm() throws Exception {
    compareToExpected(clusters("stc", QUERY_TESTSET_SAMPLE_DOCUMENTS));
  }

  @Test
  public void testKmeansAlgorithm() throws Exception {
    compareToExpected(clusters("kmeans", QUERY_TESTSET_SAMPLE_DOCUMENTS));
  }

  @Test
  public void testParamSubclusters() throws Exception {
    compareToExpected("off", clusters("mock", QUERY_TESTSET_SAMPLE_DOCUMENTS, params -> {
      params.set(EngineParameters.PARAM_INCLUDE_SUBCLUSTERS, false);
    }));
    compareToExpected("on", clusters("mock", QUERY_TESTSET_SAMPLE_DOCUMENTS, params -> {
      params.set(EngineParameters.PARAM_INCLUDE_SUBCLUSTERS, true);
    }));
  }

  @Test
  public void testParamOtherTopics() throws Exception {
    compareToExpected(clusters("mock", QUERY_TESTSET_SAMPLE_DOCUMENTS, params -> {
      params.set(EngineParameters.PARAM_INCLUDE_OTHER_TOPICS, false);
    }));
  }

  /**
   * We'll make two queries, one with- and another one without summary
   * and assert that documents are shorter when highlighter is in use.
   */
  @Test
  public void testClusteringOnHighlights() throws Exception {
    String query = "+snippet:mine +" + QUERY_TESTSET_SAMPLE_DOCUMENTS;

    Consumer<ModifiableSolrParams> common = params -> {
      params.add(EngineParameters.PARAM_FIELDS, "title, snippet");
      params.add(EngineParameters.PARAM_CONTEXT_SIZE, Integer.toString(80));
      params.add(EngineParameters.PARAM_CONTEXT_COUNT, Integer.toString(1));
    };

    List<Cluster<SolrDocument>> highlighted = clusters("echo", query,
        common.andThen(params -> {
          params.add(EngineParameters.PARAM_PREFER_QUERY_CONTEXT, "true");
        }));

    List<Cluster<SolrDocument>> full = clusters("echo", query,
        common.andThen(params -> {
          params.add(EngineParameters.PARAM_PREFER_QUERY_CONTEXT, "false");
        }));

    // Echo clustering algorithm just returns document fields as cluster labels
    // so highlighted snippets should never be longer than full field content.
    Assert.assertEquals(highlighted.size(), full.size());
    for (int i = 0; i < highlighted.size(); i++) {
      List<String> labels1 = highlighted.get(i).getLabels();
      List<String> labels2 = full.get(i).getLabels();
      assertEquals(labels1.size(), labels2.size());
      for (int j = 0; j < labels1.size(); j++) {
        MatcherAssert.assertThat("Summary shorter than original document?",
            labels1.get(j).length(),
            Matchers.lessThanOrEqualTo(labels2.get(j).length()));
      }
    }
  }

  /**
   * We'll make two queries, one short summaries and another one with longer
   * summaries and will check that the results differ.
   */
  @Test
  public void testSummaryFragSize() throws Exception {
    String query = "+snippet:mine +" + QUERY_TESTSET_SAMPLE_DOCUMENTS;

    Consumer<ModifiableSolrParams> common = params -> {
      params.add(EngineParameters.PARAM_PREFER_QUERY_CONTEXT, "true");
      params.add(EngineParameters.PARAM_FIELDS, "title, snippet");
      params.add(EngineParameters.PARAM_CONTEXT_COUNT, Integer.toString(1));
    };

    List<Cluster<SolrDocument>> shortSummaries = clusters("echo", query,
        common.andThen(params -> {
          params.add(EngineParameters.PARAM_CONTEXT_SIZE, Integer.toString(30));
        }));

    List<Cluster<SolrDocument>> longSummaries = clusters("echo", query,
        common.andThen(params -> {
          params.add(EngineParameters.PARAM_CONTEXT_COUNT, Integer.toString(80));
        }));

    Assert.assertEquals(shortSummaries.size(), longSummaries.size());
    for (int i = 0; i < shortSummaries.size(); i++) {
      List<String> shortLabels = shortSummaries.get(i).getLabels();
      List<String> longLabels = longSummaries.get(i).getLabels();
      assertEquals(shortLabels.size(), longLabels.size());
      for (int j = 0; j < shortLabels.size(); j++) {
        MatcherAssert.assertThat("Shorter summary is longer than longer summary?",
            shortLabels.get(j).length(),
            Matchers.lessThanOrEqualTo(longLabels.get(j).length()));
      }
    }
  }

  /**
   * Test passing algorithm parameters via SolrParams.
   */
  @Test
  public void testPassingAttributes() throws Exception {
    compareToExpected(clusters("mock", QUERY_TESTSET_SAMPLE_DOCUMENTS, params -> {
      params.set("maxClusters", 2);
      params.set("hierarchyDepth", 1);
      params.add(EngineParameters.PARAM_INCLUDE_OTHER_TOPICS, "false");
    }));
  }

  /**
   * Test passing algorithm parameters via Solr configuration file.
   */
  @Test
  public void testPassingAttributesViaSolrConfig() throws Exception {
    compareToExpected(clusters("mock-solrconfig-attrs", QUERY_TESTSET_SAMPLE_DOCUMENTS));
  }

  /**
   * Test maximum label truncation.
   */
  @Test
  public void testParamMaxLabels() throws Exception {
    List<Cluster<SolrDocument>> clusters = clusters("mock", QUERY_TESTSET_SAMPLE_DOCUMENTS, params -> {
      params.set("labelsPerCluster", "5");
      params.set(EngineParameters.PARAM_INCLUDE_OTHER_TOPICS, "false");
      params.set(EngineParameters.PARAM_MAX_LABELS, "3");
    });

    clusters.forEach(c -> {
      MatcherAssert.assertThat(c.getLabels(), Matchers.hasSize(3));
    });
  }

  @Test
  public void testCustomLanguageResources() throws Exception {
    compareToExpected(clusters(
        "testCustomLanguageResources",
        QUERY_TESTSET_SAMPLE_DOCUMENTS));
  }

  @Test
  public void testParamDefaultLanguage() throws Exception {
    compareToExpected(clusters(
        "testParamDefaultLanguage",
        QUERY_TESTSET_SAMPLE_DOCUMENTS));
  }

  /**
   * Verify that documents with an explicit language name
   * field are clustered in separate batches.
   *
   * @see EngineParameters#PARAM_LANGUAGE_FIELD
   */
  @Test
  public void testParamLanguageField() throws Exception {
    compareToExpected(clusters(
        "testParamLanguageField",
        QUERY_TESTSET_SAMPLE_DOCUMENTS));
  }

  private void compareToExpected(List<Cluster<SolrDocument>> clusters) throws IOException {
    compareToExpected("", clusters);
  }

  private void compareToExpected(String resourceSuffix,
                                 List<Cluster<SolrDocument>> clusters) throws IOException {
    String actual = toString(clusters);
    String expected = getTestResource(getClass(), resourceSuffix);
    compareWhitespaceNormalized(actual, expected);
  }

  static void compareWhitespaceNormalized(String actual, String expected) {
    Function<String, String> normalize = v -> v.replaceAll("\r", "").replaceAll("[ \t]+", " ").trim();

    if (!normalize.apply(expected).equals(normalize.apply(actual))) {
      throw new AssertionError(String.format(Locale.ROOT,
          "The actual clusters structure differs from the expected one. Expected:\n%s\n\nActual:\n%s",
          expected,
          actual));
    }
  }

  static String getTestResource(Class<?> clazz, String expectedResourceSuffix) throws IOException {
    RandomizedContext ctx = RandomizedContext.current();
    String resourceName = String.format(Locale.ROOT,
        "%s-%s%s.txt",
        ctx.getTargetClass().getSimpleName(),
        ctx.getTargetMethod().getName(),
        expectedResourceSuffix.isEmpty() ? "" : "-" + expectedResourceSuffix);

    String expected;
    try (InputStream is = clazz.getResourceAsStream(resourceName)) {
      if (is == null) {
        throw new AssertionError("Test resource not found: " + resourceName + " (class-relative to " +
            clazz.getName() + ")");
      }

      expected = new String(is.readAllBytes(), StandardCharsets.UTF_8);
    }
    return expected;
  }

  private String toString(List<Cluster<SolrDocument>> clusters) {
    return toString(clusters, "", new StringBuilder()).toString();
  }

  private StringBuilder toString(List<Cluster<SolrDocument>> clusters, String indent, StringBuilder sb) {
    clusters.forEach(c -> {
      sb.append(indent);
      sb.append("- " + c.getLabels().stream().collect(Collectors.joining("; ")));
      if (!c.getDocuments().isEmpty()) {
        sb.append(" [" + c.getDocuments().size() + "]");
      }
      sb.append("\n");

      if (!c.getClusters().isEmpty()) {
        toString(c.getClusters(), indent + "  ", sb);
      }
    });
    return sb;
  }

  private List<Cluster<SolrDocument>> clusters(String engineName, String query, Consumer<ModifiableSolrParams> paramsConsumer) {
    return clusters("/select", engineName, query, paramsConsumer);
  }

  private List<Cluster<SolrDocument>> clusters(String engineName, String query) {
    return clusters("/select", engineName, query, params -> {
    });
  }

  private List<Cluster<SolrDocument>> clusters(String handlerName, String engineName, String query,
                                               Consumer<ModifiableSolrParams> paramsConsumer) {
    SolrCore core = h.getCore();

    ModifiableSolrParams reqParams = new ModifiableSolrParams();
    reqParams.add(ClusteringComponent.COMPONENT_NAME, "true");
    reqParams.add(ClusteringComponent.REQUEST_PARAM_ENGINE, engineName);
    reqParams.add(CommonParams.Q, query);
    reqParams.add(CommonParams.ROWS, "1000");
    paramsConsumer.accept(reqParams);

    SearchHandler handler = (SearchHandler) core.getRequestHandler(handlerName);
    assertTrue("Clustering engine named '" + engineName + "' exists.", handler.getComponents().stream()
        .filter(c -> c instanceof ClusteringComponent)
        .flatMap(c -> ((ClusteringComponent) c).getEngineNames().stream())
        .anyMatch(localName -> Objects.equals(localName, engineName)));

    SolrQueryResponse rsp = new SolrQueryResponse();
    rsp.addResponseHeader(new SimpleOrderedMap<>());
    try (SolrQueryRequest req = new LocalSolrQueryRequest(core, reqParams)) {
      handler.handleRequest(req, rsp);
      NamedList<?> values = rsp.getValues();
      @SuppressWarnings("unchecked")
      List<NamedList<Object>> clusters = (List<NamedList<Object>>) values.get("clusters");

      String idField = core.getLatestSchema().getUniqueKeyField().getName();
      Map<String, SolrDocument> idToDoc = new HashMap<>();
      ResultContext resultContext = (ResultContext) rsp.getResponse();
      for (Iterator<SolrDocument> it = resultContext.getProcessedDocuments(); it.hasNext(); ) {
        SolrDocument doc = it.next();
        idToDoc.put(doc.getFirstValue(idField).toString(), doc);
      }

      return clusters.stream().map(c -> toCluster(c, idToDoc)).collect(Collectors.toList());
    }
  }

  @SuppressWarnings("unchecked")
  private Cluster<SolrDocument> toCluster(NamedList<Object> v, Map<String, SolrDocument> idToDoc) {
    Cluster<SolrDocument> c = new Cluster<>();
    v.forEach((key, value) -> {
      switch (key) {
        case ClusteringResponse.DOCS_NODE:
          ((List<String>) value).forEach(docId -> c.addDocument(idToDoc.get(docId)));
          break;
        case ClusteringResponse.LABELS_NODE:
          ((List<String>) value).forEach(c::addLabel);
          break;
        case ClusteringResponse.SCORE_NODE:
          c.setScore(((Number) value).doubleValue());
          break;
        case ClusteringResponse.CLUSTERS_NODE:
          ((List<NamedList<Object>>) value).forEach(sub -> {
            c.addCluster(toCluster(sub, idToDoc));
          });
          break;
        case ClusteringResponse.IS_OTHER_TOPICS:
          // Just ignore the attribute.
          break;
        default:
          throw new RuntimeException("Unknown output property " + key + " in cluster: " + v.jsonStr());
      }
    });
    return c;
  }
}

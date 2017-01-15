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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TermQuery;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.handler.clustering.AbstractClusteringTestCase;
import org.apache.solr.handler.clustering.ClusteringComponent;
import org.apache.solr.handler.clustering.ClusteringEngine;
import org.apache.solr.handler.clustering.SearchClusteringEngine;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.search.DocList;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.util.RefCounted;
import org.apache.solr.util.SolrPluginUtils;
import org.carrot2.clustering.lingo.LingoClusteringAlgorithm;
import org.carrot2.core.LanguageCode;
import org.carrot2.util.attribute.AttributeUtils;
import org.junit.Test;

/**
 *
 */
public class CarrotClusteringEngineTest extends AbstractClusteringTestCase {
  @Test
  public void testCarrotLingo() throws Exception {
    // Note: the expected number of clusters may change after upgrading Carrot2
    // due to e.g. internal improvements or tuning of Carrot2 clustering.
    final int expectedNumClusters = 10;
    checkEngine(getClusteringEngine("default"), expectedNumClusters);
  }

  @Test
  public void testProduceSummary() throws Exception {
    // We'll make two queries, one with- and another one without summary
    // and assert that documents are shorter when highlighter is in use.
    final List<NamedList<Object>> noSummaryClusters = clusterWithHighlighting(false, 80);
    final List<NamedList<Object>> summaryClusters = clusterWithHighlighting(true, 80);

    assertEquals("Equal number of clusters", noSummaryClusters.size(), summaryClusters.size());
    for (int i = 0; i < noSummaryClusters.size(); i++) {
      assertTrue("Summary shorter than original document", 
          getLabels(noSummaryClusters.get(i)).get(1).length() > 
          getLabels(summaryClusters.get(i)).get(1).length()); 
    }
  }
  
  @Test
  public void testSummaryFragSize() throws Exception {
    // We'll make two queries, one short summaries and another one with longer
    // summaries and will check that the results differ.
    final List<NamedList<Object>> shortSummaryClusters = clusterWithHighlighting(true, 30);
    final List<NamedList<Object>> longSummaryClusters = clusterWithHighlighting(true, 80);
    
    assertEquals("Equal number of clusters", shortSummaryClusters.size(), longSummaryClusters.size());
    for (int i = 0; i < shortSummaryClusters.size(); i++) {
      assertTrue("Summary shorter than original document", 
          getLabels(shortSummaryClusters.get(i)).get(1).length() < 
      getLabels(longSummaryClusters.get(i)).get(1).length()); 
    }
  }

  private List<NamedList<Object>> clusterWithHighlighting(
      boolean enableHighlighting, int fragSize) throws IOException {
    // Some documents don't have mining in the snippet
    return clusterWithHighlighting(enableHighlighting, fragSize, 1, "mine", numberOfDocs - 7);
  }

  private List<NamedList<Object>> clusterWithHighlighting(
      boolean enableHighlighting, int fragSize, int summarySnippets,
      String term, int expectedNumDocuments) throws IOException {
    
    final TermQuery query = new TermQuery(new Term("snippet", term));

    final ModifiableSolrParams summaryParams = new ModifiableSolrParams();
    summaryParams.add(CarrotParams.SNIPPET_FIELD_NAME, "snippet");
    summaryParams.add(CarrotParams.PRODUCE_SUMMARY,
        Boolean.toString(enableHighlighting));
    summaryParams
        .add(CarrotParams.SUMMARY_FRAGSIZE, Integer.toString(fragSize));
    summaryParams
        .add(CarrotParams.SUMMARY_SNIPPETS, Integer.toString(summarySnippets));
    final List<NamedList<Object>> summaryClusters = checkEngine(
        getClusteringEngine("echo"), expectedNumDocuments,
        expectedNumDocuments, query, summaryParams);
    
    return summaryClusters;
  }

  @Test
  public void testCarrotStc() throws Exception {
    checkEngine(getClusteringEngine("stc"), 3);
  }

  @Test
  public void testWithoutSubclusters() throws Exception {
    checkClusters(checkEngine(getClusteringEngine("mock"), AbstractClusteringTestCase.numberOfDocs),
        1, 1, 0);
  }

  @Test
  public void testExternalXmlAttributesFile() throws Exception {
    checkClusters(
        checkEngine(getClusteringEngine("mock-external-attrs"), 13),
        1, 4, 0);
  }

  @Test
  public void testWithSubclusters() throws Exception {
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set(CarrotParams.OUTPUT_SUB_CLUSTERS, true);
    checkClusters(checkEngine(getClusteringEngine("mock"), AbstractClusteringTestCase.numberOfDocs), 1, 1, 2);
  }

  @Test
  public void testNumDescriptions() throws Exception {
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set(AttributeUtils.getKey(MockClusteringAlgorithm.class, "labels"), 5);
    params.set(CarrotParams.NUM_DESCRIPTIONS, 3);
    checkClusters(checkEngine(getClusteringEngine("mock"), AbstractClusteringTestCase.numberOfDocs,
            params), 1, 3, 0);
  }

  @Test
  public void testClusterScores() throws Exception {
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set(AttributeUtils.getKey(MockClusteringAlgorithm.class, "depth"), 1);
    List<NamedList<Object>> clusters = checkEngine(getClusteringEngine("mock"),
        AbstractClusteringTestCase.numberOfDocs, params);
    int i = 1;
    for (NamedList<Object> cluster : clusters) {
      final Double score = getScore(cluster);
      assertNotNull(score);
      assertEquals(0.25 * i++, score, 0);
    }
  }

  @Test
  public void testOtherTopics() throws Exception {
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set(AttributeUtils.getKey(MockClusteringAlgorithm.class, "depth"), 1);
    params.set(AttributeUtils.getKey(MockClusteringAlgorithm.class, "otherTopicsModulo"), 2);
    List<NamedList<Object>> clusters = checkEngine(getClusteringEngine("mock"),
        AbstractClusteringTestCase.numberOfDocs, params);
    int i = 1;
    for (NamedList<Object> cluster : clusters) {
      assertEquals(i++ % 2 == 0 ? true : null, isOtherTopics(cluster));
    }
  }

  @Test
  public void testCarrotAttributePassing() throws Exception {
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set(AttributeUtils.getKey(MockClusteringAlgorithm.class, "depth"), 1);
    params.set(AttributeUtils.getKey(MockClusteringAlgorithm.class, "labels"), 3);
    checkClusters(checkEngine(getClusteringEngine("mock"), AbstractClusteringTestCase.numberOfDocs,
            params), 1, 3, 0);
  }

  @Test
  public void testLexicalResourcesFromSolrConfigDefaultDir() throws Exception {
    checkLexicalResourcesFromSolrConfig("lexical-resource-check",
        "online,customsolrstopword,customsolrstoplabel");
  }

  @Test
  public void testLexicalResourcesFromSolrConfigCustomDir() throws Exception {
    checkLexicalResourcesFromSolrConfig("lexical-resource-check-custom-resource-dir",
        "online,customsolrstopwordcustomdir,customsolrstoplabelcustomdir");
  }

  private void checkLexicalResourcesFromSolrConfig(String engineName, String wordsToCheck)
      throws IOException {
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("merge-resources", false);
    params.set(AttributeUtils.getKey(
        LexicalResourcesCheckClusteringAlgorithm.class, "wordsToCheck"),
        wordsToCheck);

    // "customsolrstopword" is in stopwords.en, "customsolrstoplabel" is in
    // stoplabels.mt, so we're expecting only one cluster with label "online".
    final List<NamedList<Object>> clusters = checkEngine(
        getClusteringEngine(engineName), 1, params);
    assertEquals(getLabels(clusters.get(0)), Collections.singletonList("online"));
  }

  @Test
  public void testSolrStopWordsUsedInCarrot2Clustering() throws Exception {
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("merge-resources", false);
    params.set(AttributeUtils.getKey(
        LexicalResourcesCheckClusteringAlgorithm.class, "wordsToCheck"),
    "online,solrownstopword");

    // "solrownstopword" is in stopwords.txt, so we're expecting
    // only one cluster with label "online".
    final List<NamedList<Object>> clusters = checkEngine(
        getClusteringEngine("lexical-resource-check"), 1, params);
    assertEquals(getLabels(clusters.get(0)), Collections.singletonList("online"));
  }

  @Test
  public void testSolrStopWordsNotDefinedOnAFieldForClustering() throws Exception {
    ModifiableSolrParams params = new ModifiableSolrParams();
    // Force string fields to be used for clustering. Does not make sense
    // in a real word, but does the job in the test.
    params.set(CarrotParams.TITLE_FIELD_NAME, "url");
    params.set(CarrotParams.SNIPPET_FIELD_NAME, "url");
    params.set("merge-resources", false);
    params.set(AttributeUtils.getKey(
        LexicalResourcesCheckClusteringAlgorithm.class, "wordsToCheck"),
    "online,solrownstopword");

    final List<NamedList<Object>> clusters = checkEngine(
        getClusteringEngine("lexical-resource-check"), 2, params);
    assertEquals(Collections.singletonList("online"), getLabels(clusters.get(0)));
    assertEquals(Collections.singletonList("solrownstopword"), getLabels(clusters.get(1)));
  }
  
  @Test
  public void testHighlightingOfMultiValueField() throws Exception {
    final String snippetWithoutSummary = getLabels(clusterWithHighlighting(
        false, 30, 3, "multi", 1).get(0)).get(1);
    assertTrue("Snippet contains first value", snippetWithoutSummary.contains("First"));
    assertTrue("Snippet contains second value", snippetWithoutSummary.contains("Second"));
    assertTrue("Snippet contains third value", snippetWithoutSummary.contains("Third"));

    final String snippetWithSummary = getLabels(clusterWithHighlighting(
        true, 30, 3, "multi", 1).get(0)).get(1);
    assertTrue("Snippet with summary shorter than full snippet",
        snippetWithoutSummary.length() > snippetWithSummary.length());
    assertTrue("Summary covers first value", snippetWithSummary.contains("First"));
    assertTrue("Summary covers second value", snippetWithSummary.contains("Second"));
    assertTrue("Summary covers third value", snippetWithSummary.contains("Third"));
  }
  
  @Test
  public void testConcatenatingMultipleFields() throws Exception {
    final ModifiableSolrParams params = new ModifiableSolrParams();
    params.add(CarrotParams.TITLE_FIELD_NAME, "title,heading");
    params.add(CarrotParams.SNIPPET_FIELD_NAME, "snippet,body");

    final List<String> labels = getLabels(checkEngine(
        getClusteringEngine("echo"), 1, 1, new TermQuery(new Term("body",
            "snippet")), params).get(0));
    assertTrue("Snippet contains third value", labels.get(0).contains("Title field"));
    assertTrue("Snippet contains third value", labels.get(0).contains("Heading field"));
    assertTrue("Snippet contains third value", labels.get(1).contains("Snippet field"));
    assertTrue("Snippet contains third value", labels.get(1).contains("Body field"));
  }

  @Test
  public void testHighlightingMultipleFields() throws Exception {
    final TermQuery query = new TermQuery(new Term("snippet", "content"));

    final ModifiableSolrParams params = new ModifiableSolrParams();
    params.add(CarrotParams.TITLE_FIELD_NAME, "title,heading");
    params.add(CarrotParams.SNIPPET_FIELD_NAME, "snippet,body");
    params.add(CarrotParams.PRODUCE_SUMMARY, Boolean.toString(false));
    
    final String snippetWithoutSummary = getLabels(checkEngine(
        getClusteringEngine("echo"), 1, 1, query, params).get(0)).get(1);
    assertTrue("Snippet covers snippet field", snippetWithoutSummary.contains("snippet field"));
    assertTrue("Snippet covers body field", snippetWithoutSummary.contains("body field"));

    params.set(CarrotParams.PRODUCE_SUMMARY, Boolean.toString(true));
    params.add(CarrotParams.SUMMARY_FRAGSIZE, Integer.toString(30));
    params.add(CarrotParams.SUMMARY_SNIPPETS, Integer.toString(2));
    final String snippetWithSummary = getLabels(checkEngine(
        getClusteringEngine("echo"), 1, 1, query, params).get(0)).get(1);    
    assertTrue("Snippet with summary shorter than full snippet",
        snippetWithoutSummary.length() > snippetWithSummary.length());
    assertTrue("Snippet covers snippet field", snippetWithSummary.contains("snippet field"));
    assertTrue("Snippet covers body field", snippetWithSummary.contains("body field"));

  }

  @Test
  public void testOneCarrot2SupportedLanguage() throws Exception {
    final ModifiableSolrParams params = new ModifiableSolrParams();
    params.add(CarrotParams.LANGUAGE_FIELD_NAME, "lang");

    final List<String> labels = getLabels(checkEngine(
        getClusteringEngine("echo"), 1, 1, new TermQuery(new Term("url",
            "one_supported_language")), params).get(0));
    assertEquals(3, labels.size());
    assertEquals("Correct Carrot2 language", LanguageCode.CHINESE_SIMPLIFIED.name(), labels.get(2));
  }
  
  @Test
  public void testOneCarrot2SupportedLanguageOfMany() throws Exception {
    final ModifiableSolrParams params = new ModifiableSolrParams();
    params.add(CarrotParams.LANGUAGE_FIELD_NAME, "lang");
    
    final List<String> labels = getLabels(checkEngine(
        getClusteringEngine("echo"), 1, 1, new TermQuery(new Term("url",
            "one_supported_language_of_many")), params).get(0));
    assertEquals(3, labels.size());
    assertEquals("Correct Carrot2 language", LanguageCode.GERMAN.name(), labels.get(2));
  }
  
  @Test
  public void testLanguageCodeMapping() throws Exception {
    final ModifiableSolrParams params = new ModifiableSolrParams();
    params.add(CarrotParams.LANGUAGE_FIELD_NAME, "lang");
    params.add(CarrotParams.LANGUAGE_CODE_MAP, "POLISH:pl");
    
    final List<String> labels = getLabels(checkEngine(
        getClusteringEngine("echo"), 1, 1, new TermQuery(new Term("url",
            "one_supported_language_of_many")), params).get(0));
    assertEquals(3, labels.size());
    assertEquals("Correct Carrot2 language", LanguageCode.POLISH.name(), labels.get(2));
  }
  
  @Test
  public void testPassingOfCustomFields() throws Exception {
    final ModifiableSolrParams params = new ModifiableSolrParams();
    params.add(CarrotParams.CUSTOM_FIELD_NAME, "intfield_i:intfield");
    params.add(CarrotParams.CUSTOM_FIELD_NAME, "floatfield_f:floatfield");
    params.add(CarrotParams.CUSTOM_FIELD_NAME, "heading:multi");
    
    // Let the echo mock clustering algorithm know which custom field to echo
    params.add("custom-fields", "intfield,floatfield,multi");
    
    final List<String> labels = getLabels(checkEngine(
        getClusteringEngine("echo"), 1, 1, new TermQuery(new Term("url",
            "custom_fields")), params).get(0));
    assertEquals(5, labels.size());
    assertEquals("Integer field", "10", labels.get(2));
    assertEquals("Float field", "10.5", labels.get(3));
    assertEquals("List field", "[first, second]", labels.get(4));
  }

  @Test
  public void testCustomTokenizer() throws Exception {
    final ModifiableSolrParams params = new ModifiableSolrParams();
    params.add(CarrotParams.TITLE_FIELD_NAME, "title");
    params.add(CarrotParams.SNIPPET_FIELD_NAME, "snippet");

    final List<String> labels = getLabels(checkEngine(
        getClusteringEngine("custom-duplicating-tokenizer"), 1, 15, new TermQuery(new Term("title",
            "field")), params).get(0));
    
    // The custom test tokenizer duplicates each token's text
    assertTrue("First token", labels.get(0).contains("TitleTitle"));
  }
  
  @Test
  public void testCustomStemmer() throws Exception {
    final ModifiableSolrParams params = new ModifiableSolrParams();
    params.add(CarrotParams.TITLE_FIELD_NAME, "title");
    params.add(CarrotParams.SNIPPET_FIELD_NAME, "snippet");
    
    final List<String> labels = getLabels(checkEngine(
        getClusteringEngine("custom-duplicating-stemmer"), 1, 12, new TermQuery(new Term("title",
            "field")), params).get(0));
    
    // The custom test stemmer duplicates and lowercases each token's text
    assertTrue("First token", labels.get(0).contains("titletitle"));
  }

  @Test
  public void testDefaultEngineOrder() throws Exception {
    ClusteringComponent comp = (ClusteringComponent) h.getCore().getSearchComponent("clustering-name-default");
    Map<String,SearchClusteringEngine> engines = getSearchClusteringEngines(comp);
    assertEquals(
        Arrays.asList("stc", "default", "mock"),
        new ArrayList<>(engines.keySet()));
    assertEquals(
        LingoClusteringAlgorithm.class,
        ((CarrotClusteringEngine) engines.get(ClusteringEngine.DEFAULT_ENGINE_NAME)).getClusteringAlgorithmClass());
  }

  @Test
  public void testDeclarationEngineOrder() throws Exception {
    ClusteringComponent comp = (ClusteringComponent) h.getCore().getSearchComponent("clustering-name-decl-order");
    Map<String,SearchClusteringEngine> engines = getSearchClusteringEngines(comp);
    assertEquals(
        Arrays.asList("unavailable", "lingo", "stc", "mock", "default"),
        new ArrayList<>(engines.keySet()));
    assertEquals(
        LingoClusteringAlgorithm.class,
        ((CarrotClusteringEngine) engines.get(ClusteringEngine.DEFAULT_ENGINE_NAME)).getClusteringAlgorithmClass());
  }

  @Test
  public void testDeclarationNameDuplicates() throws Exception {
    ClusteringComponent comp = (ClusteringComponent) h.getCore().getSearchComponent("clustering-name-dups");
    Map<String,SearchClusteringEngine> engines = getSearchClusteringEngines(comp);
    assertEquals(
        Arrays.asList("", "default"),
        new ArrayList<>(engines.keySet()));
    assertEquals(
        MockClusteringAlgorithm.class,
        ((CarrotClusteringEngine) engines.get(ClusteringEngine.DEFAULT_ENGINE_NAME)).getClusteringAlgorithmClass());
  }

  private CarrotClusteringEngine getClusteringEngine(String engineName) {
    ClusteringComponent comp = (ClusteringComponent) h.getCore()
            .getSearchComponent("clustering");
    assertNotNull("clustering component should not be null", comp);
    CarrotClusteringEngine engine = 
        (CarrotClusteringEngine) getSearchClusteringEngines(comp).get(engineName);
    assertNotNull("clustering engine for name: " + engineName
            + " should not be null", engine);
    return engine;
  }

  private List<NamedList<Object>> checkEngine(CarrotClusteringEngine engine,
                            int expectedNumClusters) throws IOException {
    return checkEngine(engine, numberOfDocs, expectedNumClusters, new MatchAllDocsQuery(), new ModifiableSolrParams());
  }

  private List<NamedList<Object>> checkEngine(CarrotClusteringEngine engine,
                            int expectedNumClusters, SolrParams clusteringParams) throws IOException {
    return checkEngine(engine, numberOfDocs, expectedNumClusters, new MatchAllDocsQuery(), clusteringParams);
  }


  private List<NamedList<Object>> checkEngine(CarrotClusteringEngine engine, int expectedNumDocs,
                           int expectedNumClusters, Query query, SolrParams clusteringParams) throws IOException {
    // Get all documents to cluster
    RefCounted<SolrIndexSearcher> ref = h.getCore().getSearcher();

    DocList docList;
    try {
      SolrIndexSearcher searcher = ref.get();
      docList = searcher.getDocList(query, (Query) null, new Sort(), 0,
              numberOfDocs);
      assertEquals("docList size", expectedNumDocs, docList.matches());

      ModifiableSolrParams solrParams = new ModifiableSolrParams();
      solrParams.add(clusteringParams);

      // Perform clustering
      LocalSolrQueryRequest req = new LocalSolrQueryRequest(h.getCore(), solrParams);
      Map<SolrDocument,Integer> docIds = new HashMap<>(docList.size());
      SolrDocumentList solrDocList = SolrPluginUtils.docListToSolrDocumentList( docList, searcher, engine.getFieldsToLoad(req), docIds );

      @SuppressWarnings("unchecked")
      List<NamedList<Object>> results = (List<NamedList<Object>>) engine.cluster(query, solrDocList, docIds, req);
      req.close();
      assertEquals("number of clusters: " + results, expectedNumClusters, results.size());
      checkClusters(results, false);
      return results;
    } finally {
      ref.decref();
    }
  }

  private void checkClusters(List<NamedList<Object>> results, int expectedDocCount,
                             int expectedLabelCount, int expectedSubclusterCount) {
    for (int i = 0; i < results.size(); i++) {
      NamedList<Object> cluster = results.get(i);
      checkCluster(cluster, expectedDocCount, expectedLabelCount,
              expectedSubclusterCount);
    }
  }

  private void checkClusters(List<NamedList<Object>> results, boolean hasSubclusters) {
    for (int i = 0; i < results.size(); i++) {
      checkCluster(results.get(i), hasSubclusters);
    }
  }

  private void checkCluster(NamedList<Object> cluster, boolean hasSubclusters) {
    List<Object> docs = getDocs(cluster);
    assertNotNull("docs is null and it shouldn't be", docs);
    for (int j = 0; j < docs.size(); j++) {
      Object id = docs.get(j);
      assertNotNull("id is null and it shouldn't be", id);
    }

    List<String> labels = getLabels(cluster);
    assertNotNull("labels is null but it shouldn't be", labels);

    if (hasSubclusters) {
      List<NamedList<Object>> subclusters = getSubclusters(cluster);
      assertNotNull("subclusters is null but it shouldn't be", subclusters);
    }
  }

  private void checkCluster(NamedList<Object> cluster, int expectedDocCount,
                            int expectedLabelCount, int expectedSubclusterCount) {
    checkCluster(cluster, expectedSubclusterCount > 0);
    assertEquals("number of docs in cluster", expectedDocCount,
            getDocs(cluster).size());
    assertEquals("number of labels in cluster", expectedLabelCount,
            getLabels(cluster).size());

    if (expectedSubclusterCount > 0) {
      List<NamedList<Object>> subclusters = getSubclusters(cluster);
      assertEquals("numClusters", expectedSubclusterCount, subclusters.size());
      assertEquals("number of subclusters in cluster",
              expectedSubclusterCount, subclusters.size());
    }
  }

  @SuppressWarnings("unchecked")
  private List<NamedList<Object>> getSubclusters(NamedList<Object> cluster) {
    return (List<NamedList<Object>>) cluster.get("clusters");
  }

  @SuppressWarnings("unchecked")
  private List<String> getLabels(NamedList<Object> cluster) {
    return (List<String>) cluster.get("labels");
  }

  private Double getScore(NamedList<Object> cluster) {
    return (Double) cluster.get("score");
  }

  private Boolean isOtherTopics(NamedList<Object> cluster) {
    return (Boolean)cluster.get("other-topics");
  }

  @SuppressWarnings("unchecked")
  private List<Object> getDocs(NamedList<Object> cluster) {
    return (List<Object>) cluster.get("docs");
  }
}

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

package org.apache.solr.client.solrj.io.stream;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.math3.distribution.NormalDistribution;
import org.apache.commons.math3.random.JDKRandomGenerator;
import org.apache.commons.math3.random.RandomGenerator;
import org.apache.commons.math3.util.Precision;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.io.SolrClientCache;
import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.handler.SolrDefaultStreamFactory;
import org.apache.solr.util.RTimer;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Verify auto-plist with rollup over a facet expression when using collection alias over multiple collections.
 */
@SolrTestCaseJ4.SuppressSSL
@LuceneTestCase.SuppressCodecs({"Lucene3x", "Lucene40", "Lucene41", "Lucene42", "Lucene45"})
public class ParallelFacetStreamOverAliasTest extends SolrCloudTestCase {

  private static final String ALIAS_NAME = "SOME_ALIAS_WITH_MANY_COLLS";

  private static final String id = "id";
  private static final int NUM_COLLECTIONS = 2;
  private static final int NUM_DOCS_PER_COLLECTION = 40;
  private static final int NUM_SHARDS_PER_COLLECTION = 4;
  private static final int CARD = 10;
  private static final RandomGenerator rand = new JDKRandomGenerator(5150);
  private static List<String> listOfCollections;

  @BeforeClass
  public static void setupCluster() throws Exception {
    System.setProperty("solr.tests.numeric.dv", "true");
    final RTimer timer = new RTimer();
    configureCluster(NUM_COLLECTIONS).withMetrics(false)
        .addConfig("conf", getFile("solrj").toPath().resolve("solr").resolve("configsets").resolve("streaming").resolve("conf"))
        .configure();
    cleanup();
    setupCollectionsAndAlias();
  }

  /**
   * setup the testbed with necessary collections, documents, and alias
   */
  public static void setupCollectionsAndAlias() throws Exception {

    final NormalDistribution[] dists = new NormalDistribution[CARD];
    for (int i = 0; i < dists.length; i++) {
      dists[i] = new NormalDistribution(rand, i + 1, 1d);
    }

    List<String> collections = new ArrayList<>(NUM_COLLECTIONS);
    List<Exception> errors = new LinkedList<>();
    Stream.iterate(1, n -> n + 1).limit(NUM_COLLECTIONS).forEach(colIdx -> {
      final String collectionName = "coll" + colIdx;
      collections.add(collectionName);
      try {
        CollectionAdminRequest.createCollection(collectionName, "conf", NUM_SHARDS_PER_COLLECTION, 1).process(cluster.getSolrClient());
        cluster.waitForActiveCollection(collectionName, NUM_SHARDS_PER_COLLECTION, NUM_SHARDS_PER_COLLECTION);

        // want a variable num of docs per collection so that avg of avg does not work ;-)
        final int numDocsInColl = colIdx % 2 == 0 ? NUM_DOCS_PER_COLLECTION / 2 : NUM_DOCS_PER_COLLECTION;
        final int limit = NUM_COLLECTIONS == 1 ? NUM_DOCS_PER_COLLECTION * 2 : numDocsInColl;
        UpdateRequest ur = new UpdateRequest();
        Stream.iterate(0, n -> n + 1).limit(limit)
            .forEach(docId -> ur.add(id, UUID.randomUUID().toString(),
                "a_s", "hello" + docId, "a_i", String.valueOf(docId % CARD), "a_d", String.valueOf(dists[docId % dists.length].sample())));
        ur.commit(cluster.getSolrClient(), collectionName);
      } catch (SolrServerException | IOException e) {
        errors.add(e);
      }
    });

    if (!errors.isEmpty()) {
      throw errors.get(0);
    }
    listOfCollections = collections;
    String aliasedCollectionString = String.join(",", collections);
    CollectionAdminRequest.createAlias(ALIAS_NAME, aliasedCollectionString).process(cluster.getSolrClient());
  }

  public static void cleanup() throws Exception {
    if (cluster != null && cluster.getSolrClient() != null) {
      // cleanup the alias and the collections behind it
      CollectionAdminRequest.deleteAlias(ALIAS_NAME).process(cluster.getSolrClient());
      if (listOfCollections != null) {
        listOfCollections.stream().map(CollectionAdminRequest::deleteCollection).forEach(c -> {
          try {
            c.process(cluster.getSolrClient());
          } catch (SolrServerException | IOException e) {
            e.printStackTrace();
          }
        });
      }
    }
  }

  @AfterClass
  public static void after() throws Exception {
    cleanup();
  }

  //@Ignore
  @Test
  public void testDrillOverAlias() throws Exception {

    /*
    for (int c = 0; c < CARD; c++) {
      SolrQuery query = new SolrQuery("a_i:" + c);
      query.setRows(1000);
      query.setFields("a_i", "a_d");
      query.addGetFieldStatistics("a_d");
      QueryResponse resp = cluster.getSolrClient().query(ALIAS_NAME, query);
      SolrDocumentList results = resp.getResults();
      System.out.println("For query " + query + ", found: " + results.getNumFound() + "; stats: " + resp.getFieldStatsInfo().get("a_d"));
    }
     */

    StreamContext streamContext = new StreamContext();
    SolrClientCache solrClientCache = new SolrClientCache();
    streamContext.setSolrClientCache(solrClientCache);

    String zkhost = cluster.getZkServer().getZkAddress();
    StreamFactory factory = new SolrDefaultStreamFactory().withCollectionZkHost(ALIAS_NAME, zkhost);

    String drillExpr = "" +
        "rollup(\n" +
        "  drill(\n" +
        "    SOME_ALIAS_WITH_MANY_COLLS,\n" +
        "    q=\"*:*\", \n" +
        "    fl=\"a_i,a_d\", \n" +
        "    sort=\"a_i asc\", \n" +
        "    rollup(\n" +
        "      input(), \n" +
        "      over=\"a_i\", \n" +
        "      sum(a_d), avg(a_d), min(a_d), max(a_d), count(*)" +
        "    )\n" +
        "  ),\n" +
        "  over=\"a_i\",\n" +
        "  sum(sum(a_d)), wsum(avg(a_d)), min(min(a_d)), max(max(a_d)), sum(count(*)),\n" +
        ")";

    TupleStream stream = factory.constructStream(drillExpr);
    stream.setStreamContext(streamContext);
    List<Tuple> tuples = getTuples(stream);
    /*
    for (Tuple t : tuples) {
      System.out.println(t.getFields());
    }
     */

    solrClientCache.close();
  }

  /**
   * Test parallelized calls to facet expression, one for each collection in the alias
   */
  @Test
  public void testParallelFacetOverAlias() throws Exception {

    StreamContext streamContext = new StreamContext();
    SolrClientCache solrClientCache = new SolrClientCache();
    streamContext.setSolrClientCache(solrClientCache);

    String facetExprTmpl = "" +
        "facet(\n" +
        "  SOME_ALIAS_WITH_MANY_COLLS, %s\n" +
        "  q=\"*:*\", \n" +
        "  fl=\"a_i\", \n" +
        "  sort=\"a_i asc\", \n" +
        "  buckets=\"a_i\", \n" +
        "  bucketSorts=\"count(*) asc\", \n" +
        "  bucketSizeLimit=10000, \n" +
        "  sum(a_d), avg(a_d), min(a_d), max(a_d), count(*)\n" +
        ")\n";

    String facetExpr = String.format(Locale.US, facetExprTmpl, "");

    String zkhost = cluster.getZkServer().getZkAddress();
    StreamFactory factory = new SolrDefaultStreamFactory().withDefaultZkHost(zkhost);
    final RTimer timer = new RTimer();
    TupleStream stream = factory.constructStream(facetExpr);
    stream.setStreamContext(streamContext);
    List<Tuple> plistTuples = getTuples(stream);

    assertEquals(CARD, plistTuples.size());

    //for (Tuple t : plistTuples) {
    //  System.out.println(t.getFields());
    //}

    // now re-execute the same expression w/o plist
    facetExpr = String.format(Locale.US, facetExprTmpl, "plist=false,");
    stream = factory.constructStream(facetExpr);
    stream.setStreamContext(streamContext);
    List<Tuple> tuples = getTuples(stream);
    assertEquals(CARD, tuples.size());

    // results should be identical regardless of plist=true|false
    assertListOfTuplesEquals(plistTuples, tuples);

    solrClientCache.close();
  }

  // assert results are the same, with some sorting and rounding of floating point values
  private void assertListOfTuplesEquals(List<Tuple> exp, List<Tuple> act) {
    List<SortedMap<Object, Object>> expList = exp.stream().map(this::toComparableMap).collect(Collectors.toList());
    List<SortedMap<Object, Object>> actList = act.stream().map(this::toComparableMap).collect(Collectors.toList());
    assertEquals(expList, actList);
  }

  private SortedMap<Object, Object> toComparableMap(Tuple t) {
    SortedMap<Object, Object> cmap = new TreeMap<>();
    for (Map.Entry<Object, Object> e : t.getFields().entrySet()) {
      Object value = e.getValue();
      if (value instanceof Double) {
        cmap.put(e.getKey(), Precision.round((Double) value, 5));
      } else if (value instanceof Float) {
        cmap.put(e.getKey(), Precision.round((Float) value, 3));
      } else {
        cmap.put(e.getKey(), e.getValue());
      }
    }
    return cmap;
  }

  List<Tuple> getTuples(TupleStream tupleStream) throws IOException {
    List<Tuple> tuples = new ArrayList<>();
    try (tupleStream) {
      tupleStream.open();
      for (Tuple t = tupleStream.read(); !t.EOF; t = tupleStream.read()) {
        tuples.add(t);
      }
    }
    return tuples;
  }
}

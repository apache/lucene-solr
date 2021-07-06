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
import java.util.Optional;
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
import org.apache.solr.client.solrj.io.stream.metrics.CountDistinctMetric;
import org.apache.solr.client.solrj.io.stream.metrics.CountMetric;
import org.apache.solr.client.solrj.io.stream.metrics.MaxMetric;
import org.apache.solr.client.solrj.io.stream.metrics.MeanMetric;
import org.apache.solr.client.solrj.io.stream.metrics.Metric;
import org.apache.solr.client.solrj.io.stream.metrics.MinMetric;
import org.apache.solr.client.solrj.io.stream.metrics.SumMetric;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.handler.SolrDefaultStreamFactory;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.apache.solr.client.solrj.io.stream.FacetStream.TIERED_PARAM;

/**
 * Verify auto-plist with rollup over a facet expression when using collection alias over multiple collections.
 */
@SolrTestCaseJ4.SuppressSSL
@LuceneTestCase.SuppressCodecs({"Lucene3x", "Lucene40", "Lucene41", "Lucene42", "Lucene45"})
public class ParallelFacetStreamOverAliasTest extends SolrCloudTestCase {

  private static final String ALIAS_NAME = "SOME_ALIAS_WITH_MANY_COLLS";

  private static final String id = "id";
  private static final int NUM_COLLECTIONS = 2; // this test requires at least 2 collections, each with multiple shards
  private static final int NUM_DOCS_PER_COLLECTION = 40;
  private static final int NUM_SHARDS_PER_COLLECTION = 4;
  private static final int CARDINALITY = 10;
  private static final int BUCKET_SIZE_LIMIT = Math.max(CARDINALITY * 2, 100);

  private static final RandomGenerator rand = new JDKRandomGenerator(5150);
  private static List<String> listOfCollections;
  private static SolrClientCache solrClientCache;

  @BeforeClass
  public static void setupCluster() throws Exception {
    System.setProperty("solr.tests.numeric.dv", "true");

    configureCluster(NUM_COLLECTIONS).withMetrics(false)
        .addConfig("conf", getFile("solrj").toPath().resolve("solr").resolve("configsets").resolve("streaming").resolve("conf"))
        .configure();
    cleanup();
    setupCollectionsAndAlias();

    solrClientCache = new SolrClientCache();
  }

  /**
   * setup the testbed with necessary collections, documents, and alias
   */
  public static void setupCollectionsAndAlias() throws Exception {

    final NormalDistribution[] dists = new NormalDistribution[CARDINALITY];
    for (int i = 0; i < dists.length; i++) {
      dists[i] = new NormalDistribution(rand, i + 1, 1d);
    }

    List<String> collections = new ArrayList<>(NUM_COLLECTIONS);
    final List<Exception> errors = new LinkedList<>();
    Stream.iterate(1, n -> n + 1).limit(NUM_COLLECTIONS).forEach(colIdx -> {
      final String collectionName = "coll" + colIdx;
      collections.add(collectionName);
      try {
        CollectionAdminRequest.Create createCmd =
            CollectionAdminRequest.createCollection(collectionName, "conf", NUM_SHARDS_PER_COLLECTION, 1);
        createCmd.setMaxShardsPerNode(NUM_SHARDS_PER_COLLECTION);
        createCmd.process(cluster.getSolrClient());
        cluster.waitForActiveCollection(collectionName, NUM_SHARDS_PER_COLLECTION, NUM_SHARDS_PER_COLLECTION);

        // want a variable num of docs per collection so that avg of avg does not work ;-)
        final int numDocsInColl = colIdx % 2 == 0 ? NUM_DOCS_PER_COLLECTION / 2 : NUM_DOCS_PER_COLLECTION;
        final int limit = NUM_COLLECTIONS == 1 ? NUM_DOCS_PER_COLLECTION * 2 : numDocsInColl;
        UpdateRequest ur = new UpdateRequest();
        Stream.iterate(0, n -> n + 1).limit(limit)
            .forEach(docId -> ur.add(id, UUID.randomUUID().toString(),
                "a_s", "hello" + docId,
                "a_i", String.valueOf(docId % CARDINALITY),
                "b_i", rand.nextBoolean() ? "1" : "0",
                "a_d", String.valueOf(dists[docId % dists.length].sample())));
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
        final List<Exception> errors = new LinkedList<>();
        listOfCollections.stream().map(CollectionAdminRequest::deleteCollection).forEach(c -> {
          try {
            c.process(cluster.getSolrClient());
          } catch (SolrServerException | IOException e) {
            errors.add(e);
          }
        });
        if (!errors.isEmpty()) {
          throw errors.get(0);
        }
      }
    }
  }

  @AfterClass
  public static void after() throws Exception {
    cleanup();

    if (solrClientCache != null) {
      solrClientCache.close();
    }
  }

  /**
   * Test parallelized calls to facet expression, one for each collection in the alias
   */
  @Test
  public void testParallelFacetOverAlias() throws Exception {

    String facetExprTmpl = "" +
        "facet(\n" +
        "  %s,\n" +
        "  tiered=%s,\n" +
        "  q=\"*:*\", \n" +
        "  buckets=\"a_i\", \n" +
        "  bucketSorts=\"a_i asc\", \n" +
        "  bucketSizeLimit=" + BUCKET_SIZE_LIMIT + ", \n" +
        "  sum(a_d), avg(a_d), min(a_d), max(a_d), count(*)\n" +
        ")\n";

    compareTieredStreamWithNonTiered(facetExprTmpl, 1);
  }

  /**
   * Test parallelized calls to facet expression with multiple dimensions, one for each collection in the alias
   */
  @Test
  public void testParallelFacetMultipleDimensionsOverAlias() throws Exception {

    // notice we're sorting the stream by a metric, but internally, that doesn't work for parallelization
    // so the rollup has to sort by dimensions and then apply a final re-sort once the parallel streams are merged
    String facetExprTmpl = "" +
        "facet(\n" +
        "  %s,\n" +
        "  tiered=%s,\n" +
        "  q=\"*:*\", \n" +
        "  buckets=\"a_i,b_i\", \n" + /* two dimensions here ~ doubles the number of tuples */
        "  bucketSorts=\"sum(a_d) desc\", \n" +
        "  bucketSizeLimit=" + BUCKET_SIZE_LIMIT + ", \n" +
        "  sum(a_d), avg(a_d), min(a_d), max(a_d), count(*)\n" +
        ")\n";

    compareTieredStreamWithNonTiered(facetExprTmpl, 2);
  }

  @Test
  public void testParallelFacetSortByDimensions() throws Exception {
    // notice we're sorting the stream by a metric, but internally, that doesn't work for parallelization
    // so the rollup has to sort by dimensions and then apply a final re-sort once the parallel streams are merged
    String facetExprTmpl = "" +
        "facet(\n" +
        "  %s,\n" +
        "  tiered=%s,\n" +
        "  q=\"*:*\", \n" +
        "  buckets=\"a_i,b_i\", \n" +
        "  bucketSorts=\"a_i asc, b_i asc\", \n" +
        "  bucketSizeLimit=" + BUCKET_SIZE_LIMIT + ", \n" +
        "  sum(a_d), avg(a_d), min(a_d), max(a_d), count(*)\n" +
        ")\n";

    compareTieredStreamWithNonTiered(facetExprTmpl, 2);
  }

  @Test
  public void testParallelStats() throws Exception {
    Metric[] metrics = new Metric[]{
        new CountMetric(),
        new CountDistinctMetric("a_i"),
        new SumMetric("b_i"),
        new MinMetric("a_i"),
        new MaxMetric("a_i"),
        new MeanMetric("a_d")
    };

    String zkHost = cluster.getZkServer().getZkAddress();
    StreamContext streamContext = new StreamContext();
    streamContext.setSolrClientCache(solrClientCache);

    ModifiableSolrParams solrParams = new ModifiableSolrParams();
    solrParams.add(CommonParams.Q, "*:*");
    solrParams.add(TIERED_PARAM, "true");

    // tiered stats stream
    StatsStream statsStream = new StatsStream(zkHost, ALIAS_NAME, solrParams, metrics);
    statsStream.setStreamContext(streamContext);
    List<Tuple> tieredTuples = getTuples(statsStream);
    assertEquals(1, tieredTuples.size());
    assertNotNull(statsStream.parallelizedStream);

    solrParams = new ModifiableSolrParams();
    solrParams.add(CommonParams.Q, "*:*");
    solrParams.add(TIERED_PARAM, "false");
    statsStream = new StatsStream(zkHost, ALIAS_NAME, solrParams, metrics);
    statsStream.setStreamContext(streamContext);
    // tiered should match non-tiered results
    assertListOfTuplesEquals(tieredTuples, getTuples(statsStream));
    assertNull(statsStream.parallelizedStream);
  }

  // execute the provided expression with tiered=true and compare to results of tiered=false
  private void compareTieredStreamWithNonTiered(String facetExprTmpl, int dims) throws IOException {
    String facetExpr = String.format(Locale.US, facetExprTmpl, ALIAS_NAME, "true");

    StreamContext streamContext = new StreamContext();
    streamContext.setSolrClientCache(solrClientCache);
    StreamFactory factory = new SolrDefaultStreamFactory().withDefaultZkHost(cluster.getZkServer().getZkAddress());

    TupleStream stream = factory.constructStream(facetExpr);
    stream.setStreamContext(streamContext);

    // check the parallel setup logic
    assertParallelFacetStreamConfig(stream, dims);

    List<Tuple> plistTuples = getTuples(stream);
    assertEquals(CARDINALITY * dims, plistTuples.size());

    // now re-execute the same expression w/o plist
    facetExpr = String.format(Locale.US, facetExprTmpl, ALIAS_NAME, "false");
    stream = factory.constructStream(facetExpr);
    stream.setStreamContext(streamContext);
    List<Tuple> tuples = getTuples(stream);
    assertEquals(CARDINALITY * dims, tuples.size());

    // results should be identical regardless of tiered=true|false
    assertListOfTuplesEquals(plistTuples, tuples);
  }

  private void assertParallelFacetStreamConfig(TupleStream stream, int dims) throws IOException {
    assertTrue(stream instanceof FacetStream);
    FacetStream facetStream = (FacetStream) stream;
    TupleStream[] parallelStreams = facetStream.parallelize(listOfCollections);
    assertEquals(NUM_COLLECTIONS, parallelStreams.length);
    assertTrue(parallelStreams[0] instanceof FacetStream);

    Optional<Metric[]> rollupMetrics = facetStream.getRollupMetrics(facetStream.getMetrics().toArray(new Metric[0]));
    assertTrue(rollupMetrics.isPresent());
    assertEquals(5, rollupMetrics.get().length);
    Map<String, String> selectFields = facetStream.getRollupSelectFields(rollupMetrics.get());
    assertNotNull(selectFields);
    assertEquals(5 + dims /* num metrics + num dims */, selectFields.size());
    assertEquals("a_i", selectFields.get("a_i"));
    assertEquals("max(a_d)", selectFields.get("max(max(a_d))"));
    assertEquals("min(a_d)", selectFields.get("min(min(a_d))"));
    assertEquals("sum(a_d)", selectFields.get("sum(sum(a_d))"));
    assertEquals("avg(a_d)", selectFields.get("wsum(avg(a_d), count(*), false)"));
    assertEquals("count(*)", selectFields.get("sum(count(*))"));
    if (dims > 1) {
      assertEquals("b_i", selectFields.get("b_i"));
    }
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
    try {
      tupleStream.open();
      for (Tuple t = tupleStream.read(); !t.EOF; t = tupleStream.read()) {
        tuples.add(t);
      }
    } finally {
      tupleStream.close();
    }
    return tuples;
  }
}

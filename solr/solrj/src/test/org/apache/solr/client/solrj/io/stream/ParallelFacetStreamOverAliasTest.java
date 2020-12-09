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
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
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
import org.apache.solr.client.solrj.io.stream.metrics.CountMetric;
import org.apache.solr.client.solrj.io.stream.metrics.MaxMetric;
import org.apache.solr.client.solrj.io.stream.metrics.MeanMetric;
import org.apache.solr.client.solrj.io.stream.metrics.MinMetric;
import org.apache.solr.client.solrj.io.stream.metrics.SumMetric;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.util.LogLevel;
import org.apache.solr.util.RTimer;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * objective of this test suite is to test scalability of Streaming expressions for large deployments,
 * for example where there are many collections with high sharding and each collection has millions of documents
 */
@SolrTestCaseJ4.SuppressSSL
@LuceneTestCase.SuppressCodecs({"Lucene3x", "Lucene40", "Lucene41", "Lucene42", "Lucene45"})
@LogLevel("org.apache.solr.client.solrj.io.stream=INFO;org.apache.solr.common.cloud.ZkStateReader=WARN;org.apache.solr.metrics=WARN;org.apache.solr.core.SolrCore=WARN;org.apache.solr.cloud=WARN;org.apache.solr.update=WARN;org.apache.solr.rest=ERROR;org.apache.solr.servlet.HttpSolrCall=WARN;org.apache.solr=WARN;")
public class ParallelFacetStreamOverAliasTest extends SolrCloudTestCase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final String ALIAS_NAME = "SOME_ALIAS_WITH_MANY_COLLS";

  private static final String id = "id";
  private static final int NUM_COLLECTIONS = 2;
  private static final int NUM_DOCS_PER_COLLECTION = 40;
  private static final int NUM_SHARDS_PER_COLLECTION = 4;
  private static final int CARD = 10;

  private static List<String> listOfCollections;
  private static final RandomGenerator rand = new JDKRandomGenerator(5150);

  @BeforeClass
  public static void setupCluster() throws Exception {
    final RTimer timer = new RTimer();
    configureCluster(2).withMetrics(false)
        .addConfig("conf", getFile("solrj").toPath().resolve("solr").resolve("configsets").resolve("streaming").resolve("conf"))
        .configure();
    cleanup();
    setupCollectionsAndAlias();

    if (log.isInfoEnabled())
      log.info("Took {}ms to setup cluster with {} collections", timer.getTime(), NUM_COLLECTIONS);
  }

  /**
   * setup the testbed with necessary collections, documents, and alias
   */
  public static void setupCollectionsAndAlias() throws Exception {

    final NormalDistribution[] dists = new NormalDistribution[CARD];
    for (int i=0; i < dists.length; i++) {
      dists[i] = new NormalDistribution(rand, i+1, 1d);
    }

    List<String> collections = new ArrayList<>(NUM_COLLECTIONS);
    Stream.iterate(1, n -> n + 1).limit(NUM_COLLECTIONS)
        .forEach(colIdx -> {
          final String collectionName = "coll"+colIdx;
          collections.add(collectionName);
          try {
            CollectionAdminRequest.createCollection(collectionName, "conf", NUM_SHARDS_PER_COLLECTION, 1).process(cluster.getSolrClient());
            cluster.waitForActiveCollection(collectionName, NUM_SHARDS_PER_COLLECTION, NUM_SHARDS_PER_COLLECTION);

            // want a variable num of docs per collection so that avg of avg does not work ;-)
            final int numDocsInColl = colIdx % 2 == 0 ? NUM_DOCS_PER_COLLECTION/2 : NUM_DOCS_PER_COLLECTION;
            final int limit = NUM_COLLECTIONS == 1 ? NUM_DOCS_PER_COLLECTION * 2 : numDocsInColl;
            UpdateRequest ur = new UpdateRequest();
            Stream.iterate(0, n -> n + 1).limit(limit)
                .forEach(docId -> ur.add(id, UUID.randomUUID().toString(),
                    "a_s", "hello" + docId, "a_i", String.valueOf(docId % CARD), "a_d", String.valueOf(dists[docId % dists.length].sample())));
            ur.commit(cluster.getSolrClient(), collectionName);
          } catch (SolrServerException | IOException e) {
            log.error("problem creating and loading data into collection", e);
          }
        });
    listOfCollections = collections;
    String aliasedCollectionString = String.join(",", collections);
    CollectionAdminRequest.createAlias(ALIAS_NAME, aliasedCollectionString).process(cluster.getSolrClient());
  }

  /**
   * Test parallelized calls to facet expression, one for each collection in the alias
   */
  @Test
  public void testParallelFacetOverAlias() throws Exception {
    StreamContext streamContext = new StreamContext();
    SolrClientCache solrClientCache = new SolrClientCache();
    streamContext.setSolrClientCache(solrClientCache);

    String dim = "a_i";
    String col = "a_d";
    String facetExprTmpl = "facet(%s, plist=%s, q=\"*:*\", fl=\"%s\", sort=\"%s asc\", buckets=\"%s\", bucketSorts=\"count(*) asc\", bucketSizeLimit=10000, %s)";
    String metrics = String.format(Locale.US, "sum(%s), avg(%s), min(%s), max(%s), count(*)", col, col, col, col);
    String facetExpr = String.format(Locale.US, facetExprTmpl, ALIAS_NAME, "true", dim, dim, dim, metrics);

    String zkhost = cluster.getZkServer().getZkAddress();
    StreamFactory factory = new StreamFactory()
        .withCollectionZkHost(ALIAS_NAME, zkhost)
        .withFunctionName("facet", FacetStream.class)
        .withFunctionName("sum", SumMetric.class)
        .withFunctionName("avg", MeanMetric.class)
        .withFunctionName("min", MinMetric.class)
        .withFunctionName("max", MaxMetric.class)
        .withFunctionName("count", CountMetric.class);
    for (String coll : listOfCollections) {
      factory = factory.withCollectionZkHost(coll, zkhost);
    }

    final RTimer timer = new RTimer();
    TupleStream stream = factory.constructStream(facetExpr);
    stream.setStreamContext(streamContext);
    List<Tuple> plistTuples = getTuples(stream);

    if (log.isInfoEnabled()) {
      log.info("stream {} tuples, took: {}ms, fields: {}", plistTuples.size(), timer.getTime(), plistTuples.get(0).getFields().keySet());
    }

    assertEquals(CARD, plistTuples.size());

    // now re-execute the same expression w/o plist
    facetExpr = String.format(Locale.US, facetExprTmpl, ALIAS_NAME, "false", dim, dim, dim, metrics);
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
    List<SortedMap<Object,Object>> expList = exp.stream().map(this::toComparableMap).collect(Collectors.toList());
    List<SortedMap<Object,Object>> actList = act.stream().map(this::toComparableMap).collect(Collectors.toList());
    assertEquals(expList, actList);
  }

  private SortedMap<Object,Object> toComparableMap(Tuple t) {
    SortedMap<Object,Object> cmap = new TreeMap<>();
    for (Map.Entry<Object,Object> e : t.getFields().entrySet()) {
      Object value = e.getValue();
      if (value instanceof Double) {
        cmap.put(e.getKey(), Precision.round((Double)value, 5));
      } else if (value instanceof Float) {
        cmap.put(e.getKey(), Precision.round((Float)value, 3));
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

  public static void cleanup() throws Exception {
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

  @AfterClass
  public static void after() throws Exception {
    cleanup();
  }
}

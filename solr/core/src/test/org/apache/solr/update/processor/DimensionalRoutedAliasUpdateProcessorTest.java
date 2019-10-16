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

package org.apache.solr.update.processor;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.time.Instant;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.lucene.util.IOUtils;
import org.apache.solr.client.solrj.RoutedAliasTypes;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.CollectionAdminRequest.CreateCategoryRoutedAlias;
import org.apache.solr.client.solrj.request.CollectionAdminRequest.CreateTimeRoutedAlias;
import org.apache.solr.client.solrj.response.FieldStatsInfo;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.cloud.api.collections.CategoryRoutedAlias;
import org.apache.solr.cloud.api.collections.TimeRoutedAlias;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.SolrParams;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.client.solrj.request.CollectionAdminRequest.createCategoryRoutedAlias;
import static org.apache.solr.client.solrj.request.CollectionAdminRequest.createTimeRoutedAlias;

public class DimensionalRoutedAliasUpdateProcessorTest extends RoutedAliasUpdateProcessorTest {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final String CRA = RoutedAliasTypes.CATEGORY.getSeparatorPrefix();
  private static final String TRA = RoutedAliasTypes.TIME.getSeparatorPrefix();

  private static CloudSolrClient solrClient;
  private int lastDocId = 0;
  private int numDocsDeletedOrFailed = 0;

  private static final String timeField = "timestamp_dt";
  private static final String catField = "cat_s";


  @Before
  public void doBefore() throws Exception {
    configureCluster(4).configure();
    solrClient = getCloudSolrClient(cluster);
    //log this to help debug potential causes of problems
    log.info("SolrClient: {}", solrClient);
    log.info("ClusterStateProvider {}", solrClient.getClusterStateProvider());
  }

  @After
  public void doAfter() throws Exception {
    solrClient.close();
    shutdownCluster();
  }

  @AfterClass
  public static void finish() throws Exception {
    IOUtils.close(solrClient);
  }
  @Test
  public void testTimeCat() throws Exception {
    String configName = getSaferTestName();
    createConfigSet(configName);

    CreateTimeRoutedAlias TRA_Dim = createTimeRoutedAlias(getAlias(), "2019-07-01T00:00:00Z", "+1DAY",
        getTimeField(), null);
    CreateCategoryRoutedAlias CRA_Dim = createCategoryRoutedAlias(null, getCatField(), 20, null);

    CollectionAdminRequest.DimensionalRoutedAlias dra = CollectionAdminRequest.createDimensionalRoutedAlias(getAlias(),
        CollectionAdminRequest.createCollection("_unused_", configName, 2, 2)
            .setMaxShardsPerNode(2), TRA_Dim,  CRA_Dim);

    SolrParams params = dra.getParams();
    assertEquals("Dimensional[TIME,CATEGORY]", params.get(CollectionAdminRequest.RoutedAliasAdminRequest.ROUTER_TYPE_NAME));
    System.out.println(params);
    assertEquals("20", params.get("router.1.maxCardinality"));
    assertEquals("2019-07-01T00:00:00Z", params.get("router.0.start"));

    dra.process(solrClient);

    String firstCol = timeCatDraColFor("2019-07-01", CategoryRoutedAlias.UNINITIALIZED);
    cluster.waitForActiveCollection(firstCol, 2, 4);

    // cat field... har har.. get it? ... category/cat... ...oh never mind.
    addDocsAndCommit(true, newDoc("tabby", "2019-07-02T00:00:00Z"));

    assertCatTimeInvariants(
        ap(
            firstCol,
            // lower dimensions are fleshed out because we need to maintain the order of the TRA dim and
            // not fail if we get an older document later
            timeCatDraColFor("2019-07-01", "tabby"),
            timeCatDraColFor("2019-07-02", "tabby")
        ),
        ap(
            "tabby"
        )
    );

    addDocsAndCommit(true, newDoc("calico", "2019-07-02T00:00:00Z"));

    // initial col not removed because the 07-01 CRA has not yet gained a new category (sub-dimensions are independent)
    assertCatTimeInvariants(
        ap(
            timeCatDraColFor("2019-07-01", "calico"),
            timeCatDraColFor("2019-07-02", "calico"),
            timeCatDraColFor("2019-07-01", "tabby"),
            timeCatDraColFor("2019-07-02", "tabby")
        ),
        ap(
            "tabby",
            "calico"
        )
    );

    testFailedDocument("shorthair",     "2017-10-23T00:00:00Z", "couldn't be routed" );
    testFailedDocument("shorthair",     "2020-10-23T00:00:00Z", "too far in the future" );
    testFailedDocument(null,            "2019-07-02T00:00:00Z", "Route value is null");
    testFailedDocument("foo__CRA__bar", "2019-07-02T00:00:00Z", "7 character sequence __CRA__");
    testFailedDocument("fóóCRAóóbar",   "2019-07-02T00:00:00Z", "7 character sequence __CRA__");

    // hopefully nothing changed
    assertCatTimeInvariants(
        ap(
            timeCatDraColFor("2019-07-01", "calico"),
            timeCatDraColFor("2019-07-02", "calico"),
            timeCatDraColFor("2019-07-01", "tabby"),
            timeCatDraColFor("2019-07-02", "tabby")
        ),
        ap(
            "tabby",
            "calico"
        )
    );

    // 4 docs no new collections
    addDocsAndCommit(true,
        newDoc("calico", "2019-07-02T00:00:00Z"),
        newDoc("tabby", "2019-07-01T00:00:00Z"),
        newDoc("tabby", "2019-07-01T23:00:00Z"),
        newDoc("calico", "2019-07-02T23:00:00Z")
    );

    // hopefully nothing changed
    assertCatTimeInvariants(
        ap(
            timeCatDraColFor("2019-07-01", "calico"),
            timeCatDraColFor("2019-07-02", "calico"),
            timeCatDraColFor("2019-07-01", "tabby"),
            timeCatDraColFor("2019-07-02", "tabby")
        ),
        ap(
            "tabby",
            "calico"
        )
    );

    // 4 docs 2 new collections, in random order and maybe not using the alias
    addDocsAndCommit(false,
        newDoc("calico", "2019-07-04T00:00:00Z"),
        newDoc("tabby", "2019-07-01T00:00:00Z"),
        newDoc("tabby", "2019-07-01T23:00:00Z"),
        newDoc("calico", "2019-07-03T23:00:00Z")
    );

    assertCatTimeInvariants(
        ap(
            timeCatDraColFor("2019-07-01", "calico"),
            timeCatDraColFor("2019-07-02", "calico"),
            timeCatDraColFor("2019-07-03", "calico"),
            timeCatDraColFor("2019-07-04", "calico"),
            timeCatDraColFor("2019-07-01", "tabby"),
            timeCatDraColFor("2019-07-02", "tabby")
        ),
        ap(
            "tabby",
            "calico"
        )
    );

    // now test with async pre-create.
    CollectionAdminRequest.setAliasProperty(getAlias())
        .addProperty("router.0.preemptiveCreateMath", "30MINUTE").process(solrClient);

    addDocsAndCommit(true,
        newDoc("shorthair", "2019-07-02T23:40:00Z"), // create 2 sync 1 async
        newDoc("calico", "2019-07-03T23:00:00Z")     // does not create
    );

    waitColAndAlias(getAlias(), "",   TRA + "2019-07-03" + CRA + "shorthair", 2);

    assertCatTimeInvariants(
        ap(
            timeCatDraColFor("2019-07-01", "calico"),
            timeCatDraColFor("2019-07-02", "calico"),
            timeCatDraColFor("2019-07-03", "calico"),
            timeCatDraColFor("2019-07-04", "calico"),
            timeCatDraColFor("2019-07-01", "shorthair"),
            timeCatDraColFor("2019-07-02", "shorthair"),
            timeCatDraColFor("2019-07-03", "shorthair"),
            timeCatDraColFor("2019-07-01", "tabby"),
            timeCatDraColFor("2019-07-02", "tabby")
        ),
        ap(
            "shorthair",
            "tabby",
            "calico"
        )
    );

    addDocsAndCommit(false,
        newDoc("shorthair", "2019-07-02T23:40:00Z"), // should be no change
        newDoc("calico", "2019-07-03T23:00:00Z")
    );

    /*
     Here we need to be testing that something that should not be created (extra preemptive async collections)
     didn't get created (a bug that actually got killed during development, and caused later asserts to
     fail due to wrong number of collections). There's no way to set a watch for something that doesn't and
     should never exist... Thus, the only choice is to sleep and make sure nothing appeared while we were asleep.
    */
    Thread.sleep(5000);

    assertCatTimeInvariants(
        ap(
            timeCatDraColFor("2019-07-01", "calico"),
            timeCatDraColFor("2019-07-02", "calico"),
            timeCatDraColFor("2019-07-03", "calico"),
            timeCatDraColFor("2019-07-04", "calico"),
            timeCatDraColFor("2019-07-01", "shorthair"),
            timeCatDraColFor("2019-07-02", "shorthair"),
            timeCatDraColFor("2019-07-03", "shorthair"),
            timeCatDraColFor("2019-07-01", "tabby"),
            timeCatDraColFor("2019-07-02", "tabby")
        ),
        ap(
            "shorthair",
            "tabby",
            "calico"
        )
    );

    // now test with auto-delete.
    CollectionAdminRequest.setAliasProperty(getAlias())
        .addProperty("router.0.autoDeleteAge", "/DAY-5DAY").process(solrClient);

    // this one should not yet cause deletion
    addDocsAndCommit(false,
        newDoc("shorthair", "2019-07-02T23:00:00Z"), // no effect expected
        newDoc("calico", "2019-07-05T23:00:00Z")     // create 1
    );

    assertCatTimeInvariants(
        ap(
            timeCatDraColFor("2019-07-01", "calico"),
            timeCatDraColFor("2019-07-02", "calico"),
            timeCatDraColFor("2019-07-03", "calico"),
            timeCatDraColFor("2019-07-04", "calico"),
            timeCatDraColFor("2019-07-05", "calico"),
            timeCatDraColFor("2019-07-01", "shorthair"),
            timeCatDraColFor("2019-07-02", "shorthair"),
            timeCatDraColFor("2019-07-03", "shorthair"),
            timeCatDraColFor("2019-07-01", "tabby"),
            timeCatDraColFor("2019-07-02", "tabby")
        ),
        ap(
            "shorthair",
            "tabby",
            "calico"
        )
    );

    // have to only send to alias here since one of the collections will be deleted.
    addDocsAndCommit(true,
        newDoc("shorthair", "2019-07-02T23:00:00Z"), // no effect expected
        newDoc("calico", "2019-07-06T00:00:00Z")     // create July 6, delete July 1
    );
    waitCoreCount(getAlias() +  TRA + "2019-07-01" + CRA + "calico", 0);

    assertCatTimeInvariants(
        ap(
            timeCatDraColFor("2019-07-02", "calico"),
            timeCatDraColFor("2019-07-03", "calico"),
            timeCatDraColFor("2019-07-04", "calico"),
            timeCatDraColFor("2019-07-05", "calico"),
            timeCatDraColFor("2019-07-06", "calico"),
            // note that other categories are unaffected
            timeCatDraColFor("2019-07-01", "shorthair"),
            timeCatDraColFor("2019-07-02", "shorthair"),
            timeCatDraColFor("2019-07-03", "shorthair"),
            timeCatDraColFor("2019-07-01", "tabby"),
            timeCatDraColFor("2019-07-02", "tabby")
        ),
        ap(
            "shorthair",
            "tabby",
            "calico"
        )
    );

    // verify that all the documents ended up in the right collections.
    QueryResponse resp = solrClient.query(getAlias(), params(
        "q", "*:*",
        "rows", "100",
        "fl","*,[shard]",
        "sort", "id asc"
    ));
    SolrDocumentList results = resp.getResults();
    assertEquals(18, results.getNumFound());
    for (SolrDocument result : results) {
      String shard = String.valueOf(result.getFieldValue("[shard]"));
      String cat = String.valueOf(result.getFieldValue("cat_s"));
      Date date = (Date) result.getFieldValue("timestamp_dt");
      String day = date.toInstant().toString().split("T")[0];
      assertTrue(shard.contains(cat));
      assertTrue(shard.contains(day));
    }
  }


  @Test
  public void testCatTime() throws Exception {
    String configName = getSaferTestName();
    createConfigSet(configName);

    CreateTimeRoutedAlias TRA_Dim = createTimeRoutedAlias(getAlias(), "2019-07-01T00:00:00Z", "+1DAY",
        getTimeField(), null);
    CreateCategoryRoutedAlias CRA_Dim = createCategoryRoutedAlias(null, getCatField(), 20, null);

    CollectionAdminRequest.DimensionalRoutedAlias dra = CollectionAdminRequest.createDimensionalRoutedAlias(getAlias(),
        CollectionAdminRequest.createCollection("_unused_", configName, 2, 2)
            .setMaxShardsPerNode(2), CRA_Dim, TRA_Dim);

    SolrParams params = dra.getParams();
    assertEquals("Dimensional[CATEGORY,TIME]", params.get(CollectionAdminRequest.RoutedAliasAdminRequest.ROUTER_TYPE_NAME));
    System.out.println(params);
    assertEquals("20", params.get("router.0.maxCardinality"));
    assertEquals("2019-07-01T00:00:00Z", params.get("router.1.start"));

    dra.process(solrClient);

    String firstCol = catTimeDraColFor(CategoryRoutedAlias.UNINITIALIZED, "2019-07-01");
    cluster.waitForActiveCollection(firstCol, 2, 4);

    // cat field... har har.. get it? ... category/cat... ...oh never mind.
    addDocsAndCommit(true, newDoc("tabby", "2019-07-02T00:00:00Z"));

    assertCatTimeInvariants(
        ap(
            firstCol,
            catTimeDraColFor("tabby", "2019-07-01"),
            catTimeDraColFor("tabby", "2019-07-02")
        ),
        ap(
            "tabby"
        )
    );

    addDocsAndCommit(true, newDoc("calico", "2019-07-02T00:00:00Z"));

    // initial col should be removed
    assertCatTimeInvariants(
        ap(
            catTimeDraColFor("calico", "2019-07-01"),
            catTimeDraColFor("calico", "2019-07-02"),
            catTimeDraColFor("tabby", "2019-07-01"),
            catTimeDraColFor("tabby", "2019-07-02")
        ),
        ap(
            "tabby",
            "calico"
        )
    );

    testFailedDocument("shorthair",     "2017-10-23T00:00:00Z", "couldn't be routed" );
    testFailedDocument("shorthair",     "2020-10-23T00:00:00Z", "too far in the future" );
    testFailedDocument(null,            "2019-07-02T00:00:00Z", "Route value is null");
    testFailedDocument("foo__CRA__bar", "2019-07-02T00:00:00Z", "7 character sequence __CRA__");
    testFailedDocument("fóóCRAóóbar",   "2019-07-02T00:00:00Z", "7 character sequence __CRA__");

    // hopefully nothing changed
    assertCatTimeInvariants(
        ap(
            catTimeDraColFor("calico", "2019-07-01"),
            catTimeDraColFor("calico", "2019-07-02"),
            catTimeDraColFor("tabby", "2019-07-01"),
            catTimeDraColFor("tabby", "2019-07-02")
        ),
        ap(
            "tabby",
            "calico"
        )
    );

    // 4 docs no new collections
    addDocsAndCommit(true,
        newDoc("calico", "2019-07-02T00:00:00Z"),
        newDoc("tabby", "2019-07-01T00:00:00Z"),
        newDoc("tabby", "2019-07-01T23:00:00Z"),
        newDoc("calico", "2019-07-02T23:00:00Z")
    );

    // hopefully nothing changed
    assertCatTimeInvariants(
        ap(
            catTimeDraColFor("calico", "2019-07-01"),
            catTimeDraColFor("calico", "2019-07-02"),
            catTimeDraColFor("tabby", "2019-07-01"),
            catTimeDraColFor("tabby", "2019-07-02")
        ),
        ap(
            "tabby",
            "calico"
        )
    );

    // 4 docs 2 new collections, in random order and maybe not using the alias
    addDocsAndCommit(false,
        newDoc("calico", "2019-07-04T00:00:00Z"),
        newDoc("tabby", "2019-07-01T00:00:00Z"),
        newDoc("tabby", "2019-07-01T23:00:00Z"),
        newDoc("calico", "2019-07-03T23:00:00Z")
    );

    assertCatTimeInvariants(
        ap(
            catTimeDraColFor("calico", "2019-07-01"),
            catTimeDraColFor("calico", "2019-07-02"),
            catTimeDraColFor("calico", "2019-07-03"),
            catTimeDraColFor("calico", "2019-07-04"),
            catTimeDraColFor("tabby", "2019-07-01"),
            catTimeDraColFor("tabby", "2019-07-02")
            // tabby collections not filled in. No guarantee that time periods remain in sync
            // across categories.
        ),
        ap(
            "tabby",
            "calico"
        )
    );

    // now test with async pre-create.
    CollectionAdminRequest.setAliasProperty(getAlias())
        .addProperty("router.1.preemptiveCreateMath", "30MINUTE").process(solrClient);

    addDocsAndCommit(false,
        newDoc("shorthair", "2019-07-02T23:40:00Z"), // create 2 sync 1 async
        newDoc("calico", "2019-07-03T23:00:00Z")     // does not create
    );

    waitColAndAlias(getAlias(), "", CRA + "shorthair" + TRA + "2019-07-03", 2);

    assertCatTimeInvariants(
        ap(
            catTimeDraColFor("calico", "2019-07-01"),
            catTimeDraColFor("calico", "2019-07-02"),
            catTimeDraColFor("calico", "2019-07-03"),
            catTimeDraColFor("calico", "2019-07-04"),
            catTimeDraColFor("shorthair", "2019-07-01"),
            catTimeDraColFor("shorthair", "2019-07-02"),
            catTimeDraColFor("shorthair", "2019-07-03"),
            catTimeDraColFor("tabby", "2019-07-01"),
            catTimeDraColFor("tabby", "2019-07-02")
        ),
        ap(
            "shorthair",
            "tabby",
            "calico"
        )
    );

    addDocsAndCommit(false,
        newDoc("shorthair", "2019-07-02T23:40:00Z"), // should be no change
        newDoc("calico", "2019-07-03T23:00:00Z")
    );

    /*
     Here we need to be testing that something that should not be created (extra preemptive async collections)
     didn't get created (a bug that actually got killed during development, and caused later asserts to
     fail due to wrong number of collections). There's no way to set a watch for something that doesn't and
     should never exist... Thus, the only choice is to sleep and make sure nothing appeared while we were asleep.
    */
    Thread.sleep(5000);

    assertCatTimeInvariants(
        ap(
            catTimeDraColFor("calico", "2019-07-01"),
            catTimeDraColFor("calico", "2019-07-02"),
            catTimeDraColFor("calico", "2019-07-03"),
            catTimeDraColFor("calico", "2019-07-04"),
            catTimeDraColFor("shorthair", "2019-07-01"),
            catTimeDraColFor("shorthair", "2019-07-02"),
            catTimeDraColFor("shorthair", "2019-07-03"),
            catTimeDraColFor("tabby", "2019-07-01"),
            catTimeDraColFor("tabby", "2019-07-02")
        ),
        ap(
            "shorthair",
            "tabby",
            "calico"
        )
    );

    // now test with auto-delete.
    CollectionAdminRequest.setAliasProperty(getAlias())
        .addProperty("router.1.autoDeleteAge", "/DAY-5DAY").process(solrClient);

    // this one should not yet cause deletion
    addDocsAndCommit(false,
        newDoc("shorthair", "2019-07-02T23:00:00Z"), // no effect expected
        newDoc("calico", "2019-07-05T23:00:00Z")     // create 1
    );

    assertCatTimeInvariants(
        ap(
            catTimeDraColFor("calico", "2019-07-01"),
            catTimeDraColFor("calico", "2019-07-02"),
            catTimeDraColFor("calico", "2019-07-03"),
            catTimeDraColFor("calico", "2019-07-04"),
            catTimeDraColFor("calico", "2019-07-05"),
            catTimeDraColFor("shorthair", "2019-07-01"),
            catTimeDraColFor("shorthair", "2019-07-02"),
            catTimeDraColFor("shorthair", "2019-07-03"),
            catTimeDraColFor("tabby", "2019-07-01"),
            catTimeDraColFor("tabby", "2019-07-02")
        ),
        ap(
            "shorthair",
            "tabby",
            "calico"
        )
    );

    addDocsAndCommit(true,
        newDoc("shorthair", "2019-07-02T23:00:00Z"), // no effect expected
        newDoc("calico", "2019-07-06T00:00:00Z")     // create July 6, delete July 1
    );
    waitCoreCount(getAlias() + CRA + "calico" + TRA + "2019-07-01", 0);

    assertCatTimeInvariants(
        ap(
            catTimeDraColFor("calico", "2019-07-02"),
            catTimeDraColFor("calico", "2019-07-03"),
            catTimeDraColFor("calico", "2019-07-04"),
            catTimeDraColFor("calico", "2019-07-05"),
            catTimeDraColFor("calico", "2019-07-06"),
            // note that other categories are unaffected
            catTimeDraColFor("shorthair", "2019-07-01"),
            catTimeDraColFor("shorthair", "2019-07-02"),
            catTimeDraColFor("shorthair", "2019-07-03"),
            catTimeDraColFor("tabby", "2019-07-01"),
            catTimeDraColFor("tabby", "2019-07-02")
        ),
        ap(
            "shorthair",
            "tabby",
            "calico"
        )
    );

    // verify that all the documents ended up in the right collections.
    QueryResponse resp = solrClient.query(getAlias(), params(
        "q", "*:*",
        "rows", "100",
        "fl","*,[shard]",
        "sort", "id asc"
    ));
    SolrDocumentList results = resp.getResults();
    assertEquals(18, results.getNumFound());
    for (SolrDocument result : results) {
      String shard = String.valueOf(result.getFieldValue("[shard]"));
      String cat = String.valueOf(result.getFieldValue("cat_s"));
      Date date = (Date) result.getFieldValue("timestamp_dt");
      String day = date.toInstant().toString().split("T")[0];
      assertTrue(shard.contains(cat));
      assertTrue(shard.contains(day));
    }

  }

  public String catTimeDraColFor(String category, String timestamp) {
    return getAlias() + CRA + category + TRA + timestamp;
  }

  public String timeCatDraColFor(String timestamp, String category) {
    return getAlias()  + TRA + timestamp + CRA + category;
  }

  /**
   * Test for invariant conditions when dealing with a DRA that is category X time.
   *
   * @param expectedCols the collections we expect to see
   * @param categories   the categories added thus far
   */
  private void assertCatTimeInvariants(String[] expectedCols, String[] categories) throws Exception {
    final int expectNumFound = lastDocId - numDocsDeletedOrFailed; //lastDocId is effectively # generated docs
    int totalNumFound = 0;

    final List<String> cols = new CollectionAdminRequest.ListAliases().process(solrClient).getAliasesAsLists().get(getSaferTestName());
    assert !cols.isEmpty();

    for (String category : categories) {
      List<String> cats = cols.stream().filter(c -> c.contains(category)).collect(Collectors.toList());
      Object[] expectedColOrder = cats.stream().sorted(Collections.reverseOrder()).toArray();
      Object[] actuals = cats.toArray();
      assertArrayEquals("expected reverse sorted",
          expectedColOrder,
          actuals);

      Instant colEndInstant = null; // exclusive end

      for (String col : cats) { // ASSUMPTION: reverse sorted order
        Instant colStartInstant;
        try {
          colStartInstant = TimeRoutedAlias.parseInstantFromCollectionName(getAlias(), col);
        } catch (Exception e) {
          String colTmp = col;
          // special case for tests... all of which have no more than one TRA dimension
          // This won't work if we decide to write a test with 2 time dimensions.
          // (but that's an odd case so we'll wait)
          int traIndex = colTmp.indexOf(TRA)+ TRA.length();
          while (colTmp.lastIndexOf("__") > traIndex) {
            colTmp = colTmp.substring(0,colTmp.lastIndexOf("__"));
          }
          colStartInstant = TimeRoutedAlias.parseInstantFromCollectionName(getAlias(), colTmp);
        }
        final QueryResponse colStatsResp = solrClient.query(col, params(
            "q", "*:*",
            "fq", catField + ":" + category,
            "rows", "0",
            "stats", "true",
            "stats.field", getTimeField()));
        long numFound = colStatsResp.getResults().getNumFound();
        if (numFound > 0) {
          totalNumFound += numFound;
          final FieldStatsInfo timestampStats = colStatsResp.getFieldStatsInfo().get(getTimeField());
          assertTrue(colStartInstant.toEpochMilli() <= ((Date) timestampStats.getMin()).getTime());
          if (colEndInstant != null) {
            assertTrue(colEndInstant.toEpochMilli() > ((Date) timestampStats.getMax()).getTime());
          }
        }

        colEndInstant = colStartInstant; // next older segment will max out at our current start time
      }

    }

    assertEquals(expectNumFound, totalNumFound);

    assertEquals("COLS FOUND:" + cols, expectedCols.length, cols.size());
  }

  private void testFailedDocument(String category, String timestamp, String errorMsg) throws SolrServerException, IOException {
    try {
      final UpdateResponse resp = solrClient.add(getAlias(), newDoc(category, timestamp));
      // if we have a TolerantUpdateProcessor then we see it there)
      final Object errors = resp.getResponseHeader().get("errors"); // Tolerant URP
      assertTrue(errors != null && errors.toString().contains(errorMsg));
    } catch (SolrException e) {
      String message = e.getMessage();
      assertTrue("expected message to contain" + errorMsg + " but message was " + message , message.contains(errorMsg));
    }
    numDocsDeletedOrFailed++;
  }

  // convenience for constructing arrays.
  private String[] ap(String... p) {
    return p;
  }


  private SolrInputDocument newDoc(String category, String timestamp) {
    Instant instant = Instant.parse(timestamp);
    return sdoc("id", Integer.toString(++lastDocId),
        getTimeField(), instant.toString(),
        getCatField(), category,
        getIntField(), "0"); // always 0
  }

  private String getTimeField() {
    return timeField;
  }

  private String getCatField() {
    return catField;
  }

  @Override
  public String getAlias() {
    return getSaferTestName();
  }

  @Override
  public CloudSolrClient getSolrClient() {
    return solrClient;
  }

}

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

package org.apache.solr.cloud;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.Locale;
import java.util.Map;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.lucene.util.IOUtils;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.common.cloud.Aliases;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.update.processor.TimeRoutedAliasUpdateProcessor;
import org.apache.solr.util.DateMathParser;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Direct http tests of the CreateRoutedAlias functionality.
 */
public class CreateRoutedAliasTest extends SolrCloudTestCase {

  private  CloudSolrClient solrClient;
  private  CloseableHttpClient httpclient;

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(2).configure();
  }

  @After
  public  void finish() throws Exception {
    IOUtils.close(solrClient, httpclient);
  }

  @Before
  public void doBefore() throws Exception {
    solrClient = getCloudSolrClient(cluster);
    httpclient = HttpClients.createDefault();
    // delete aliases first to avoid problems such as: https://issues.apache.org/jira/browse/SOLR-11839
    ZkStateReader zkStateReader = cluster.getSolrClient().getZkStateReader();
    zkStateReader.aliasesHolder.applyModificationAndExportToZk(aliases -> {
      Aliases a = zkStateReader.getAliases();
      for (String alias : a.getCollectionAliasMap().keySet()) {
        a = a.cloneWithCollectionAlias(alias,null); // remove
      }
      return a;
    });
    for (String col : CollectionAdminRequest.listCollections(solrClient)) {
      CollectionAdminRequest.deleteCollection(col).process(solrClient);
    }
  }

  @Test
  public void testV2() throws Exception {
    final String aliasName = "testAlias";
    cluster.uploadConfigSet(configset("_default"), aliasName);
    final String baseUrl = cluster.getRandomJetty(random()).getBaseUrl().toString();
        HttpPost post = new HttpPost(baseUrl + "/____v2/c");
        post.setHeader("Content-Type", ContentType.APPLICATION_JSON.getMimeType());
    HttpEntity httpEntity = new InputStreamEntity(org.apache.commons.io.IOUtils.toInputStream("{\n" +
        "  \"create-routed-alias\" : {\n" +
        "    \"name\": \"testaliasV2\",\n" +
        "    \"router\" : {\n" +
        "      \"name\": \"time\",\n" +
        "      \"field\": \"evt_dt\",\n" +
        "      \"interval\":\"+2HOUR\",\n" +
        "      \"max-future-ms\":\"14400000\"\n" +
        "    },\n" +
        "    \"start\":\"NOW/DAY\",\n" + // small window for test failure once a day.
        "    \"create-collection\" : {\n" +
        "      \"router\": {\n" +
        "        \"name\":\"implicit\",\n" +
        "        \"field\":\"foo_s\"\n" +
        "      },\n" +
        "      \"shards\":\"foo,bar,baz\",\n" +
        "      \"config\":\"_default\",\n" +
        "      \"numShards\": 2,\n" +
        "      \"tlogReplicas\":1,\n" +
        "      \"pullReplicas\":1,\n" +
        "      \"maxShardsPerNode\":3,\n" +
        "      \"properties\" : {\n" +
        "        \"foobar\":\"bazbam\"\n" +
        "      }\n" +
        "    }\n" +
        "  }\n" +
        "}", "UTF-8"), org.apache.http.entity.ContentType.APPLICATION_JSON);
    post.setEntity(httpEntity);
    try (CloseableHttpResponse response = httpclient.execute(post)) {
      assertEquals(200, response.getStatusLine().getStatusCode());
    }
    Date date = DateMathParser.parseMath(new Date(), "NOW/DAY");
    String initialCollectionName = TimeRoutedAliasUpdateProcessor
        .formatCollectionNameFromInstant("testaliasV2", date.toInstant(),
            TimeRoutedAliasUpdateProcessor.DATE_TIME_FORMATTER);
    HttpGet get = new HttpGet(baseUrl + "/____v2/c/"+ initialCollectionName);
    try (CloseableHttpResponse response = httpclient.execute(get)) {
      assertEquals(200, response.getStatusLine().getStatusCode());
    }

    Aliases aliases = cluster.getSolrClient().getZkStateReader().getAliases();
    Map<String, String> collectionAliasMap = aliases.getCollectionAliasMap();
    String alias = collectionAliasMap.get("testaliasV2");
    assertNotNull(alias);
    Map<String, String> meta = aliases.getCollectionAliasMetadata("testaliasV2");
    assertNotNull(meta);
    assertEquals("evt_dt",meta.get("router.field"));
    assertEquals("foo_s",meta.get("collection-create.router.field"));
    assertEquals("bazbam",meta.get("collection-create.property.foobar"));
  }

  @Test
  public void testV1() throws Exception {

    final String aliasName = "testAlias";
    cluster.uploadConfigSet(configset("_default"), aliasName);
    final String baseUrl = cluster.getRandomJetty(random()).getBaseUrl().toString();
    Instant instant = Instant.now().truncatedTo(ChronoUnit.HOURS); // mostly make sure no millis
    String timestamp = DateTimeFormatter.ISO_INSTANT.format(instant);
    HttpGet get = new HttpGet(baseUrl + "/admin/collections?action=CREATEROUTEDALIAS" +
        "&wt=xml" +
        "&name=testalias" +
        "&router.field=evt_dt" +
        "&router.name=time" +
        "&start=" + timestamp +
        "&router.interval=%2B30MINUTE" +
        "&router.max-future-ms=60000" +
        "&create-collection.collection.configName=_default" +
        "&create-collection.numShards=2");
    try (CloseableHttpResponse response = httpclient.execute(get)) {
      assertEquals(200, response.getStatusLine().getStatusCode());
    }
    String initialCollectionName = TimeRoutedAliasUpdateProcessor
        .formatCollectionNameFromInstant("testalias", instant,
            TimeRoutedAliasUpdateProcessor.DATE_TIME_FORMATTER);
    get = new HttpGet(baseUrl + "/____v2/c/"+ initialCollectionName);
    try (CloseableHttpResponse response = httpclient.execute(get)) {
      assertEquals(200, response.getStatusLine().getStatusCode());
    }

    Aliases aliases = cluster.getSolrClient().getZkStateReader().getAliases();
    Map<String, String> collectionAliasMap = aliases.getCollectionAliasMap();
    String alias = collectionAliasMap.get("testalias");
    assertNotNull(alias);
    Map<String, String> meta = aliases.getCollectionAliasMetadata("testalias");
    assertNotNull(meta);
    assertEquals("evt_dt",meta.get("router.field"));
    assertEquals("_default",meta.get("collection-create.collection.configName"));
    assertEquals("2",meta.get("collection-create.numShards"));
    assertEquals(null ,meta.get("start"));
  }

  @Test
  public void testAliasNameMustBeValid() throws Exception {

    final String aliasName = "testAlias";
    cluster.uploadConfigSet(configset("_default"), aliasName);
    final String baseUrl = cluster.getRandomJetty(random()).getBaseUrl().toString();
    Instant instant = Instant.now().truncatedTo(ChronoUnit.HOURS); // mostly make sure no millis
    String timestamp = DateTimeFormatter.ISO_INSTANT.format(instant);

    HttpGet get = new HttpGet(baseUrl + "/admin/collections?action=CREATEROUTEDALIAS" +
        "&wt=json" +
        "&name=735741!45" +  // ! not allowed
        "&router.field=evt_dt" +
        "&router.name=time" +
        "&start=" + timestamp +
        "&router.interval=%2B30MINUTE" +
        "&router.max-future-ms=60000" +
        "&create-collection.collection.configName=_default" +
        "&create-collection.numShards=2");
    try (CloseableHttpResponse response = httpclient.execute(get)) {
      assertEquals(400, response.getStatusLine().getStatusCode());
      assertErrorStartsWith(response,"Invalid alias");

    }
  }
  @Test
  public void testRandomRouterNameFails() throws Exception {

    final String aliasName = "testAlias";
    cluster.uploadConfigSet(configset("_default"), aliasName);
    final String baseUrl = cluster.getRandomJetty(random()).getBaseUrl().toString();
    Instant instant = Instant.now().truncatedTo(ChronoUnit.HOURS); // mostly make sure no millis
    String timestamp = DateTimeFormatter.ISO_INSTANT.format(instant);
    HttpGet get = new HttpGet(baseUrl + "/admin/collections?action=CREATEROUTEDALIAS" +
        "&wt=json" +
        "&name=testalias" +
        "&router.field=evt_dt" +
        "&router.name=tiafasme" +
        "&start=" + timestamp +
        "&router.interval=%2B30MINUTE" +
        "&router.max-future-ms=60000" +
        "&create-collection.collection.configName=_default" +
        "&create-collection.numShards=2");
    try (CloseableHttpResponse response = httpclient.execute(get)) {
      assertEquals(400, response.getStatusLine().getStatusCode());
      assertErrorStartsWith(response,"Only time based routing is supported");
    }
  }

  @Test
  public void testTimeStampWithMsFails() throws Exception {
    final String aliasName = "testAlias";
    cluster.uploadConfigSet(configset("_default"), aliasName);
    final String baseUrl = cluster.getRandomJetty(random()).getBaseUrl().toString();
    Instant instant = Instant.now().truncatedTo(ChronoUnit.HOURS).plus(123,ChronoUnit.MILLIS); // mostly make sure no millis
    String timestamp = DateTimeFormatter.ISO_INSTANT.format(instant);
    HttpGet get = new HttpGet(baseUrl + "/admin/collections?action=CREATEROUTEDALIAS" +
        "&wt=json" +
        "&name=testalias" +
        "&router.field=evt_dt" +
        "&router.name=time" +
        "&start=" + timestamp +
        "&router.interval=%2B30MINUTE" +
        "&router.max-future-ms=60000" +
        "&create-collection.collection.configName=_default" +
        "&create-collection.numShards=2");
    try (CloseableHttpResponse response = httpclient.execute(get)) {
      assertEquals(400, response.getStatusLine().getStatusCode());
      assertErrorStartsWith(response, "Start Time for the first collection must be a timestamp of the format yyyy-MM-dd_HH_mm_ss");
    }
  }

  private void assertErrorStartsWith(CloseableHttpResponse response, String prefix) throws IOException {
    String entity = getStringEntity(response);
    System.out.println(entity);
    ObjectMapper mapper = new ObjectMapper();
    Map map = mapper.readValue(entity, new TypeReference<Map<String, Object>>() {});
    Map exception = (Map) map.get("exception");
    if (exception == null) {
      exception = (Map) map.get("error");
    }
    String msg = (String) exception.get("msg");
    assertTrue(msg.toLowerCase(Locale.ROOT).startsWith(prefix.toLowerCase(Locale.ROOT)));
  }

  private String getStringEntity(CloseableHttpResponse response) throws IOException {
    ByteArrayOutputStream outstream = new ByteArrayOutputStream();
    response.getEntity().writeTo(outstream);
    return new String(outstream.toByteArray(), StandardCharsets.UTF_8);
  }

  @Test
  public void testBadDateMathIntervalFails() throws Exception {

    final String aliasName = "testAlias";
    cluster.uploadConfigSet(configset("_default"), aliasName);
    final String baseUrl = cluster.getRandomJetty(random()).getBaseUrl().toString();
    Instant instant = Instant.now().truncatedTo(ChronoUnit.HOURS); // mostly make sure no millis
    String timestamp = DateTimeFormatter.ISO_INSTANT.format(instant);
    HttpGet get = new HttpGet(baseUrl + "/admin/collections?action=CREATEROUTEDALIAS" +
        "&wt=json" +
        "&name=testalias" +
        "&router.field=evt_dt" +
        "&router.name=time" +
        "&start=" + timestamp +
        "&router.interval=%2B30MINUTEx" +
        "&router.max-future-ms=60000" +
        "&create-collection.collection.configName=_default" +
        "&create-collection.numShards=2");
    try (CloseableHttpResponse response = httpclient.execute(get)) {
      assertEquals(400, response.getStatusLine().getStatusCode());
      assertErrorStartsWith(response,"Invalid Date Math");
    }
  }
  @Test
  public void testNegativeFutureFails() throws Exception {

    final String aliasName = "testAlias";
    cluster.uploadConfigSet(configset("_default"), aliasName);
    final String baseUrl = cluster.getRandomJetty(random()).getBaseUrl().toString();
    Instant instant = Instant.now().truncatedTo(ChronoUnit.HOURS); // mostly make sure no millis
    String timestamp = DateTimeFormatter.ISO_INSTANT.format(instant);
    HttpGet get = new HttpGet(baseUrl + "/admin/collections?action=CREATEROUTEDALIAS" +
        "&wt=json" +
        "&name=testalias" +
        "&router.field=evt_dt" +
        "&router.name=time" +
        "&start=" + timestamp +
        "&router.interval=%2B30MINUTE" +
        "&router.max-future-ms=-60000" +
        "&create-collection.collection.configName=_default" +
        "&create-collection.numShards=2");
    try (CloseableHttpResponse response = httpclient.execute(get)) {
      assertEquals(400, response.getStatusLine().getStatusCode());
      assertErrorStartsWith(response, "router.max-future-ms must be a valid long integer");
    }
  }
  @Test
  public void testUnParseableFutureFails() throws Exception {

    final String aliasName = "testAlias";
    cluster.uploadConfigSet(configset("_default"), aliasName);
    final String baseUrl = cluster.getRandomJetty(random()).getBaseUrl().toString();
    Instant instant = Instant.now().truncatedTo(ChronoUnit.HOURS); // mostly make sure no millis
    String timestamp = DateTimeFormatter.ISO_INSTANT.format(instant);
    HttpGet get = new HttpGet(baseUrl + "/admin/collections?action=CREATEROUTEDALIAS" +
        "&wt=json" +
        "&name=testalias" +
        "&router.field=evt_dt" +
        "&router.name=time" +
        "&start=" + timestamp +
        "&router.interval=%2B30MINUTE" +
        "&router.max-future-ms=SixtyThousandMiliseconds" +
        "&create-collection.collection.configName=_default" +
        "&create-collection.numShards=2");
    try (CloseableHttpResponse response = httpclient.execute(get)) {
      assertEquals(400, response.getStatusLine().getStatusCode());
      assertErrorStartsWith(response, "router.max-future-ms must be a valid long integer");
    }
  }

  // not testing collection parameters, those should inherit error checking from the collection creation code.
}

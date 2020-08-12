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
package org.apache.solr.handler.component;

import static org.hamcrest.CoreMatchers.containsString;

import java.io.IOException;
import java.util.List;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.TermsResponse;
import org.apache.solr.client.solrj.response.TermsResponse.Term;
import org.apache.solr.cloud.ConfigRequest;
import org.apache.solr.cloud.MiniSolrCloudCluster;
import org.apache.solr.common.util.NamedList;
import org.junit.Test;

public class CustomTermsComponentTest extends ShardsWhitelistTest {

  public static class CustomTermsComponent extends TermsComponent {
    
    public void init( @SuppressWarnings({"rawtypes"})NamedList args )
    {
      super.init(args);
    }

    @Override
    protected void checkShardsWhitelist(final ResponseBuilder rb, final List<String> lst) {
      // ignore shards whitelist
    }

  }

  private static String addCustomHandlerWithTermsComponentConfig(final MiniSolrCloudCluster cluster, final String collection,
      final String defaultHandlerName, final String shardsWhitelist) throws Exception {
    return addCustomHandler(cluster, collection, defaultHandlerName, shardsWhitelist);
  }

  private static String addCustomHandlerWithCustomTermsComponent(final MiniSolrCloudCluster cluster, final String collection,
      final String defaultHandlerName) throws Exception {
    return addCustomHandler(cluster, collection, defaultHandlerName, null);
  }

  private static String addCustomHandler(final MiniSolrCloudCluster cluster, final String collection,
      final String defaultHandlerName, final String shardsWhitelist) throws Exception {

    // determine custom handler name (the exact name should not matter)
    final String customHandlerName = defaultHandlerName+"_custom"+random().nextInt();

    // determine custom terms component name (the exact name should not matter)
    final String customTermsComponentName = TermsComponent.COMPONENT_NAME+"_custom"+random().nextInt();

    // determine terms component class name and attributes
    final String customTermsComponentClass;
    final String customTermsComponentAttributesJSON;
    if (shardsWhitelist != null) {
      customTermsComponentClass = TermsComponent.class.getName();
      customTermsComponentAttributesJSON =
          "    '"+HttpShardHandlerFactory.INIT_SHARDS_WHITELIST+"' : '"+shardsWhitelist+"',\n";
    } else {
      customTermsComponentClass = CustomTermsComponent.class.getName();
      customTermsComponentAttributesJSON = "";
    }

    // add custom component
    cluster.getSolrClient().request(
        new ConfigRequest(
            "{\n" +
            "  'add-searchcomponent': {\n" +
            customTermsComponentAttributesJSON +
            "    'name': '"+customTermsComponentName+"',\n" +
            "    'class': '"+customTermsComponentClass+"'\n" +
            "  }\n" +
            "}"),
        collection);

    // add custom handler
    cluster.getSolrClient().request(
        new ConfigRequest(
            "{\n" +
            "  'add-requesthandler': {\n" +
            "    'name' : '"+customHandlerName+"',\n" +
            "    'class' : '"+SearchHandler.class.getName()+"',\n" +
            "    'components' : [ '"+QueryComponent.COMPONENT_NAME+"', '"+customTermsComponentName+"' ]\n" +
            "  }\n" +
            "}"),
        collection);

    return customHandlerName;
  }

  @Test
  @Override
  public void test() throws Exception {
    for (final String clusterId : clusterId2cluster.keySet()) {
      final MiniSolrCloudCluster cluster = clusterId2cluster.get(clusterId);
      final String collection = COLLECTION_NAME;
      doTest(cluster, collection);
    }
  }

  private static void doTest(final MiniSolrCloudCluster cluster, final String collection) throws Exception {

    // add some documents
    final String id = "id";
    final String f1 = "a_t";
    final String f2 = "b_t";
    final String v1 = "bee";
    final String v2 = "buzz";
    {
      new UpdateRequest()
          .add(sdoc(id, 1, f1, v1, f2, v2+" "+v2+" "+v2))
          .add(sdoc(id, 2, f1, v1+" "+v1, f2, v2+" "+v2))
          .add(sdoc(id, 3, f1, v1+" "+v1+" "+v1, f2, v2))
          .commit(cluster.getSolrClient(), collection);
    }

    // search for the documents' terms ...
    final String defaultHandlerName = "/select";

    // search with the default handler ...
    final String shards = findAndCheckTerms(cluster, collection,
        defaultHandlerName,
        null, // ... without specifying shards
        (random().nextBoolean() ? null : f1), v1,
        (random().nextBoolean() ? null : f2), v2,
        null);

    // search with the default handler ...
    findAndCheckTerms(cluster, collection,
        defaultHandlerName,
        shards, // ... with specified shards, but all valid
        (random().nextBoolean() ? null : f1), v1,
        (random().nextBoolean() ? null : f2), v2,
        null);
    
    ignoreException("not on the shards whitelist");
    // this case should fail
    findAndCheckTerms(cluster, collection,
        defaultHandlerName,
        shards + ",http://" + DEAD_HOST_1, // ... with specified shards with one invalid
        (random().nextBoolean() ? null : f1), v1,
        (random().nextBoolean() ? null : f2), v2,
        "No live SolrServers available to handle this request");
    unIgnoreException("not on the shards whitelist");

    // configure a custom handler ...
    final String customHandlerName;
    if (random().nextBoolean()) {
      // ... with a shards whitelist
      customHandlerName = addCustomHandlerWithTermsComponentConfig(cluster, collection, defaultHandlerName, shards);
    } else {
      // ... with a custom terms component that disregards shards whitelist logic
      customHandlerName = addCustomHandlerWithCustomTermsComponent(cluster, collection, defaultHandlerName);
    }

    // search with the custom handler ...
    findAndCheckTerms(cluster, collection,
        customHandlerName,
        shards, // ... with specified shards
        (random().nextBoolean() ? null : f1), v1,
        (random().nextBoolean() ? null : f2), v2,
        null);

  }

  private static String findAndCheckTerms(final MiniSolrCloudCluster cluster, final String collection,
      String requestHandlerName, String in_shards,
      String field1, String value1,
      String field2, String value2,
      String solrServerExceptionMessagePrefix) throws IOException {

      // compose the query ...
      final SolrQuery solrQuery =  new SolrQuery("*:*");
      solrQuery.setRequestHandler(requestHandlerName);
      solrQuery.add("shards.qt", requestHandlerName);
      // ... asking for terms ...
      solrQuery.setTerms(true);
      if (field1 != null) {
        solrQuery.addTermsField(field1);
      }
      if (field2 != null) {
        solrQuery.addTermsField(field2);
      }
      // ... and shards info ...
      solrQuery.add("shards.info", "true");
      // ... passing shards to use (if we have a preference)
      if (in_shards != null) {
        solrQuery.add("shards", in_shards);
      }

      // make the query
      final QueryResponse queryResponse;
      try {
        queryResponse = new QueryRequest(solrQuery)
            .process(cluster.getSolrClient(), collection);
        assertNull("expected exception ("+solrServerExceptionMessagePrefix+") not encountered", solrServerExceptionMessagePrefix);
      } catch (SolrServerException sse) {
        assertNotNull("unexpectedly caught exception "+sse, solrServerExceptionMessagePrefix);
        assertTrue(sse.getMessage().startsWith(solrServerExceptionMessagePrefix));
        assertThat(sse.getCause().getMessage(), containsString("not on the shards whitelist"));
        return null;
      }

      // analyse the response ...
      final TermsResponse termsResponse = queryResponse.getTermsResponse();
      // ... checking the terms returned ...
      checkTermsResponse(termsResponse, field1, value1);
      checkTermsResponse(termsResponse, field2, value2);
      // ... and assemble info about the shards ...
      final String out_shards = extractShardAddresses(queryResponse, ",");
      // ... to return to the caller
      return out_shards;
    }
  

  @SuppressWarnings("unchecked")
  private static String extractShardAddresses(final QueryResponse queryResponse, final String delimiter) {
    final StringBuilder sb = new StringBuilder();
    final NamedList<Object> nl = (NamedList<Object>)queryResponse.getResponse().get("shards.info");
    assertNotNull(queryResponse.toString(), nl);
    for (int ii = 0; ii < nl.size(); ++ii) {
      final String shardAddress = (String)((NamedList<Object>)nl.getVal(ii)).get("shardAddress");
      if (sb.length() > 0) {
        sb.append(delimiter);
      }
      sb.append(shardAddress);
    }
    return sb.toString();
  }

  private static void checkTermsResponse(TermsResponse termsResponse, String field, String value) {
    if (field != null) {
      final List<Term> ttList = termsResponse.getTerms(field);
      assertEquals(1, ttList.size());
      assertEquals(value, ttList.get(0).getTerm());
    }
  }

}

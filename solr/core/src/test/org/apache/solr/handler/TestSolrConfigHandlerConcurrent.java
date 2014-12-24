package org.apache.solr.handler;

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

import java.io.IOException;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.http.HttpEntity;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.util.EntityUtils;
import org.apache.lucene.queryparser.xml.ParserException;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.impl.CloudSolrServer;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.cloud.AbstractFullDistribZkTestBase;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.core.ConfigOverlay;
import org.apache.solr.util.RESTfulServerProvider;
import org.apache.solr.util.RestTestHarness;
import org.noggit.JSONParser;
import org.noggit.ObjectBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Arrays.asList;
import static org.apache.solr.core.ConfigOverlay.getObjectByPath;
import static org.apache.solr.rest.schema.TestBulkSchemaAPI.getAsMap;
import static org.noggit.ObjectBuilder.getVal;


public class TestSolrConfigHandlerConcurrent extends AbstractFullDistribZkTestBase {


  static final Logger log =  LoggerFactory.getLogger(TestSolrConfigHandlerConcurrent.class);
  private List<RestTestHarness> restTestHarnesses = new ArrayList<>();

  private void setupHarnesses() {
    for (final SolrServer client : clients) {
      RestTestHarness harness = new RestTestHarness(new RESTfulServerProvider() {
        @Override
        public String getBaseURL() {
          return ((HttpSolrServer)client).getBaseURL();
        }
      });
      restTestHarnesses.add(harness);
    }
  }

  @Override
  public void doTest() throws Exception {
    Map editable_prop_map = (Map) new ObjectBuilder(new JSONParser(new StringReader(
        ConfigOverlay.MAPPING))).getObject();
    Map caches = (Map) editable_prop_map.get("query");

    setupHarnesses();
    List<Thread> threads = new ArrayList<>(caches.size());
    final List<List> collectErrors = new ArrayList<>();

    for (Object o : caches.entrySet()) {
      final Map.Entry e = (Map.Entry) o;
      Thread t = new Thread() {
        @Override
        public void run() {
          try {
            ArrayList errs = new ArrayList();
            collectErrors.add(errs);
            invokeBulkCall((String)e.getKey() , errs, (Map) e.getValue());
          } catch (IOException e) {
            e.printStackTrace();
          } catch (Exception e) {
            e.printStackTrace();
          }

        }
      };
      threads.add(t);
      t.start();
    }


    for (Thread thread : threads) thread.join();

    boolean success = true;

    for (List e : collectErrors) {
      if(!e.isEmpty()){
        success = false;
        log.error(e.toString());
      }

    }

    assertTrue(collectErrors.toString(), success);


  }


  private void invokeBulkCall(String  cacheName, List<String> errs, Map val) throws Exception {

    String payload = "{" +
        "'set-property' : {'query.CACHENAME.size':'CACHEVAL1'," +
        "                  'query.CACHENAME.initialSize':'CACHEVAL2'}," +
        "'set-property': {'query.CACHENAME.autowarmCount' : 'CACHEVAL3'}" +
        "}";

    Set<String> errmessages = new HashSet<>();
    for(int i =1;i<2;i++){//make it  ahigher number
      RestTestHarness publisher = restTestHarnesses.get(r.nextInt(restTestHarnesses.size()));
      payload = payload.replaceAll("CACHENAME" , cacheName);
      String val1 = String.valueOf(10 * i + 1);
      payload = payload.replace("CACHEVAL1", val1);
      String val2 = String.valueOf(10 * i + 2);
      payload = payload.replace("CACHEVAL2", val2);
      String val3 = String.valueOf(10 * i + 3);
      payload = payload.replace("CACHEVAL3", val3);

      String response = publisher.post("/config?wt=json", SolrTestCaseJ4.json(payload));
      Map map = (Map) getVal(new JSONParser(new StringReader(response)));
      Object errors = map.get("errors");
      if(errors!= null){
        errs.add(new String(ZkStateReader.toJSON(errors), StandardCharsets.UTF_8));
        return;
      }

      DocCollection coll = cloudClient.getZkStateReader().getClusterState().getCollection("collection1");
      List<String> urls = new ArrayList<>();
      for (Slice slice : coll.getSlices()) {
        for (Replica replica : slice.getReplicas())
          urls.add(""+replica.get(ZkStateReader.BASE_URL_PROP) + "/"+replica.get(ZkStateReader.CORE_NAME_PROP));
      }


      //get another node
      String url = urls.get(urls.size());

      long startTime = System.nanoTime();
      long maxTimeoutSeconds = 20;
      while ( TimeUnit.SECONDS.convert(System.nanoTime() - startTime, TimeUnit.NANOSECONDS) < maxTimeoutSeconds) {
        Thread.sleep(100);
        errmessages.clear();
        Map respMap = getAsMap(url+"/config/overlay?wt=json", cloudClient);
        Map m = (Map) respMap.get("overlay");
        if(m!= null) m = (Map) m.get("props");
        if(m == null) {
          errmessages.add(MessageFormat.format( "overlay does not exist for cache: {0} , iteration: {1} response {2} ", cacheName, i, respMap.toString()));
          continue;
        }


        Object o = getObjectByPath(m, true, asList("query", cacheName, "size"));
        if(!val1.equals(o)) errmessages.add(MessageFormat.format("'size' property not set, expected = {0}, actual {1}", val1,o));

        o = getObjectByPath(m, true, asList("query", cacheName, "initialSize"));
        if(!val2.equals(o)) errmessages.add(MessageFormat.format("'initialSize' property not set, expected = {0}, actual {1}", val2,o));

        o = getObjectByPath(m, true, asList("query", cacheName, "autowarmCount"));
        if(!val3.equals(o)) errmessages.add(MessageFormat.format("'autowarmCount' property not set, expected = {0}, actual {1}", val3,o));
        if(errmessages.isEmpty()) break;
      }
      if(!errmessages.isEmpty()) {
        errs.addAll(errmessages);
        return;
      }
    }

  }

  public static Map getAsMap(String uri, CloudSolrServer cloudClient) throws Exception {
    HttpGet get = new HttpGet(uri) ;
    HttpEntity entity = null;
    try {
      entity = cloudClient.getLbServer().getHttpClient().execute(get).getEntity();
      String response = EntityUtils.toString(entity, StandardCharsets.UTF_8);
      try {
        return (Map) ObjectBuilder.getVal(new JSONParser(new StringReader(response)));
      } catch (JSONParser.ParseException e) {
        log.error(response,e);
        throw e;
      }
    } finally {
      EntityUtils.consumeQuietly(entity);
      get.releaseConnection();
    }
  }
}

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

import static java.util.Arrays.asList;
import static org.apache.solr.common.util.Utils.getObjectByPath;
import static org.noggit.ObjectBuilder.getVal;

import java.io.StringReader;
import java.lang.invoke.MethodHandles;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.http.HttpEntity;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.util.EntityUtils;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.cloud.AbstractFullDistribZkTestBase;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.common.util.Utils;
import org.apache.solr.core.ConfigOverlay;
import org.apache.solr.util.RESTfulServerProvider;
import org.apache.solr.util.RestTestHarness;
import org.junit.Test;
import org.noggit.JSONParser;
import org.noggit.ObjectBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class TestSolrConfigHandlerConcurrent extends AbstractFullDistribZkTestBase {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private List<RestTestHarness> restTestHarnesses = new ArrayList<>();

  private void setupHarnesses() {
    for (final SolrClient client : clients) {
      RestTestHarness harness = new RestTestHarness(new RESTfulServerProvider() {
        @Override
        public String getBaseURL() {
          return ((HttpSolrClient)client).getBaseURL();
        }
      });
      restTestHarnesses.add(harness);
    }
  }

  @Override
  public void distribTearDown() throws Exception {
    super.distribTearDown();
    for (RestTestHarness h : restTestHarnesses) {
      h.close();
    }
  }

  @Test
  public void test() throws Exception {
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
      String response;
      String val1;
      String val2;
      String val3;
      try {
        payload = payload.replaceAll("CACHENAME" , cacheName);
        val1 = String.valueOf(10 * i + 1);
        payload = payload.replace("CACHEVAL1", val1);
        val2 = String.valueOf(10 * i + 2);
        payload = payload.replace("CACHEVAL2", val2);
        val3 = String.valueOf(10 * i + 3);
        payload = payload.replace("CACHEVAL3", val3);
  
        response = publisher.post("/config?wt=json", SolrTestCaseJ4.json(payload));
      } finally {
        publisher.close();
      }
      
      Map map = (Map) getVal(new JSONParser(new StringReader(response)));
      Object errors = map.get("errors");
      if(errors!= null){
        errs.add(new String(Utils.toJSON(errors), StandardCharsets.UTF_8));
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
          errmessages.add(StrUtils.formatString("overlay does not exist for cache: {0} , iteration: {1} response {2} ", cacheName, i, respMap.toString()));
          continue;
        }


        Object o = getObjectByPath(m, true, asList("query", cacheName, "size"));
        if(!val1.equals(o)) errmessages.add(StrUtils.formatString("'size' property not set, expected = {0}, actual {1}", val1, o));

        o = getObjectByPath(m, true, asList("query", cacheName, "initialSize"));
        if(!val2.equals(o)) errmessages.add(StrUtils.formatString("'initialSize' property not set, expected = {0}, actual {1}", val2, o));

        o = getObjectByPath(m, true, asList("query", cacheName, "autowarmCount"));
        if(!val3.equals(o)) errmessages.add(StrUtils.formatString("'autowarmCount' property not set, expected = {0}, actual {1}", val3, o));
        if(errmessages.isEmpty()) break;
      }
      if(!errmessages.isEmpty()) {
        errs.addAll(errmessages);
        return;
      }
    }

  }

  public static Map getAsMap(String uri, CloudSolrClient cloudClient) throws Exception {
    HttpGet get = new HttpGet(uri) ;
    HttpEntity entity = null;
    try {
      entity = cloudClient.getLbClient().getHttpClient().execute(get).getEntity();
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

package org.apache.solr.schema;

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


import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.cloud.AbstractFullDistribZkTestBase;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.util.RESTfulServerProvider;
import org.apache.solr.util.RestTestHarness;
import org.noggit.JSONParser;
import org.noggit.ObjectBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static java.text.MessageFormat.format;
import static org.apache.solr.rest.schema.TestBulkSchemaAPI.getCopyFields;
import static org.apache.solr.rest.schema.TestBulkSchemaAPI.getObj;

public class TestBulkSchemaConcurrent  extends AbstractFullDistribZkTestBase {
  static final Logger log =  LoggerFactory.getLogger(TestBulkSchemaConcurrent.class);
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

    final int threadCount = 5;
    setupHarnesses();
    Thread[] threads = new Thread[threadCount];
    final List<List> collectErrors = new ArrayList<>();


    for(int i=0;i<threadCount;i++){
      final int finalI = i;
      threads[i] = new Thread(){
        @Override
        public void run() {
          try {
            ArrayList errs = new ArrayList();
            collectErrors.add(errs);
            invokeBulkCall(finalI,errs);
          } catch (IOException e) {
            e.printStackTrace();
          } catch (Exception e) {
            e.printStackTrace();
          }

        }
      };

      threads[i].start();
    }

    for (Thread thread : threads) thread.join();

    boolean success = true;

    for (List e : collectErrors) {
      if(!e.isEmpty()){
        success = false;
        log.error(e.toString());
      }

    }

    assertTrue(success);


  }

  private void invokeBulkCall(int seed, ArrayList<String> errs) throws Exception {
    String payload = "{\n" +
        "          'add-field' : {\n" +
        "                       'name':'replaceFieldA',\n" +
        "                       'type': 'string',\n" +
        "                       'stored':true,\n" +
        "                       'indexed':false\n" +
        "                       },\n" +
        "          'add-dynamic-field' : {\n" +
        "                       'name' :'replaceDynamicField',\n" +
        "                        'type':'string',\n" +
        "                        'stored':true,\n" +
        "                        'indexed':true\n" +
        "                        },\n" +
        "          'add-copy-field' : {\n" +
        "                       'source' :'replaceFieldA',\n" +
        "                        'dest':['replaceDynamicCopyFieldDest']\n" +
        "                        },\n" +
        "          'add-field-type' : {\n" +
        "                       'name' :'myNewFieldTypeName',\n" +
        "                       'class' : 'solr.StrField',\n" +
        "                        'sortMissingLast':'true'\n" +
        "                        }\n" +
        "\n" +
        " }";
    String aField = "a" + seed;
    String dynamicFldName = "*_lol" + seed;
    String dynamicCopyFldDest = "hello_lol"+seed;
    String newFieldTypeName = "mystr" + seed;


    RestTestHarness publisher = restTestHarnesses.get(r.nextInt(restTestHarnesses.size()));
    payload = payload.replace("replaceFieldA1", aField);

    payload = payload.replace("replaceDynamicField", dynamicFldName);
    payload = payload.replace("dynamicFieldLol","lol"+seed);

    payload = payload.replace("replaceDynamicCopyFieldDest",dynamicCopyFldDest);
    payload = payload.replace("myNewFieldTypeName", newFieldTypeName);
    String response = publisher.post("/schema?wt=json", SolrTestCaseJ4.json(payload));
    Map map = (Map) ObjectBuilder.getVal(new JSONParser(new StringReader(response)));
    Object errors = map.get("errors");
    if(errors!= null){
      errs.add(new String(ZkStateReader.toJSON(errors), StandardCharsets.UTF_8));
      return;
    }

    //get another node
    RestTestHarness harness = restTestHarnesses.get(r.nextInt(restTestHarnesses.size()));
    long startTime = System.nanoTime();
    boolean success = false;
    long maxTimeoutMillis = 100000;
    Set<String> errmessages = new HashSet<>();
    while ( ! success
        && TimeUnit.MILLISECONDS.convert(System.nanoTime() - startTime, TimeUnit.NANOSECONDS) < maxTimeoutMillis) {
      errmessages.clear();
      Map m = getObj(harness, aField, "fields");
      if(m== null) errmessages.add(format("field {0} not created", aField));

      m = getObj(harness, dynamicFldName, "dynamicFields");
      if(m== null) errmessages.add(format("dynamic field {0} not created", dynamicFldName));

      List l = getCopyFields(harness, "a1");
      if(!checkCopyField(l,aField,dynamicCopyFldDest))
        errmessages.add(format("CopyField source={0},dest={1} not created" , aField,dynamicCopyFldDest));

      m = getObj(harness, "mystr", "fieldTypes");
      if(m == null) errmessages.add(format("new type {}  not created" , newFieldTypeName));
      Thread.sleep(10);
    }
    if(!errmessages.isEmpty()){
      errs.addAll(errmessages);
    }
  }

  private boolean checkCopyField(List<Map> l, String src, String dest) {
    if(l == null) return false;
    for (Map map : l) {
      if(src.equals(map.get("source")) &&
          dest.equals(map.get("dest"))) return true;
    }
    return false;
  }


}

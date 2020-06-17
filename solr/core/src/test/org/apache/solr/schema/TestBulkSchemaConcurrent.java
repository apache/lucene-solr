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
package org.apache.solr.schema;

import java.lang.invoke.MethodHandles;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.cloud.AbstractFullDistribZkTestBase;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.common.util.Utils;
import org.apache.solr.util.RestTestHarness;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.rest.schema.TestBulkSchemaAPI.getObj;
import static org.apache.solr.rest.schema.TestBulkSchemaAPI.getSourceCopyFields;

public class TestBulkSchemaConcurrent  extends AbstractFullDistribZkTestBase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @BeforeClass
  public static void initSysProperties() {
    System.setProperty("managed.schema.mutable", "true");
    System.setProperty("enable.update.log", "true");
  }

  protected String getCloudSolrConfig() {
    return "solrconfig-managed-schema.xml";
  }

  @Test
  @SuppressWarnings({"unchecked"})
  public void test() throws Exception {

    final int threadCount = 5;
    setupRestTestHarnesses();
    Thread[] threads = new Thread[threadCount];
    @SuppressWarnings({"rawtypes"})
    final List<List> collectErrors = Collections.synchronizedList(new ArrayList<>());

    for (int i = 0 ; i < threadCount ; i++) {
      final int finalI = i;
      threads[i] = new Thread(){
        @Override
        public void run() {
          @SuppressWarnings({"rawtypes"})
          ArrayList errs = new ArrayList();
          collectErrors.add(errs);
          try {
            invokeBulkAddCall(finalI, errs);
            invokeBulkReplaceCall(finalI, errs);
            invokeBulkDeleteCall(finalI, errs);
          } catch (Exception e) {
            e.printStackTrace();
          }
        }
      };

      threads[i].start();
    }

    for (Thread thread : threads) thread.join();

    boolean success = true;

    for (@SuppressWarnings({"rawtypes"})List e : collectErrors) {
      if (e != null &&  !e.isEmpty()) {
        success = false;
        log.error("{}", e);
      }
    }

    assertTrue(collectErrors.toString(), success);
  }

  @SuppressWarnings({"unchecked"})
  private void invokeBulkAddCall(int seed, ArrayList<String> errs) throws Exception {
    String payload = "{\n" +
        "          'add-field' : {\n" +
        "                       'name':'replaceFieldA',\n" +
        "                       'type': 'string',\n" +
        "                       'stored':true,\n" +
        "                       'indexed':false\n" +
        "                       },\n" +
        "          'add-dynamic-field' : {\n" +
        "                       'name' :'replaceDynamicField',\n" +
        "                       'type':'string',\n" +
        "                       'stored':true,\n" +
        "                       'indexed':true\n" +
        "                       },\n" +
        "          'add-copy-field' : {\n" +
        "                       'source' :'replaceFieldA',\n" +
        "                       'dest':['replaceDynamicCopyFieldDest']\n" +
        "                       },\n" +
        "          'add-field-type' : {\n" +
        "                       'name' :'myNewFieldTypeName',\n" +
        "                       'class' : 'solr.StrField',\n" +
        "                       'sortMissingLast':'true'\n" +
        "                       }\n" +
        " }";
    String aField = "a" + seed;
    String dynamicFldName = "*_lol" + seed;
    String dynamicCopyFldDest = "hello_lol"+seed;
    String newFieldTypeName = "mystr" + seed;

    payload = payload.replace("replaceFieldA", aField);
    payload = payload.replace("replaceDynamicField", dynamicFldName);
    payload = payload.replace("replaceDynamicCopyFieldDest", dynamicCopyFldDest);
    payload = payload.replace("myNewFieldTypeName", newFieldTypeName);

    RestTestHarness publisher = randomRestTestHarness(r);
    String response = publisher.post("/schema", SolrTestCaseJ4.json(payload));
    @SuppressWarnings({"rawtypes"})
    Map map = (Map) Utils.fromJSONString(response);
    Object errors = map.get("errors");
    if (errors != null) {
      errs.add(new String(Utils.toJSON(errors), StandardCharsets.UTF_8));
      return;
    }

    //get another node
    Set<String> errmessages = new HashSet<>();
    RestTestHarness harness = randomRestTestHarness(r);
    try {
      long startTime = System.nanoTime();
      long maxTimeoutMillis = 100000;
      while (TimeUnit.MILLISECONDS.convert(System.nanoTime() - startTime, TimeUnit.NANOSECONDS) < maxTimeoutMillis) {
        errmessages.clear();
        @SuppressWarnings({"rawtypes"})
        Map m = getObj(harness, aField, "fields");
        if (m == null) errmessages.add(StrUtils.formatString("field {0} not created", aField));
        
        m = getObj(harness, dynamicFldName, "dynamicFields");
        if (m == null) errmessages.add(StrUtils.formatString("dynamic field {0} not created", dynamicFldName));

        @SuppressWarnings({"rawtypes"})
        List l = getSourceCopyFields(harness, aField);
        if (!checkCopyField(l, aField, dynamicCopyFldDest))
          errmessages.add(StrUtils.formatString("CopyField source={0},dest={1} not created", aField, dynamicCopyFldDest));
        
        m = getObj(harness, newFieldTypeName, "fieldTypes");
        if (m == null) errmessages.add(StrUtils.formatString("new type {0}  not created", newFieldTypeName));
        
        if (errmessages.isEmpty()) break;
        
        Thread.sleep(10);
      }
    } finally {
      harness.close();
    }
    if (!errmessages.isEmpty()) {
      errs.addAll(errmessages);
    }
  }

  @SuppressWarnings({"unchecked"})
  private void invokeBulkReplaceCall(int seed, ArrayList<String> errs) throws Exception {
    String payload = "{\n" +
        "          'replace-field' : {\n" +
        "                       'name':'replaceFieldA',\n" +
        "                       'type': 'text',\n" +
        "                       'stored':true,\n" +
        "                       'indexed':true\n" +
        "                       },\n" +
        "          'replace-dynamic-field' : {\n" +
        "                       'name' :'replaceDynamicField',\n" +
        "                        'type':'text',\n" +
        "                        'stored':true,\n" +
        "                        'indexed':true\n" +
        "                        },\n" +
        "          'replace-field-type' : {\n" +
        "                       'name' :'myNewFieldTypeName',\n" +
        "                       'class' : 'solr.TextField'\n" +
        "                        }\n" +
        " }";
    String aField = "a" + seed;
    String dynamicFldName = "*_lol" + seed;
    String dynamicCopyFldDest = "hello_lol"+seed;
    String newFieldTypeName = "mystr" + seed;

    payload = payload.replace("replaceFieldA", aField);
    payload = payload.replace("replaceDynamicField", dynamicFldName);
    payload = payload.replace("myNewFieldTypeName", newFieldTypeName);

    RestTestHarness publisher = randomRestTestHarness(r);
    String response = publisher.post("/schema", SolrTestCaseJ4.json(payload));
    @SuppressWarnings({"rawtypes"})
    Map map = (Map) Utils.fromJSONString(response);
    Object errors = map.get("errors");
    if (errors != null) {
      errs.add(new String(Utils.toJSON(errors), StandardCharsets.UTF_8));
      return;
    }

    //get another node
    Set<String> errmessages = new HashSet<>();
    RestTestHarness harness = randomRestTestHarness(r);
    try {
      long startTime = System.nanoTime();
      long maxTimeoutMillis = 100000;
      while (TimeUnit.MILLISECONDS.convert(System.nanoTime() - startTime, TimeUnit.NANOSECONDS) < maxTimeoutMillis) {
        errmessages.clear();
        @SuppressWarnings({"rawtypes"})
        Map m = getObj(harness, aField, "fields");
        if (m == null) errmessages.add(StrUtils.formatString("field {0} no longer present", aField));

        m = getObj(harness, dynamicFldName, "dynamicFields");
        if (m == null) errmessages.add(StrUtils.formatString("dynamic field {0} no longer present", dynamicFldName));

        @SuppressWarnings({"rawtypes"})
        List l = getSourceCopyFields(harness, aField);
        if (!checkCopyField(l, aField, dynamicCopyFldDest))
          errmessages.add(StrUtils.formatString("CopyField source={0},dest={1} no longer present", aField, dynamicCopyFldDest));

        m = getObj(harness, newFieldTypeName, "fieldTypes");
        if (m == null) errmessages.add(StrUtils.formatString("new type {0} no longer present", newFieldTypeName));

        if (errmessages.isEmpty()) break;

        Thread.sleep(10);
      }
    } finally {
      harness.close();
    }
    if (!errmessages.isEmpty()) {
      errs.addAll(errmessages);
    }
  }

  @SuppressWarnings({"unchecked"})
  private void invokeBulkDeleteCall(int seed, ArrayList<String> errs) throws Exception {
    String payload = "{\n" +
        "          'delete-copy-field' : {\n" +
        "                       'source' :'replaceFieldA',\n" +
        "                       'dest':['replaceDynamicCopyFieldDest']\n" +
        "                       },\n" +
        "          'delete-field' : {'name':'replaceFieldA'},\n" +
        "          'delete-dynamic-field' : {'name' :'replaceDynamicField'},\n" +
        "          'delete-field-type' : {'name' :'myNewFieldTypeName'}\n" +
        " }";
    String aField = "a" + seed;
    String dynamicFldName = "*_lol" + seed;
    String dynamicCopyFldDest = "hello_lol"+seed;
    String newFieldTypeName = "mystr" + seed;

    payload = payload.replace("replaceFieldA", aField);
    payload = payload.replace("replaceDynamicField", dynamicFldName);
    payload = payload.replace("replaceDynamicCopyFieldDest",dynamicCopyFldDest);
    payload = payload.replace("myNewFieldTypeName", newFieldTypeName);

    RestTestHarness publisher = randomRestTestHarness(r);
    String response = publisher.post("/schema", SolrTestCaseJ4.json(payload));
    @SuppressWarnings({"rawtypes"})
    Map map = (Map) Utils.fromJSONString(response);
    Object errors = map.get("errors");
    if (errors != null) {
      errs.add(new String(Utils.toJSON(errors), StandardCharsets.UTF_8));
      return;
    }

    //get another node
    Set<String> errmessages = new HashSet<>();
    RestTestHarness harness = randomRestTestHarness(r);
    try {
      long startTime = System.nanoTime();
      long maxTimeoutMillis = 100000;
      while (TimeUnit.MILLISECONDS.convert(System.nanoTime() - startTime, TimeUnit.NANOSECONDS) < maxTimeoutMillis) {
        errmessages.clear();
        @SuppressWarnings({"rawtypes"})
        Map m = getObj(harness, aField, "fields");
        if (m != null) errmessages.add(StrUtils.formatString("field {0} still exists", aField));

        m = getObj(harness, dynamicFldName, "dynamicFields");
        if (m != null) errmessages.add(StrUtils.formatString("dynamic field {0} still exists", dynamicFldName));

        @SuppressWarnings({"rawtypes"})
        List l = getSourceCopyFields(harness, aField);
        if (checkCopyField(l, aField, dynamicCopyFldDest))
          errmessages.add(StrUtils.formatString("CopyField source={0},dest={1} still exists", aField, dynamicCopyFldDest));

        m = getObj(harness, newFieldTypeName, "fieldTypes");
        if (m != null) errmessages.add(StrUtils.formatString("new type {0} still exists", newFieldTypeName));

        if (errmessages.isEmpty()) break;

        Thread.sleep(10);
      }
    } finally {
      harness.close();
    }
    if (!errmessages.isEmpty()) {
      errs.addAll(errmessages);
    }
  }

  private boolean checkCopyField(@SuppressWarnings({"rawtypes"})List<Map> l, String src, String dest) {
    if (l == null) return false;
    for (@SuppressWarnings({"rawtypes"})Map map : l) {
      if (src.equals(map.get("source")) && dest.equals(map.get("dest"))) 
        return true;
    }
    return false;
  }
}

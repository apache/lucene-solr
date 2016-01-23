package org.apache.solr.update.processor;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import com.google.common.collect.ImmutableMap;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.schema.TrieDateField;
import org.apache.solr.util.DateFormatUtil;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

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

public class AtomicUpdatesTest extends SolrTestCaseJ4 {

  @BeforeClass
  public static void beforeTests() throws Exception {
    System.setProperty("enable.update.log", "true");
    initCore("solrconfig.xml", "schema.xml");
  }

  @Before
  public void before() {
    h.update("<delete><query>*:*</query></delete>");
    assertU(commit());
  }
  @Test
  public void testRemove() throws Exception {
    SolrInputDocument doc;

    doc = new SolrInputDocument();
    doc.setField("id", "1");
    doc.setField("cat", new String[]{"aaa", "bbb", "ccc", "ccc", "ddd"});
    assertU(adoc(doc));

    doc = new SolrInputDocument();
    doc.setField("id", "2");
    doc.setField("cat", new String[]{"aaa", "bbb", "bbb", "ccc", "ddd"});
    assertU(adoc(doc));


    doc = new SolrInputDocument();
    doc.setField("id", "20");
    doc.setField("cat", new String[]{"aaa", "ccc", "ddd"});
    assertU(adoc(doc));

    doc = new SolrInputDocument();
    doc.setField("id", "21");
    doc.setField("cat", new String[]{"aaa", "bbb", "ddd"});
    assertU(adoc(doc));


    assertU(commit());

    assertQ(req("q", "cat:*", "indent", "true"), "//result[@numFound = '4']");
    assertQ(req("q", "cat:bbb", "indent", "true"), "//result[@numFound = '3']");


    doc = new SolrInputDocument();
    doc.setField("id", "1");
    List<String> removeList = new ArrayList<String>();
    removeList.add("bbb");
    removeList.add("ccc");
    doc.setField("cat", ImmutableMap.of("remove", removeList)); //behavior when hitting Solr through ZK
    assertU(adoc(doc));
    assertU(commit());

    assertQ(req("q", "cat:*", "indent", "true"), "//result[@numFound = '4']");
    assertQ(req("q", "cat:bbb", "indent", "true"), "//result[@numFound = '2']");

    doc = new SolrInputDocument();
    doc.setField("id", "21");
    removeList = new ArrayList<String>();
    removeList.add("bbb");
    removeList.add("ccc");
    doc.setField("cat", ImmutableMap.of("remove", removeList)); //behavior when hitting Solr through ZK
    assertU(adoc(doc));
    assertU(commit());

    assertQ(req("q", "cat:*", "indent", "true"), "//result[@numFound = '4']");
    assertQ(req("q", "cat:bbb", "indent", "true"), "//result[@numFound = '1']");

    doc = new SolrInputDocument();
    doc.setField("id", "1");
    doc.setField("cat", ImmutableMap.of("remove", "aaa")); //behavior when hitting Solr directly

    assertU(adoc(doc));
    assertU(commit());

    assertQ(req("q", "cat:*", "indent", "true"), "//result[@numFound = '4']");
    assertQ(req("q", "cat:aaa", "indent", "true"), "//result[@numFound = '3']");
  }

  @Test
  public void testRemoveInteger() throws Exception {
    SolrInputDocument doc;

    doc = new SolrInputDocument();
    doc.setField("id", "1001");
    doc.setField("intRemove", new String[]{"111", "222", "333", "333", "444"});
    
    assertU(adoc(doc));

    doc = new SolrInputDocument();
    doc.setField("id", "1002");
    doc.setField("intRemove", new String[]{"111", "222", "222", "333", "444"});
    assertU(adoc(doc));


    doc = new SolrInputDocument();
    doc.setField("id", "1020");
    doc.setField("intRemove", new String[]{"111", "333", "444"});
    assertU(adoc(doc));

    doc = new SolrInputDocument();
    doc.setField("id", "1021");
    doc.setField("intRemove", new String[]{"111", "222", "444"});
    assertU(adoc(doc));

    assertU(commit());

    assertQ(req("q", "intRemove:*", "indent", "true"), "//result[@numFound = '4']");
    assertQ(req("q", "intRemove:222", "indent", "true"), "//result[@numFound = '3']");


    doc = new SolrInputDocument();
    doc.setField("id", "1001");
    List<Long> removeList = new ArrayList<Long>();
    removeList.add(new Long(222));
    removeList.add(new Long(333));
    doc.setField("intRemove", ImmutableMap.of("remove", removeList)); //behavior when hitting Solr through ZK
    assertU(adoc(doc));
    assertU(commit());

    assertQ(req("q", "intRemove:*", "indent", "true"), "//result[@numFound = '4']");
    assertQ(req("q", "intRemove:222", "indent", "true"), "//result[@numFound = '2']");

    doc = new SolrInputDocument();
    doc.setField("id", "1021");
    removeList = new ArrayList<Long>();
    removeList.add(new Long(222));
    removeList.add(new Long(333));    
    doc.setField("intRemove", ImmutableMap.of("remove", removeList)); //behavior when hitting Solr through ZK
    assertU(adoc(doc));
    assertU(commit());

    assertQ(req("q", "intRemove:*", "indent", "true"), "//result[@numFound = '4']");
    assertQ(req("q", "intRemove:222", "indent", "true"), "//result[@numFound = '1']");

    doc = new SolrInputDocument();
    doc.setField("id", "1001");
    doc.setField("intRemove", ImmutableMap.of("remove", 111)); //behavior when hitting Solr directly

    assertU(adoc(doc));
    assertU(commit());

    assertQ(req("q", "intRemove:*", "indent", "true"), "//result[@numFound = '4']");
    assertQ(req("q", "intRemove:111", "indent", "true"), "//result[@numFound = '3']");
  }


  @Test
  public void testRemoveIntegerInDocSavedWithInteger() throws Exception {
    SolrInputDocument doc;

    doc = new SolrInputDocument();
    doc.setField("id", "1001");
    doc.setField("intRemove", new Integer[]{111, 222, 333, 333, 444});
    
    assertU(adoc(doc));

    doc = new SolrInputDocument();
    doc.setField("id", "1002");
    doc.setField("intRemove", new Integer[]{111, 222, 222, 333, 444});
    assertU(adoc(doc));


    doc = new SolrInputDocument();
    doc.setField("id", "1020");
    doc.setField("intRemove", new Integer[]{111, 333, 444});
    assertU(adoc(doc));

    doc = new SolrInputDocument();
    doc.setField("id", "1021");
    doc.setField("intRemove", new Integer[]{111, 222, 444});
    assertU(adoc(doc));

    assertU(commit());

    assertQ(req("q", "intRemove:*", "indent", "true"), "//result[@numFound = '4']");
    assertQ(req("q", "intRemove:222", "indent", "true"), "//result[@numFound = '3']");


    doc = new SolrInputDocument();
    doc.setField("id", "1001");
    List<Long> removeList = new ArrayList<Long>();
    removeList.add(new Long(222));
    removeList.add(new Long(333));
    doc.setField("intRemove", ImmutableMap.of("remove", removeList)); //behavior when hitting Solr through ZK
    assertU(adoc(doc));
    assertU(commit());

    assertQ(req("q", "intRemove:*", "indent", "true"), "//result[@numFound = '4']");
    assertQ(req("q", "intRemove:222", "indent", "true"), "//result[@numFound = '2']");

    doc = new SolrInputDocument();
    doc.setField("id", "1021");
    removeList = new ArrayList<Long>();
    removeList.add(new Long(222));
    removeList.add(new Long(333));    
    doc.setField("intRemove", ImmutableMap.of("remove", removeList)); //behavior when hitting Solr through ZK
    assertU(adoc(doc));
    assertU(commit());

    assertQ(req("q", "intRemove:*", "indent", "true"), "//result[@numFound = '4']");
    assertQ(req("q", "intRemove:222", "indent", "true"), "//result[@numFound = '1']");

    doc = new SolrInputDocument();
    doc.setField("id", "1001");
    doc.setField("intRemove", ImmutableMap.of("remove", 111)); //behavior when hitting Solr directly

    assertU(adoc(doc));
    assertU(commit());

    assertQ(req("q", "intRemove:*", "indent", "true"), "//result[@numFound = '4']");
    assertQ(req("q", "intRemove:111", "indent", "true"), "//result[@numFound = '3']");
  }

  @Test
  public void testRemoveIntegerUsingStringType() throws Exception {
    SolrInputDocument doc;

    doc = new SolrInputDocument();
    doc.setField("id", "1001");
    doc.setField("intRemove", new String[]{"111", "222", "333", "333", "444"});
    assertU(adoc(doc));

    doc = new SolrInputDocument();
    doc.setField("id", "1002");
    doc.setField("intRemove", new String[]{"111", "222", "222", "333", "444"});
    assertU(adoc(doc));

    doc = new SolrInputDocument();
    doc.setField("id", "1020");
    doc.setField("intRemove", new String[]{"111", "333", "444"});
    assertU(adoc(doc));

    doc = new SolrInputDocument();
    doc.setField("id", "1021");
    doc.setField("intRemove", new String[]{"111", "222", "444"});
    assertU(adoc(doc));
    assertU(commit());

    assertQ(req("q", "intRemove:*", "indent", "true"), "//result[@numFound = '4']");
    assertQ(req("q", "intRemove:222", "indent", "true"), "//result[@numFound = '3']");


    doc = new SolrInputDocument();
    doc.setField("id", "1001");
    List<String> removeList = new ArrayList<String>();
    removeList.add("222");
    removeList.add("333");
    doc.setField("intRemove", ImmutableMap.of("remove", removeList)); //behavior when hitting Solr through ZK
    assertU(adoc(doc));
    assertU(commit());

    assertQ(req("q", "intRemove:*", "indent", "true"), "//result[@numFound = '4']");
    assertQ(req("q", "intRemove:222", "indent", "true"), "//result[@numFound = '2']");

    doc = new SolrInputDocument();
    doc.setField("id", "1021");
    removeList = new ArrayList<String>();
    removeList.add("222");
    removeList.add("333");
    doc.setField("intRemove", ImmutableMap.of("remove", removeList)); //behavior when hitting Solr through ZK
    assertU(adoc(doc));
    assertU(commit());

    assertQ(req("q", "intRemove:*", "indent", "true"), "//result[@numFound = '4']");
    assertQ(req("q", "intRemove:222", "indent", "true"), "//result[@numFound = '1']");

    doc = new SolrInputDocument();
    doc.setField("id", "1001");
    doc.setField("intRemove", ImmutableMap.of("remove", "111")); //behavior when hitting Solr directly

    assertU(adoc(doc));
    assertU(commit());

    assertQ(req("q", "intRemove:*", "indent", "true"), "//result[@numFound = '4']");
    assertQ(req("q", "intRemove:111", "indent", "true"), "//result[@numFound = '3']");
  }

  @Test
  public void testRemoveIntegerUsingLongType() throws Exception {
    SolrInputDocument doc;

    doc = new SolrInputDocument();
    doc.setField("id", "1001");
    doc.setField("intRemove", new Long[]{111L, 222L, 333L, 333L, 444L});
    assertU(adoc(doc));

    doc = new SolrInputDocument();
    doc.setField("id", "1002");
    doc.setField("intRemove", new Long[]{111L, 222L, 222L, 333L, 444L});
    assertU(adoc(doc));

    doc = new SolrInputDocument();
    doc.setField("id", "1020");
    doc.setField("intRemove", new Long[]{111L, 333L, 444L});
    assertU(adoc(doc));

    doc = new SolrInputDocument();
    doc.setField("id", "1021");
    doc.setField("intRemove", new Long[]{111L, 222L, 444L});
    assertU(adoc(doc));

    assertU(commit());

    assertQ(req("q", "intRemove:*", "indent", "true"), "//result[@numFound = '4']");
    assertQ(req("q", "intRemove:222", "indent", "true"), "//result[@numFound = '3']");

    doc = new SolrInputDocument();
    doc.setField("id", "1001");
    List<Long> removeList = new ArrayList<Long>();
    removeList.add(222L);
    removeList.add(333L);
    doc.setField("intRemove", ImmutableMap.of("remove", removeList)); //behavior when hitting Solr through ZK
    assertU(adoc(doc));
    assertU(commit());

    assertQ(req("q", "intRemove:*", "indent", "true"), "//result[@numFound = '4']");
    assertQ(req("q", "intRemove:222", "indent", "true"), "//result[@numFound = '2']");

    doc = new SolrInputDocument();
    doc.setField("id", "1021");
    removeList = new ArrayList<Long>();
    removeList.add(222L);
    removeList.add(333L);
    doc.setField("intRemove", ImmutableMap.of("remove", removeList)); //behavior when hitting Solr through ZK
    assertU(adoc(doc));
    assertU(commit());

    assertQ(req("q", "intRemove:*", "indent", "true"), "//result[@numFound = '4']");
    assertQ(req("q", "intRemove:222", "indent", "true"), "//result[@numFound = '1']");

    doc = new SolrInputDocument();
    doc.setField("id", "1001");
    doc.setField("intRemove", ImmutableMap.of("remove", 111L)); //behavior when hitting Solr directly

    assertU(adoc(doc));
    assertU(commit());

    assertQ(req("q", "intRemove:*", "indent", "true"), "//result[@numFound = '4']");
    assertQ(req("q", "intRemove:111", "indent", "true"), "//result[@numFound = '3']");
  }


  @Test
  public void testRemoveIntegerUsingFloatType() throws Exception {
    SolrInputDocument doc;

    doc = new SolrInputDocument();
//    add with float in integer field
//    doc.setField("id", "1001");
//    doc.setField("intRemove", new Float[]{111.10F, 222.20F, 333.30F, 333.30F, 444.40F});
//    assertU(adoc(doc));
//
//    doc = new SolrInputDocument();
//    doc.setField("id", "1002");
//    doc.setField("intRemove", new Float[]{111.10F, 222.20F, 222.20F, 333.30F, 444.40F});
//    assertU(adoc(doc));
//
//    doc = new SolrInputDocument();
//    doc.setField("id", "1020");
//    doc.setField("intRemove", new Float[]{111.10F, 333.30F, 444.40F});
//    assertU(adoc(doc));
//
//    doc = new SolrInputDocument();
//    doc.setField("id", "1021");
//    doc.setField("intRemove", new Float[]{111.10F, 222.20F, 444.40F});

    doc.setField("id", "1001");
    doc.setField("intRemove", new String[]{"111", "222", "333", "333", "444"});
    assertU(adoc(doc));

    doc = new SolrInputDocument();
    doc.setField("id", "1002");
    doc.setField("intRemove", new String[]{"111", "222", "222", "333", "444"});
    assertU(adoc(doc));

    doc = new SolrInputDocument();
    doc.setField("id", "1020");
    doc.setField("intRemove", new String[]{"111", "333", "444"});
    assertU(adoc(doc));

    doc = new SolrInputDocument();
    doc.setField("id", "1021");
    doc.setField("intRemove", new String[]{"111", "222", "444"});    
    assertU(adoc(doc));

    assertU(commit());

    assertQ(req("q", "intRemove:*", "indent", "true"), "//result[@numFound = '4']");
    assertQ(req("q", "intRemove:222", "indent", "true"), "//result[@numFound = '3']");


    doc = new SolrInputDocument();
    doc.setField("id", "1001");
    List<Float> removeList = new ArrayList<Float>();
    removeList.add(222.20F);
    removeList.add(333.30F);
    doc.setField("intRemove", ImmutableMap.of("remove", removeList)); //behavior when hitting Solr through ZK
    assertU(adoc(doc));
    assertU(commit());

    assertQ(req("q", "intRemove:*", "indent", "true"), "//result[@numFound = '4']");
    assertQ(req("q", "intRemove:222", "indent", "true"), "//result[@numFound = '2']");

    doc = new SolrInputDocument();
    doc.setField("id", "1021");
    removeList = new ArrayList<Float>();
    removeList.add(222.20F);
    removeList.add(333.30F);
    doc.setField("intRemove", ImmutableMap.of("remove", removeList)); //behavior when hitting Solr through ZK
    assertU(adoc(doc));
    assertU(commit());

    assertQ(req("q", "intRemove:*", "indent", "true"), "//result[@numFound = '4']");
    assertQ(req("q", "intRemove:222", "indent", "true"), "//result[@numFound = '1']");

    doc = new SolrInputDocument();
    doc.setField("id", "1001");
    doc.setField("intRemove", ImmutableMap.of("remove", 111L)); //behavior when hitting Solr directly

    assertU(adoc(doc));
    assertU(commit());

    assertQ(req("q", "intRemove:*", "indent", "true"), "//result[@numFound = '4']");
    assertQ(req("q", "intRemove:111", "indent", "true"), "//result[@numFound = '3']");
  }
  

  @Test
  public void testRemoveIntegerUsingDoubleType() throws Exception {
    SolrInputDocument doc;

    doc = new SolrInputDocument();
    doc.setField("id", "1001");
    doc.setField("intRemove", new String[]{"11111111", "22222222", "33333333", "33333333", "44444444"});
    assertU(adoc(doc));

    doc = new SolrInputDocument();
    doc.setField("id", "1002");
    doc.setField("intRemove", new String[]{"11111111", "22222222", "22222222", "33333333", "44444444"});
    assertU(adoc(doc));

    doc = new SolrInputDocument();
    doc.setField("id", "1020");
    doc.setField("intRemove", new String[]{"11111111", "33333333", "44444444"});
    assertU(adoc(doc));

    doc = new SolrInputDocument();
    doc.setField("id", "1021");
    doc.setField("intRemove", new String[]{"11111111", "22222222", "44444444"});
    assertU(adoc(doc));

    assertU(commit());

    assertQ(req("q", "intRemove:*", "indent", "true"), "//result[@numFound = '4']");
    assertQ(req("q", "intRemove:22222222", "indent", "true"), "//result[@numFound = '3']");


    doc = new SolrInputDocument();
    doc.setField("id", "1001");
    List<Double> removeList = new ArrayList<Double>();
    removeList.add(22222222D);
    removeList.add(33333333D);
    doc.setField("intRemove", ImmutableMap.of("remove", removeList)); //behavior when hitting Solr through ZK
    assertU(adoc(doc));
    assertU(commit());

    assertQ(req("q", "intRemove:*", "indent", "true"), "//result[@numFound = '4']");
    assertQ(req("q", "intRemove:22222222", "indent", "true"), "//result[@numFound = '2']");

    doc = new SolrInputDocument();
    doc.setField("id", "1021");
    removeList = new ArrayList<Double>();
    removeList.add(22222222D);
    removeList.add(33333333D);
    doc.setField("intRemove", ImmutableMap.of("remove", removeList)); //behavior when hitting Solr through ZK
    assertU(adoc(doc));
    assertU(commit());

    assertQ(req("q", "intRemove:*", "indent", "true"), "//result[@numFound = '4']");
    assertQ(req("q", "intRemove:22222222", "indent", "true"), "//result[@numFound = '1']");

    doc = new SolrInputDocument();
    doc.setField("id", "1001");
    doc.setField("intRemove", ImmutableMap.of("remove", 11111111D)); //behavior when hitting Solr directly

    assertU(adoc(doc));
    assertU(commit());

    assertQ(req("q", "intRemove:*", "indent", "true"), "//result[@numFound = '4']");
    assertQ(req("q", "intRemove:11111111", "indent", "true"), "//result[@numFound = '3']");
  }
  
  @Test
  public void testRemoveDateUsingStringType() throws Exception {
    SolrInputDocument doc;

    doc = new SolrInputDocument();
    doc.setField("id", "10001");
    doc.setField("dateRemove", new String[]{"2014-09-01T12:00:00Z", "2014-09-02T12:00:00Z", "2014-09-03T12:00:00Z", "2014-09-03T12:00:00Z", "2014-09-04T12:00:00Z"});
    assertU(adoc(doc));

    doc = new SolrInputDocument();
    doc.setField("id", "10002");
    doc.setField("dateRemove", new String[]{"2014-09-01T12:00:00Z", "2014-09-02T12:00:00Z", "2014-09-02T12:00:00Z", "2014-09-03T12:00:00Z", "2014-09-04T12:00:00Z"});
    assertU(adoc(doc));

    doc = new SolrInputDocument();
    doc.setField("id", "10020");
    doc.setField("dateRemove", new String[]{"2014-09-01T12:00:00Z", "2014-09-03T12:00:00Z", "2014-09-04T12:00:00Z"});
    assertU(adoc(doc));

    doc = new SolrInputDocument();
    doc.setField("id", "10021");
    doc.setField("dateRemove", new String[]{"2014-09-01T12:00:00Z", "2014-09-02T12:00:00Z", "2014-09-04T12:00:00Z"});
    assertU(adoc(doc));

    assertU(commit());

    assertQ(req("q", "dateRemove:*", "indent", "true"), "//result[@numFound = '4']");
    assertQ(req("q", "dateRemove:\"2014-09-02T12:00:00Z\"", "indent", "true"), "//result[@numFound = '3']");

    doc = new SolrInputDocument();
    doc.setField("id", "10001");
    List<String> removeList = new ArrayList<String>();
    removeList.add("2014-09-02T12:00:00Z");
    removeList.add("2014-09-03T12:00:00Z");

    doc.setField("dateRemove", ImmutableMap.of("remove", removeList)); //behavior when hitting Solr through ZK
    assertU(adoc(doc));
    assertU(commit());

    assertQ(req("q", "dateRemove:*", "indent", "true"), "//result[@numFound = '4']");
    assertQ(req("q", "dateRemove:\"2014-09-02T12:00:00Z\"", "indent", "true"), "//result[@numFound = '2']");

    doc = new SolrInputDocument();
    doc.setField("id", "10021");
    removeList = new ArrayList<String>();
    removeList.add("2014-09-02T12:00:00Z");
    removeList.add("2014-09-03T12:00:00Z");
    doc.setField("dateRemove", ImmutableMap.of("remove", removeList)); //behavior when hitting Solr through ZK
    assertU(adoc(doc));
    assertU(commit());

    assertQ(req("q", "dateRemove:*", "indent", "true"), "//result[@numFound = '4']");
    assertQ(req("q", "dateRemove:\"2014-09-02T12:00:00Z\"", "indent", "true"), "//result[@numFound = '1']");

    doc = new SolrInputDocument();
    doc.setField("id", "10001");
    doc.setField("dateRemove", ImmutableMap.of("remove", "2014-09-01T12:00:00Z")); //behavior when hitting Solr directly

    assertU(adoc(doc));
    assertU(commit());

    assertQ(req("q", "dateRemove:*", "indent", "true"), "//result[@numFound = '4']");
    assertQ(req("q", "dateRemove:\"2014-09-01T12:00:00Z\"", "indent", "true"), "//result[@numFound = '3']");
  }
  
  @Ignore("Remove Date is not supported in other formats than UTC")
  @Test
  public void testRemoveDateUsingDateType() throws Exception {
    SolrInputDocument doc;

    doc = new SolrInputDocument();
    doc.setField("id", "10001");
    TrieDateField trieDF = new TrieDateField();
    Date tempDate = DateFormatUtil.parseMath(null, "2014-02-01T12:00:00Z");
    doc.setField("dateRemove", new Date[]{DateFormatUtil.parseMath(null, "2014-02-01T12:00:00Z"), 
        DateFormatUtil.parseMath(null, "2014-07-02T12:00:00Z"),
        DateFormatUtil.parseMath(null, "2014-02-03T12:00:00Z"),
        DateFormatUtil.parseMath(null, "2014-02-03T12:00:00Z"),
        DateFormatUtil.parseMath(null, "2014-02-04T12:00:00Z")
        });
    assertU(adoc(doc));

    doc = new SolrInputDocument();
    doc.setField("id", "10002");
    doc.setField("dateRemove", new Date[]{DateFormatUtil.parseMath(null, "2014-02-01T12:00:00Z"), 
        DateFormatUtil.parseMath(null, "2014-07-02T12:00:00Z"),
        DateFormatUtil.parseMath(null, "2014-02-02T12:00:00Z"),
        DateFormatUtil.parseMath(null, "2014-02-03T12:00:00Z"),
        DateFormatUtil.parseMath(null, "2014-02-04T12:00:00Z")
        });
    assertU(adoc(doc));

    doc = new SolrInputDocument();
    doc.setField("id", "10020");
    doc.setField("dateRemove", new Date[]{DateFormatUtil.parseMath(null, "2014-02-01T12:00:00Z"), 
        DateFormatUtil.parseMath(null, "2014-02-03T12:00:00Z"),
        DateFormatUtil.parseMath(null, "2014-02-04T12:00:00Z")
        });
    assertU(adoc(doc));

    doc = new SolrInputDocument();
    doc.setField("id", "10021");
    doc.setField("dateRemove", new Date[]{DateFormatUtil.parseMath(null, "2014-02-01T12:00:00Z"), 
        DateFormatUtil.parseMath(null, "2014-02-02T12:00:00Z"),
        DateFormatUtil.parseMath(null, "2014-02-04T12:00:00Z")
        });
    assertU(adoc(doc));

    assertU(commit());

    assertQ(req("q", "dateRemove:*", "indent", "true"), "//result[@numFound = '4']");
    String dateString = DateFormatUtil.parseMath(null, "2014-02-02T12:00:00Z").toString();
//    assertQ(req("q", "dateRemove:"+URLEncoder.encode(dateString, "UTF-8"), "indent", "true"), "//result[@numFound = '3']");
//    assertQ(req("q", "dateRemove:\"2014-09-02T12:00:00Z\"", "indent", "true"), "//result[@numFound = '3']");
//    assertQ(req("q", "dateRemove:"+dateString, "indent", "true"), "//result[@numFound = '3']"); //Sun Feb 02 10:00:00 FNT 2014
    assertQ(req("q", "dateRemove:\"Sun Feb 02 10:00:00 FNT 2014\"", "indent", "true"), "//result[@numFound = '3']"); //Sun Feb 02 10:00:00 FNT 2014


    doc = new SolrInputDocument();
    doc.setField("id", "10001");
    List<Date> removeList = new ArrayList<Date>();
    removeList.add(DateFormatUtil.parseMath(null, "2014-09-02T12:00:00Z"));
    removeList.add(DateFormatUtil.parseMath(null, "2014-09-03T12:00:00Z"));

    doc.setField("dateRemove", ImmutableMap.of("remove", removeList)); //behavior when hitting Solr through ZK
    assertU(adoc(doc));
    assertU(commit());

    assertQ(req("q", "dateRemove:*", "indent", "true"), "//result[@numFound = '4']");
    assertQ(req("q", "dateRemove:\"2014-09-02T12:00:00Z\"", "indent", "true"), "//result[@numFound = '2']");

    doc = new SolrInputDocument();
    doc.setField("id", "10021");
    removeList = new ArrayList<Date>();
    removeList.add(DateFormatUtil.parseMath(null, "2014-09-02T12:00:00Z"));
    removeList.add(DateFormatUtil.parseMath(null, "2014-09-03T12:00:00Z"));
    doc.setField("dateRemove", ImmutableMap.of("remove", removeList)); //behavior when hitting Solr through ZK
    assertU(adoc(doc));
    assertU(commit());

    assertQ(req("q", "dateRemove:*", "indent", "true"), "//result[@numFound = '4']");
    assertQ(req("q", "dateRemove:\"2014-09-02T12:00:00Z\"", "indent", "true"), "//result[@numFound = '1']");

    doc = new SolrInputDocument();
    doc.setField("id", "10001");
    doc.setField("dateRemove", ImmutableMap.of("remove", DateFormatUtil.parseMath(null, "2014-09-01T12:00:00Z"))); //behavior when hitting Solr directly

    assertU(adoc(doc));
    assertU(commit());

    assertQ(req("q", "dateRemove:*", "indent", "true"), "//result[@numFound = '4']");
    assertQ(req("q", "dateRemove:\"2014-09-01T12:00:00Z\"", "indent", "true"), "//result[@numFound = '3']");
  }
 
  @Test
  public void testRemoveFloatUsingFloatType() throws Exception {
    SolrInputDocument doc;

    doc = new SolrInputDocument();
    doc.setField("id", "10001");
    doc.setField("floatRemove", new Float[]{111.111F, 222.222F, 333.333F, 333.333F, 444.444F});

    assertU(adoc(doc));

    doc = new SolrInputDocument();
    doc.setField("id", "10002");
    doc.setField("floatRemove", new Float[]{111.111F, 222.222F, 222.222F, 333.333F, 444.444F});
    assertU(adoc(doc));


    doc = new SolrInputDocument();
    doc.setField("id", "10020");
    doc.setField("floatRemove", new Float[]{111.111F, 333.333F, 444.444F});
    assertU(adoc(doc));

    doc = new SolrInputDocument();
    doc.setField("id", "10021");
    doc.setField("floatRemove", new Float[]{111.111F, 222.222F, 444.444F});
    assertU(adoc(doc));

    assertU(commit());

    assertQ(req("q", "floatRemove:*", "indent", "true"), "//result[@numFound = '4']");
    assertQ(req("q", "floatRemove:\"222.222\"", "indent", "true"), "//result[@numFound = '3']");


    doc = new SolrInputDocument();
    doc.setField("id", "10001");
    List<Float> removeList = new ArrayList<Float>();
    removeList.add(222.222F);
    removeList.add(333.333F);

    doc.setField("floatRemove", ImmutableMap.of("remove", removeList)); //behavior when hitting Solr through ZK
    assertU(adoc(doc));
    assertU(commit());

    assertQ(req("q", "floatRemove:*", "indent", "true"), "//result[@numFound = '4']");
    assertQ(req("q", "floatRemove:\"222.222\"", "indent", "true"), "//result[@numFound = '2']");

    doc = new SolrInputDocument();
    doc.setField("id", "10021");
    removeList = new ArrayList<Float>();
    removeList.add(222.222F);
    removeList.add(333.333F);
    doc.setField("floatRemove", ImmutableMap.of("remove", removeList)); //behavior when hitting Solr through ZK
    assertU(adoc(doc));
    assertU(commit());

    assertQ(req("q", "floatRemove:*", "indent", "true"), "//result[@numFound = '4']");
    assertQ(req("q", "floatRemove:\"222.222\"", "indent", "true"), "//result[@numFound = '1']");

    doc = new SolrInputDocument();
    doc.setField("id", "10001");
    doc.setField("floatRemove", ImmutableMap.of("remove", "111.111")); //behavior when hitting Solr directly

    assertU(adoc(doc));
    assertU(commit());

    assertQ(req("q", "floatRemove:*", "indent", "true"), "//result[@numFound = '4']");
    assertQ(req("q", "floatRemove:\"111.111\"", "indent", "true"), "//result[@numFound = '3']");
  }
  
  @Test
  public void testRemoveFloatUsingStringType() throws Exception {
    SolrInputDocument doc;

    doc = new SolrInputDocument();
    doc.setField("id", "10001");
    doc.setField("floatRemove", new String[]{"111.111", "222.222", "333.333", "333.333", "444.444"});

    assertU(adoc(doc));

    doc = new SolrInputDocument();
    doc.setField("id", "10002");
    doc.setField("floatRemove", new String[]{"111.111", "222.222", "222.222", "333.333", "444.444"});
    assertU(adoc(doc));


    doc = new SolrInputDocument();
    doc.setField("id", "10020");
    doc.setField("floatRemove", new String[]{"111.111", "333.333", "444.444"});
    assertU(adoc(doc));

    doc = new SolrInputDocument();
    doc.setField("id", "10021");
    doc.setField("floatRemove", new String[]{"111.111", "222.222", "444.444"});
    assertU(adoc(doc));

    assertU(commit());

    assertQ(req("q", "floatRemove:*", "indent", "true"), "//result[@numFound = '4']");
    assertQ(req("q", "floatRemove:\"222.222\"", "indent", "true"), "//result[@numFound = '3']");


    doc = new SolrInputDocument();
    doc.setField("id", "10001");
    List<String> removeList = new ArrayList<String>();
    removeList.add("222.222");
    removeList.add("333.333");

    doc.setField("floatRemove", ImmutableMap.of("remove", removeList)); //behavior when hitting Solr through ZK
    assertU(adoc(doc));
    assertU(commit());

    assertQ(req("q", "floatRemove:*", "indent", "true"), "//result[@numFound = '4']");
    assertQ(req("q", "floatRemove:\"222.222\"", "indent", "true"), "//result[@numFound = '2']");

    doc = new SolrInputDocument();
    doc.setField("id", "10021");
    removeList = new ArrayList<String>();
    removeList.add("222.222");
    removeList.add("333.333");
    doc.setField("floatRemove", ImmutableMap.of("remove", removeList)); //behavior when hitting Solr through ZK
    assertU(adoc(doc));
    assertU(commit());

    assertQ(req("q", "floatRemove:*", "indent", "true"), "//result[@numFound = '4']");
    assertQ(req("q", "floatRemove:\"222.222\"", "indent", "true"), "//result[@numFound = '1']");

    doc = new SolrInputDocument();
    doc.setField("id", "10001");
    doc.setField("floatRemove", ImmutableMap.of("remove", "111.111")); //behavior when hitting Solr directly

    assertU(adoc(doc));
    assertU(commit());

    assertQ(req("q", "floatRemove:*", "indent", "true"), "//result[@numFound = '4']");
    assertQ(req("q", "floatRemove:\"111.111\"", "indent", "true"), "//result[@numFound = '3']");
  }

 @Test
  public void testRemoveregex() throws Exception {
    SolrInputDocument doc;

    doc = new SolrInputDocument();
    doc.setField("id", "1");
    doc.setField("cat", new String[]{"aaa", "bbb", "ccc", "ccc", "ddd"});
    assertU(adoc(doc));

    doc = new SolrInputDocument();
    doc.setField("id", "2");
    doc.setField("cat", new String[]{"aaa", "bbb", "bbb", "ccc", "ddd"});
    assertU(adoc(doc));


    doc = new SolrInputDocument();
    doc.setField("id", "20");
    doc.setField("cat", new String[]{"aaa", "ccc", "ddd"});
    assertU(adoc(doc));

    doc = new SolrInputDocument();
    doc.setField("id", "21");
    doc.setField("cat", new String[]{"aaa", "bbb", "ddd"});
    assertU(adoc(doc));


    assertU(commit());

    assertQ(req("q", "cat:*", "indent", "true"), "//result[@numFound = '4']");
    assertQ(req("q", "cat:bbb", "indent", "true"), "//result[@numFound = '3']");


    doc = new SolrInputDocument();
    doc.setField("id", "1");
    List<String> removeList = new ArrayList<>();
    removeList.add(".b.");
    removeList.add("c+c");
    doc.setField("cat", ImmutableMap.of("removeregex", removeList)); //behavior when hitting Solr through ZK
    assertU(adoc(doc));
    assertU(commit());

    assertQ(req("q", "cat:*", "indent", "true"), "//result[@numFound = '4']");
    assertQ(req("q", "cat:bbb", "indent", "true"), "//result[@numFound = '2']");

    doc = new SolrInputDocument();
    doc.setField("id", "21");
    removeList = new ArrayList<>();
    removeList.add("bb*");
    removeList.add("cc+");
    doc.setField("cat", ImmutableMap.of("removeregex", removeList)); //behavior when hitting Solr through ZK
    assertU(adoc(doc));
    assertU(commit());

    assertQ(req("q", "cat:*", "indent", "true"), "//result[@numFound = '4']");
    assertQ(req("q", "cat:bbb", "indent", "true"), "//result[@numFound = '1']");

    doc = new SolrInputDocument();
    doc.setField("id", "1");
    doc.setField("cat", ImmutableMap.of("removeregex", "a.a")); //behavior when hitting Solr directly

    assertU(adoc(doc));
    assertU(commit());

    assertQ(req("q", "cat:*", "indent", "true"), "//result[@numFound = '4']");
    assertQ(req("q", "cat:aaa", "indent", "true"), "//result[@numFound = '3']");
  }

  @Test
  public void testAdd() throws Exception {
    SolrInputDocument doc = new SolrInputDocument();
    doc.setField("id", "3");
    doc.setField("cat", new String[]{"aaa", "ccc"});
    assertU(adoc(doc));

    doc = new SolrInputDocument();
    doc.setField("id", "4");
    doc.setField("cat", new String[]{"aaa", "ccc"});
    assertU(adoc(doc));

    assertU(commit());

    assertQ(req("q", "cat:*", "indent", "true"), "//result[@numFound = '2']");
    assertQ(req("q", "cat:bbb", "indent", "true"), "//result[@numFound = '0']");


    doc = new SolrInputDocument();
    doc.setField("id", "3");
    doc.setField("cat", ImmutableMap.of("add", "bbb"));
    assertU(adoc(doc));
    assertU(commit());

    assertQ(req("q", "cat:*", "indent", "true"), "//result[@numFound = '2']");
    assertQ(req("q", "cat:bbb", "indent", "true"), "//result[@numFound = '1']");
  }

  @Test
  public void testSet() throws Exception {
    SolrInputDocument doc;

    doc = new SolrInputDocument();
    doc.setField("id", "5");
    doc.setField("cat", new String[]{"aaa", "ccc"});
    assertU(adoc(doc));

    doc = new SolrInputDocument();
    doc.setField("id", "6");
    doc.setField("cat", new String[]{"aaa", "ccc"});
    assertU(adoc(doc));

    assertU(commit());

    assertQ(req("q", "cat:*", "indent", "true"), "//result[@numFound = '2']");
    assertQ(req("q", "cat:aaa", "indent", "true"), "//result[@numFound = '2']");
    assertQ(req("q", "cat:bbb", "indent", "true"), "//result[@numFound = '0']");
    assertQ(req("q", "cat:ccc", "indent", "true"), "//result[@numFound = '2']");


    doc = new SolrInputDocument();
    doc.setField("id", "5");
    doc.setField("cat", ImmutableMap.of("set", "bbb"));
    assertU(adoc(doc));
    assertU(commit());

    assertQ(req("q", "cat:*", "indent", "true"), "//result[@numFound = '2']");
    assertQ(req("q", "cat:aaa", "indent", "true"), "//result[@numFound = '1']");
    assertQ(req("q", "cat:bbb", "indent", "true"), "//result[@numFound = '1']");
    assertQ(req("q", "cat:ccc", "indent", "true"), "//result[@numFound = '1']");
  }

  public void testAtomicUpdatesOnDateFields() throws Exception {
    String[] dateFieldNames = {"simple_tdt1", "simple_tdts", "simple_tdtdv1", "simple_tdtdvs"};

    SolrInputDocument doc = new SolrInputDocument();
    doc.setField("id", "6");
    // Even adding a single value here for the multiValue field causes the update later on to fail
    doc.setField("simple_tdt1", "1986-01-01T00:00:00Z"); // single-valued
    doc.setField("simple_tdts", new String[] {"1986-01-01T00:00:00Z"});
    doc.setField("simple_tdtdv", "1986-01-01T00:00:00Z");
    doc.setField("simple_tdtdvs", new String[] {"1986-01-01T00:00:00Z"});
    // An independent field that we want to update later on
    doc.setField("other_i", "42");
    assertU(adoc(doc));
    assertU(commit());

    assertQ(req("q", "id:6"), "boolean(//result/doc/date[@name='simple_tdt1'])");


    for (String dateFieldName : dateFieldNames) {
      // none (this can fail with Invalid Date String exception)
      doc = new SolrInputDocument();
      doc.setField("id", "6");
      doc.setField("other_i", ImmutableMap.of("set", "43")); // set the independent field to another value
      assertU(adoc(doc));

      if (dateFieldName.endsWith("s"))  {
        // add
        doc = new SolrInputDocument();
        doc.setField("id", "6");
        doc.setField("other_i", ImmutableMap.of("set", "43")); // set the independent field to another value
        doc.setField(dateFieldName, ImmutableMap.of("add", "1987-01-01T00:00:00Z"));
        assertU(adoc(doc));

        // remove
        doc = new SolrInputDocument();
        doc.setField("id", "6");
        doc.setField("other_i", ImmutableMap.of("set", "43")); // set the independent field to another value
        doc.setField(dateFieldName, ImmutableMap.of("remove", "1987-01-01T00:00:00Z"));
        assertU(adoc(doc));
      } else {
        // set
        doc = new SolrInputDocument();
        doc.setField("id", "6");
        doc.setField("other_i", ImmutableMap.of("set", "43")); // set the independent field to another value
        doc.setField(dateFieldName, ImmutableMap.of("set", "1987-01-01T00:00:00Z"));
        assertU(adoc(doc));

        // unset
        doc = new SolrInputDocument();
        doc.setField("id", "6");
        doc.setField("other_i", ImmutableMap.of("set", "43")); // set the independent field to another value
        doc.setField(dateFieldName, map("set", null));
        assertU(adoc(doc));
      }

      assertU(commit());
      if (dateFieldName.endsWith("s"))  {
        assertQ(req("q", "id:6"), "//result/doc[count(arr[@name='" + dateFieldName + "'])=1]");
        assertQ(req("q", "id:6"), "//result/doc/arr[@name='" + dateFieldName + "'][count(date)=1]");
      } else {
        assertQ(req("q", "id:6"), "//result/doc[count(date[@name='" + dateFieldName + "'])=0]");
      }
    }

  }

  @Test
  public void testAtomicUpdatesOnNonStoredDocValues() throws Exception {
    assertU(adoc(sdoc("id", 2, "title", "title2", "single_i_dvo", 100)));
    assertU(adoc(sdoc("id", 3, "title", "title3", "single_d_dvo", 3.14)));
    assertU(adoc(sdoc("id", 4, "single_s_dvo", "abc", "single_i_dvo", 1)));
    assertU(commit());

    assertU(adoc(sdoc("id", 2, "title", ImmutableMap.of("set", "newtitle2"),
        "single_i_dvo", ImmutableMap.of("inc", 1))));
    assertU(adoc(sdoc("id", 3, "title", ImmutableMap.of("set", "newtitle3"),
        "single_d_dvo", ImmutableMap.of("inc", 1))));
    assertU(adoc(sdoc("id", 4, "single_i_dvo", ImmutableMap.of("inc", 1))));
    assertU(commit());

    assertJQ(req("q", "id:2"),
        "/response/docs/[0]/id==2",
        "/response/docs/[0]/title/[0]=='newtitle2'",
        "/response/docs/[0]/single_i_dvo==101");

    assertJQ(req("q", "id:3"),
        1e-4,
        "/response/docs/[0]/id==3",
        "/response/docs/[0]/title/[0]=='newtitle3'",
        "/response/docs/[0]/single_d_dvo==4.14");

    assertJQ(req("q", "id:4"),
        1e-4,
        "/response/docs/[0]/id==4",
        "/response/docs/[0]/single_s_dvo=='abc'",
        "/response/docs/[0]/single_i_dvo==2");

    // test that non stored docvalues was carried forward for a non-docvalue update
    assertU(adoc(sdoc("id", 3, "title", ImmutableMap.of("set", "newertitle3"))));
    assertU(commit());
    assertJQ(req("q", "id:3"),
        1e-4,
        "/response/docs/[0]/id==3",
        "/response/docs/[0]/title/[0]=='newertitle3'",
        "/response/docs/[0]/single_d_dvo==4.14");
  }

  @Test
  public void testAtomicUpdatesOnNonStoredDocValuesMulti() throws Exception {
    assertU(adoc(sdoc("id", 1, "title", "title1", "multi_ii_dvo", 100, "multi_ii_dvo", Integer.MAX_VALUE)));
    assertU(commit());

    assertU(adoc(sdoc("id", 1, "title", ImmutableMap.of("set", "newtitle1"))));
    assertU(commit());

    // test that non stored multivalued docvalues was carried forward for a non docvalues update
    assertJQ(req("q", "id:1"),
        "/response/docs/[0]/id==1",
        "/response/docs/[0]/title/[0]=='newtitle1'",
        "/response/docs/[0]/multi_ii_dvo/[0]==100",
        "/response/docs/[0]/multi_ii_dvo/[1]==" + Integer.MAX_VALUE);
  }

  @Test
  public void testInvalidOperation() {
    SolrInputDocument doc;

    doc = new SolrInputDocument();
    doc.setField("id", "7");
    doc.setField("cat", new String[]{"aaa", "ccc"});
    assertU(adoc(doc));

    assertU(commit());

    assertQ(req("q", "cat:*", "indent", "true"), "//result[@numFound = '1']");

    doc = new SolrInputDocument();
    doc.setField("id", "7");
    doc.setField("cat", ImmutableMap.of("whatever", "bbb"));
    assertU(adoc(doc));
    assertU(commit());

    assertQ(req("q", "cat:*", "indent", "true"), "//result[@numFound = '1']");
    assertQ(req("q", "cat:aaa", "indent", "true"), "//result[@numFound = '1']");
    assertQ(req("q", "cat:bbb", "indent", "true"), "//result[@numFound = '0']");
    assertQ(req("q", "cat:ccc", "indent", "true"), "//result[@numFound = '1']");

  }
}

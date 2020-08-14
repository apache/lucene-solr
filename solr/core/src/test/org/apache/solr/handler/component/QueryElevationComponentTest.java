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

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.lang.invoke.MethodHandles;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

import com.carrotsearch.randomizedtesting.rules.SystemPropertiesRestoreRule;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.util.BytesRef;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.GroupParams;
import org.apache.solr.common.params.QueryElevationParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.util.FileUtils;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueryElevationComponentTest extends SolrTestCaseJ4 {

  @Rule
  public TestRule solrTestRules = RuleChain.outerRule(new SystemPropertiesRestoreRule());


  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @BeforeClass
  public static void beforeClass() {
    switch (random().nextInt(3)) {
      case 0:
        System.setProperty("solr.tests.id.stored", "true");
        System.setProperty("solr.tests.id.docValues", "true");
        break;
      case 1:
        System.setProperty("solr.tests.id.stored", "true");
        System.setProperty("solr.tests.id.docValues", "false");
        break;
      case 2:
        System.setProperty("solr.tests.id.stored", "false");
        System.setProperty("solr.tests.id.docValues", "true");
        break;
      default:
        fail("Bad random number generated not between 0-2 inclusive");
        break;
    }
  }

  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
  }

  private void init(String schema) throws Exception {
    init("solrconfig-elevate.xml", schema);
  }

  private void init(String config, String schema) throws Exception {
    //write out elevate-data.xml to the Data dir first by copying it from conf, which we know exists, this way we can test both conf and data configurations
    File parent = new File(TEST_HOME() + "/collection1", "conf");
    File elevateFile = new File(parent, "elevate.xml");
    File elevateDataFile = new File(initAndGetDataDir(), "elevate-data.xml");
    FileUtils.copyFile(elevateFile, elevateDataFile);


    initCore(config,schema);
    clearIndex();
    assertU(commit());
  }

  //TODO should be @After ?
  private void delete() {
    deleteCore();
  }

  @Test
  public void testFieldType() throws Exception {
    try {
      init("schema11.xml");
      clearIndex();
      assertU(commit());
      assertU(adoc("id", "1", "text", "XXXX XXXX", "str_s", "a"));
      assertU(adoc("id", "2", "text", "YYYY", "str_s", "b"));
      assertU(adoc("id", "3", "text", "ZZZZ", "str_s", "c"));

      assertU(adoc("id", "4", "text", "XXXX XXXX", "str_s", "x"));
      assertU(adoc("id", "5", "text", "YYYY YYYY", "str_s", "y"));
      assertU(adoc("id", "6", "text", "XXXX XXXX", "str_s", "z"));
      assertU(adoc("id", "7", "text", "AAAA", "str_s", "a"));
      assertU(adoc("id", "8", "text", "AAAA", "str_s", "a"));
      assertU(adoc("id", "9", "text", "AAAA AAAA", "str_s", "a"));
      assertU(commit());

      assertQ("", req(CommonParams.Q, "AAAA", CommonParams.QT, "/elevate",
          CommonParams.FL, "id, score, [elevated]")
          , "//*[@numFound='3']"
          , "//result/doc[1]/str[@name='id'][.='7']"
          , "//result/doc[2]/str[@name='id'][.='9']"
          , "//result/doc[3]/str[@name='id'][.='8']",
          "//result/doc[1]/bool[@name='[elevated]'][.='true']",
          "//result/doc[2]/bool[@name='[elevated]'][.='false']",
          "//result/doc[3]/bool[@name='[elevated]'][.='false']"
      );
    } finally {
      delete();
    }
  }

  @Test
  public void testGroupedQuery() throws Exception {
    try {
      init("schema11.xml");
      clearIndex();
      assertU(commit());
      assertU(adoc("id", "1", "text", "XXXX XXXX", "str_s", "a"));
      assertU(adoc("id", "2", "text", "XXXX AAAA", "str_s", "b"));
      assertU(adoc("id", "3", "text", "ZZZZ", "str_s", "c"));
      assertU(adoc("id", "4", "text", "XXXX ZZZZ", "str_s", "d"));
      assertU(adoc("id", "5", "text", "ZZZZ ZZZZ", "str_s", "e"));
      assertU(adoc("id", "6", "text", "AAAA AAAA AAAA", "str_s", "f"));
      assertU(adoc("id", "7", "text", "AAAA AAAA ZZZZ", "str_s", "g"));
      assertU(adoc("id", "8", "text", "XXXX", "str_s", "h"));
      assertU(adoc("id", "9", "text", "YYYY ZZZZ", "str_s", "i"));

      assertU(adoc("id", "22", "text", "XXXX ZZZZ AAAA", "str_s", "b"));
      assertU(adoc("id", "66", "text", "XXXX ZZZZ AAAA", "str_s", "f"));
      assertU(adoc("id", "77", "text", "XXXX ZZZZ AAAA", "str_s", "g"));

      assertU(commit());

      final String groups = "//arr[@name='groups']";

      assertQ("non-elevated group query",
              req(CommonParams.Q, "AAAA",
                  CommonParams.QT, "/elevate",
                  GroupParams.GROUP_FIELD, "str_s",
                  GroupParams.GROUP, "true",
                  GroupParams.GROUP_TOTAL_COUNT, "true",
                  GroupParams.GROUP_LIMIT, "100",
                  QueryElevationParams.ENABLE, "false",
                  CommonParams.FL, "id, score, [elevated]")
              , "//*[@name='ngroups'][.='3']"
              , "//*[@name='matches'][.='6']"

              , groups +"/lst[1]//doc[1]/str[@name='id'][.='6']"
              , groups +"/lst[1]//doc[1]/bool[@name='[elevated]'][.='false']"
              , groups +"/lst[1]//doc[2]/str[@name='id'][.='66']"
              , groups +"/lst[1]//doc[2]/bool[@name='[elevated]'][.='false']"

              , groups +"/lst[2]//doc[1]/str[@name='id'][.='7']"
              , groups +"/lst[2]//doc[1]/bool[@name='[elevated]'][.='false']"
              , groups +"/lst[2]//doc[2]/str[@name='id'][.='77']"
              , groups +"/lst[2]//doc[2]/bool[@name='[elevated]'][.='false']"

              , groups +"/lst[3]//doc[1]/str[@name='id'][.='2']"
              , groups +"/lst[3]//doc[1]/bool[@name='[elevated]'][.='false']"
              , groups +"/lst[3]//doc[2]/str[@name='id'][.='22']"
              , groups +"/lst[3]//doc[2]/bool[@name='[elevated]'][.='false']"
              );

      assertQ("elevated group query",
              req(CommonParams.Q, "AAAA",
                  CommonParams.QT, "/elevate",
                  GroupParams.GROUP_FIELD, "str_s",
                  GroupParams.GROUP, "true",
                  GroupParams.GROUP_TOTAL_COUNT, "true",
                  GroupParams.GROUP_LIMIT, "100",
                  CommonParams.FL, "id, score, [elevated]")
              , "//*[@name='ngroups'][.='3']"
              , "//*[@name='matches'][.='6']"

              , groups +"/lst[1]//doc[1]/str[@name='id'][.='7']"
              , groups +"/lst[1]//doc[1]/bool[@name='[elevated]'][.='true']"
              , groups +"/lst[1]//doc[2]/str[@name='id'][.='77']"
              , groups +"/lst[1]//doc[2]/bool[@name='[elevated]'][.='false']"

              , groups +"/lst[2]//doc[1]/str[@name='id'][.='6']"
              , groups +"/lst[2]//doc[1]/bool[@name='[elevated]'][.='false']"
              , groups +"/lst[2]//doc[2]/str[@name='id'][.='66']"
              , groups +"/lst[2]//doc[2]/bool[@name='[elevated]'][.='false']"

              , groups +"/lst[3]//doc[1]/str[@name='id'][.='2']"
              , groups +"/lst[3]//doc[1]/bool[@name='[elevated]'][.='false']"
              , groups +"/lst[3]//doc[2]/str[@name='id'][.='22']"
              , groups +"/lst[3]//doc[2]/bool[@name='[elevated]'][.='false']"
              );

      assertQ("non-elevated because sorted group query",
              req(CommonParams.Q, "AAAA",
                  CommonParams.QT, "/elevate",
                  CommonParams.SORT, "id asc",
                  GroupParams.GROUP_FIELD, "str_s",
                  GroupParams.GROUP, "true",
                  GroupParams.GROUP_TOTAL_COUNT, "true",
                  GroupParams.GROUP_LIMIT, "100",
                  CommonParams.FL, "id, score, [elevated]")
              , "//*[@name='ngroups'][.='3']"
              , "//*[@name='matches'][.='6']"

              , groups +"/lst[1]//doc[1]/str[@name='id'][.='2']"
              , groups +"/lst[1]//doc[1]/bool[@name='[elevated]'][.='false']"
              , groups +"/lst[1]//doc[2]/str[@name='id'][.='22']"
              , groups +"/lst[1]//doc[2]/bool[@name='[elevated]'][.='false']"

              , groups +"/lst[2]//doc[1]/str[@name='id'][.='6']"
              , groups +"/lst[2]//doc[1]/bool[@name='[elevated]'][.='false']"
              , groups +"/lst[2]//doc[2]/str[@name='id'][.='66']"
              , groups +"/lst[2]//doc[2]/bool[@name='[elevated]'][.='false']"

              , groups +"/lst[3]//doc[1]/str[@name='id'][.='7']"
              , groups +"/lst[3]//doc[1]/bool[@name='[elevated]'][.='true']"
              , groups +"/lst[3]//doc[2]/str[@name='id'][.='77']"
              , groups +"/lst[3]//doc[2]/bool[@name='[elevated]'][.='false']"
              );

      assertQ("force-elevated sorted group query",
              req(CommonParams.Q, "AAAA",
                  CommonParams.QT, "/elevate",
                  CommonParams.SORT, "id asc",
                  QueryElevationParams.FORCE_ELEVATION, "true",
                  GroupParams.GROUP_FIELD, "str_s",
                  GroupParams.GROUP, "true",
                  GroupParams.GROUP_TOTAL_COUNT, "true",
                  GroupParams.GROUP_LIMIT, "100",
                  CommonParams.FL, "id, score, [elevated]")
              , "//*[@name='ngroups'][.='3']"
              , "//*[@name='matches'][.='6']"

              , groups +"/lst[1]//doc[1]/str[@name='id'][.='7']"
              , groups +"/lst[1]//doc[1]/bool[@name='[elevated]'][.='true']"
              , groups +"/lst[1]//doc[2]/str[@name='id'][.='77']"
              , groups +"/lst[1]//doc[2]/bool[@name='[elevated]'][.='false']"

              , groups +"/lst[2]//doc[1]/str[@name='id'][.='2']"
              , groups +"/lst[2]//doc[1]/bool[@name='[elevated]'][.='false']"
              , groups +"/lst[2]//doc[2]/str[@name='id'][.='22']"
              , groups +"/lst[2]//doc[2]/bool[@name='[elevated]'][.='false']"

              , groups +"/lst[3]//doc[1]/str[@name='id'][.='6']"
              , groups +"/lst[3]//doc[1]/bool[@name='[elevated]'][.='false']"
              , groups +"/lst[3]//doc[2]/str[@name='id'][.='66']"
              , groups +"/lst[3]//doc[2]/bool[@name='[elevated]'][.='false']"
              );


      assertQ("non-elevated because of sort within group query",
              req(CommonParams.Q, "AAAA",
                  CommonParams.QT, "/elevate",
                  CommonParams.SORT, "id asc",
                  GroupParams.GROUP_SORT, "id desc",
                  GroupParams.GROUP_FIELD, "str_s",
                  GroupParams.GROUP, "true",
                  GroupParams.GROUP_TOTAL_COUNT, "true",
                  GroupParams.GROUP_LIMIT, "100",
                  CommonParams.FL, "id, score, [elevated]")
              , "//*[@name='ngroups'][.='3']"
              , "//*[@name='matches'][.='6']"

              , groups +"/lst[1]//doc[1]/str[@name='id'][.='22']"
              , groups +"/lst[1]//doc[1]/bool[@name='[elevated]'][.='false']"
              , groups +"/lst[1]//doc[2]/str[@name='id'][.='2']"
              , groups +"/lst[1]//doc[2]/bool[@name='[elevated]'][.='false']"

              , groups +"/lst[2]//doc[1]/str[@name='id'][.='66']"
              , groups +"/lst[2]//doc[1]/bool[@name='[elevated]'][.='false']"
              , groups +"/lst[2]//doc[2]/str[@name='id'][.='6']"
              , groups +"/lst[2]//doc[2]/bool[@name='[elevated]'][.='false']"

              , groups +"/lst[3]//doc[1]/str[@name='id'][.='77']"
              , groups +"/lst[3]//doc[1]/bool[@name='[elevated]'][.='false']"
              , groups +"/lst[3]//doc[2]/str[@name='id'][.='7']"
              , groups +"/lst[3]//doc[2]/bool[@name='[elevated]'][.='true']"
              );


      assertQ("force elevated sort within sorted group query",
              req(CommonParams.Q, "AAAA",
                  CommonParams.QT, "/elevate",
                  CommonParams.SORT, "id asc",
                  GroupParams.GROUP_SORT, "id desc",
                  QueryElevationParams.FORCE_ELEVATION, "true",
                  GroupParams.GROUP_FIELD, "str_s",
                  GroupParams.GROUP, "true",
                  GroupParams.GROUP_TOTAL_COUNT, "true",
                  GroupParams.GROUP_LIMIT, "100",
                  CommonParams.FL, "id, score, [elevated]")
              , "//*[@name='ngroups'][.='3']"
              , "//*[@name='matches'][.='6']"

              , groups +"/lst[1]//doc[1]/str[@name='id'][.='7']"
              , groups +"/lst[1]//doc[1]/bool[@name='[elevated]'][.='true']"
              , groups +"/lst[1]//doc[2]/str[@name='id'][.='77']"
              , groups +"/lst[1]//doc[2]/bool[@name='[elevated]'][.='false']"

              , groups +"/lst[2]//doc[1]/str[@name='id'][.='22']"
              , groups +"/lst[2]//doc[1]/bool[@name='[elevated]'][.='false']"
              , groups +"/lst[2]//doc[2]/str[@name='id'][.='2']"
              , groups +"/lst[2]//doc[2]/bool[@name='[elevated]'][.='false']"

              , groups +"/lst[3]//doc[1]/str[@name='id'][.='66']"
              , groups +"/lst[3]//doc[1]/bool[@name='[elevated]'][.='false']"
              , groups +"/lst[3]//doc[2]/str[@name='id'][.='6']"
              , groups +"/lst[3]//doc[2]/bool[@name='[elevated]'][.='false']"
              );

    } finally {
      delete();
    }
  }

  @Test
  public void testTrieFieldType() throws Exception {
    try {
      init("schema.xml");
      clearIndex();
      assertU(commit());
      assertU(adoc("id", "1", "text", "XXXX XXXX",           "str_s", "a" ));
      assertU(adoc("id", "2", "text", "YYYY",      "str_s", "b" ));
      assertU(adoc("id", "3", "text", "ZZZZ", "str_s", "c" ));

      assertU(adoc("id", "4", "text", "XXXX XXXX",                 "str_s", "x" ));
      assertU(adoc("id", "5", "text", "YYYY YYYY",         "str_s", "y" ));
      assertU(adoc("id", "6", "text", "XXXX XXXX", "str_s", "z" ));
      assertU(adoc("id", "7", "text", "AAAA", "str_s", "a" ));
      assertU(adoc("id", "8", "text", "AAAA", "str_s", "a" ));
      assertU(adoc("id", "9", "text", "AAAA AAAA", "str_s", "a" ));
      assertU(commit());

      assertQ("", req(CommonParams.Q, "AAAA", CommonParams.QT, "/elevate",
          CommonParams.FL, "id, score, [elevated]")
              ,"//*[@numFound='3']"
              ,"//result/doc[1]/str[@name='id'][.='7']"
              ,"//result/doc[2]/str[@name='id'][.='8']"
              ,"//result/doc[3]/str[@name='id'][.='9']",
              "//result/doc[1]/bool[@name='[elevated]'][.='true']",
              "//result/doc[2]/bool[@name='[elevated]'][.='false']",
              "//result/doc[3]/bool[@name='[elevated]'][.='false']"
              );
    } finally{
      delete();
    }
  }


  @Test
  public void testInterface() throws Exception {
    try {
      init("schema12.xml");
      SolrCore core = h.getCore();

      NamedList<String> args = new NamedList<>();
      args.add(QueryElevationComponent.FIELD_TYPE, "string");
      args.add(QueryElevationComponent.CONFIG_FILE, "elevate.xml");

      QueryElevationComponent comp = new QueryElevationComponent();
      comp.init(args);
      comp.inform(core);

      SolrQueryRequest req = req();
      IndexReader reader = req.getSearcher().getIndexReader();
      QueryElevationComponent.ElevationProvider elevationProvider = comp.getElevationProvider(reader, core);
      req.close();

      // Make sure the boosts loaded properly
      assertEquals(11, elevationProvider.size());
      assertEquals(1, elevationProvider.getElevationForQuery("XXXX").elevatedIds.size());
      assertEquals(2, elevationProvider.getElevationForQuery("YYYY").elevatedIds.size());
      assertEquals(3, elevationProvider.getElevationForQuery("ZZZZ").elevatedIds.size());
      assertNull(elevationProvider.getElevationForQuery("xxxx"));
      assertNull(elevationProvider.getElevationForQuery("yyyy"));
      assertNull(elevationProvider.getElevationForQuery("zzzz"));

      // Now test the same thing with a lowercase filter: 'lowerfilt'
      args = new NamedList<>();
      args.add(QueryElevationComponent.FIELD_TYPE, "lowerfilt");
      args.add(QueryElevationComponent.CONFIG_FILE, "elevate.xml");

      comp = new QueryElevationComponent();
      comp.init(args);
      comp.inform(core);
      elevationProvider = comp.getElevationProvider(reader, core);
      assertEquals(11, elevationProvider.size());
      assertEquals(1, elevationProvider.getElevationForQuery("XXXX").elevatedIds.size());
      assertEquals(2, elevationProvider.getElevationForQuery("YYYY").elevatedIds.size());
      assertEquals(3, elevationProvider.getElevationForQuery("ZZZZ").elevatedIds.size());
      assertEquals(1, elevationProvider.getElevationForQuery("xxxx").elevatedIds.size());
      assertEquals(2, elevationProvider.getElevationForQuery("yyyy").elevatedIds.size());
      assertEquals(3, elevationProvider.getElevationForQuery("zzzz").elevatedIds.size());

      assertEquals("xxxx", comp.analyzeQuery("XXXX"));
      assertEquals("xxxxyyyy", comp.analyzeQuery("XXXX YYYY"));

      assertQ("Make sure QEC handles null queries", req("qt", "/elevate", "q.alt", "*:*", "defType", "dismax"),
          "//*[@numFound='0']");
    } finally {
      delete();
    }

  }

  @Test
  public void testMarker() throws Exception {
    try {
      init("schema12.xml");
      assertU(adoc("id", "1", "title", "XXXX XXXX", "str_s1", "a"));
      assertU(adoc("id", "2", "title", "YYYY", "str_s1", "b"));
      assertU(adoc("id", "3", "title", "ZZZZ", "str_s1", "c"));

      assertU(adoc("id", "4", "title", "XXXX XXXX", "str_s1", "x"));
      assertU(adoc("id", "5", "title", "YYYY YYYY", "str_s1", "y"));
      assertU(adoc("id", "6", "title", "XXXX XXXX", "str_s1", "z"));
      assertU(adoc("id", "7", "title", "AAAA", "str_s1", "a"));
      assertU(commit());

      assertQ("", req(CommonParams.Q, "XXXX", CommonParams.QT, "/elevate",
          CommonParams.FL, "id, score, [elevated]")
          , "//*[@numFound='3']"
          , "//result/doc[1]/str[@name='id'][.='1']"
          , "//result/doc[2]/str[@name='id'][.='4']"
          , "//result/doc[3]/str[@name='id'][.='6']",
          "//result/doc[1]/bool[@name='[elevated]'][.='true']",
          "//result/doc[2]/bool[@name='[elevated]'][.='false']",
          "//result/doc[3]/bool[@name='[elevated]'][.='false']"
      );

      assertQ("", req(CommonParams.Q, "AAAA", CommonParams.QT, "/elevate",
          CommonParams.FL, "id, score, [elevated]")
              ,"//*[@numFound='1']"
              ,"//result/doc[1]/str[@name='id'][.='7']",
              "//result/doc[1]/bool[@name='[elevated]'][.='true']"
              );

      assertQ("", req(CommonParams.Q, "AAAA", CommonParams.QT, "/elevate",
          CommonParams.FL, "id, score, [elev]")
          , "//*[@numFound='1']"
          , "//result/doc[1]/str[@name='id'][.='7']",
          "not(//result/doc[1]/bool[@name='[elevated]'][.='false'])",
          "not(//result/doc[1]/bool[@name='[elev]'][.='false'])" // even though we asked for elev, there is no Transformer registered w/ that, so we shouldn't get a result
      );
    } finally {
      delete();
    }
  }

  @Test
  public void testMarkExcludes() throws Exception {
    try {
      init("schema12.xml");
      assertU(adoc("id", "1", "title", "XXXX XXXX", "str_s1", "a"));
      assertU(adoc("id", "2", "title", "YYYY", "str_s1", "b"));
      assertU(adoc("id", "3", "title", "ZZZZ", "str_s1", "c"));

      assertU(adoc("id", "4", "title", "XXXX XXXX", "str_s1", "x"));
      assertU(adoc("id", "5", "title", "YYYY YYYY", "str_s1", "y"));
      assertU(adoc("id", "6", "title", "XXXX XXXX", "str_s1", "z"));
      assertU(adoc("id", "7", "title", "AAAA", "str_s1", "a"));

      assertU(adoc("id", "8", "title", " QQQQ trash trash", "str_s1", "q"));
      assertU(adoc("id", "9", "title", " QQQQ QQQQ  trash", "str_s1", "r"));
      assertU(adoc("id", "10", "title", "QQQQ QQQQ  QQQQ ", "str_s1", "s"));

      assertU(commit());

      assertQ("", req(CommonParams.Q, "XXXX XXXX", CommonParams.QT, "/elevate",
          QueryElevationParams.MARK_EXCLUDES, "true",
          "indent", "true",
          CommonParams.FL, "id, score, [excluded]")
          , "//*[@numFound='4']"
          , "//result/doc[1]/str[@name='id'][.='5']"
          , "//result/doc[2]/str[@name='id'][.='1']"
          , "//result/doc[3]/str[@name='id'][.='4']"
          , "//result/doc[4]/str[@name='id'][.='6']",
          "//result/doc[1]/bool[@name='[excluded]'][.='false']",
          "//result/doc[2]/bool[@name='[excluded]'][.='false']",
          "//result/doc[3]/bool[@name='[excluded]'][.='false']",
          "//result/doc[4]/bool[@name='[excluded]'][.='true']"
      );

      //ask for excluded as a field, but don't actually request the MARK_EXCLUDES
      //thus, number 6 should not be returned, b/c it is excluded
      assertQ("", req(CommonParams.Q, "XXXX XXXX", CommonParams.QT, "/elevate",
          QueryElevationParams.MARK_EXCLUDES, "false",
          CommonParams.FL, "id, score, [excluded]")
          , "//*[@numFound='3']"
          , "//result/doc[1]/str[@name='id'][.='5']"
          , "//result/doc[2]/str[@name='id'][.='1']"
          , "//result/doc[3]/str[@name='id'][.='4']",
          "//result/doc[1]/bool[@name='[excluded]'][.='false']",
          "//result/doc[2]/bool[@name='[excluded]'][.='false']",
          "//result/doc[3]/bool[@name='[excluded]'][.='false']"
      );

      // test that excluded results are on the same positions in the result list
      // as when elevation component is disabled
      // (i.e. test that elevation component with MARK_EXCLUDES does not boost
      // excluded results)
      assertQ("", req(CommonParams.Q, "QQQQ", CommonParams.QT, "/elevate",
          QueryElevationParams.ENABLE, "false",
          "indent", "true",
          CommonParams.FL, "id, score")
          , "//*[@numFound='3']"
          , "//result/doc[1]/str[@name='id'][.='10']"
          , "//result/doc[2]/str[@name='id'][.='9']"
          , "//result/doc[3]/str[@name='id'][.='8']"
      );
      assertQ("", req(CommonParams.Q, "QQQQ", CommonParams.QT, "/elevate",
          QueryElevationParams.MARK_EXCLUDES, "true",
          "indent", "true",
          CommonParams.FL, "id, score, [excluded]")
          , "//*[@numFound='3']"
          , "//result/doc[1]/str[@name='id'][.='10']"
          , "//result/doc[2]/str[@name='id'][.='9']"
          , "//result/doc[3]/str[@name='id'][.='8']",
          "//result/doc[1]/bool[@name='[excluded]'][.='true']",
          "//result/doc[2]/bool[@name='[excluded]'][.='false']",
          "//result/doc[3]/bool[@name='[excluded]'][.='false']"
      );
    } finally {
      delete();
    }
  }

  @Test
  public void testSorting() throws Exception {
    try {
      init("schema12.xml");
      assertU(adoc("id", "a", "title", "ipod trash trash", "str_s1", "group1"));
      assertU(adoc("id", "b", "title", "ipod ipod  trash", "str_s1", "group2"));
      assertU(adoc("id", "c", "title", "ipod ipod  ipod ", "str_s1", "group2"));

      assertU(adoc("id", "x", "title", "boosted",                 "str_s1", "group1"));
      assertU(adoc("id", "y", "title", "boosted boosted",         "str_s1", "group2"));
      assertU(adoc("id", "z", "title", "boosted boosted boosted", "str_s1", "group2"));
      assertU(commit());

      final String query = "title:ipod";

      final SolrParams baseParams = params(
          "qt", "/elevate",
          "q", query,
          "fl", "id,score",
          "indent", "true");

      QueryElevationComponent booster = (QueryElevationComponent) h.getCore().getSearchComponent("elevate");
      IndexReader reader = h.getCore().withSearcher(SolrIndexSearcher::getIndexReader);

      assertQ("Make sure standard sort works as expected", req(baseParams)
          , "//*[@numFound='3']"
          , "//result/doc[1]/str[@name='id'][.='c']"
          , "//result/doc[2]/str[@name='id'][.='b']"
          , "//result/doc[3]/str[@name='id'][.='a']"
      );

      // Explicitly set what gets boosted
      booster.setTopQueryResults(reader, query, false, new String[]{"x", "y", "z"}, null);

      assertQ("All six should make it", req(baseParams)
          , "//*[@numFound='6']"
          , "//result/doc[1]/str[@name='id'][.='x']"
          , "//result/doc[2]/str[@name='id'][.='y']"
          , "//result/doc[3]/str[@name='id'][.='z']"
          , "//result/doc[4]/str[@name='id'][.='c']"
          , "//result/doc[5]/str[@name='id'][.='b']"
          , "//result/doc[6]/str[@name='id'][.='a']"
      );

      // now switch the order:
      booster.setTopQueryResults(reader, query, false, new String[]{"a", "x"}, null);
      assertQ(req(baseParams)
          , "//*[@numFound='4']"
          , "//result/doc[1]/str[@name='id'][.='a']"
          , "//result/doc[2]/str[@name='id'][.='x']"
          , "//result/doc[3]/str[@name='id'][.='c']"
          , "//result/doc[4]/str[@name='id'][.='b']"
      );

      // Try normal sort by 'id'
      // default 'forceBoost' should be false
      assertFalse(booster.forceElevation);
      assertQ(req(baseParams, "sort", "id asc")
          , "//*[@numFound='4']"
          , "//result/doc[1]/str[@name='id'][.='a']"
          , "//result/doc[2]/str[@name='id'][.='b']"
          , "//result/doc[3]/str[@name='id'][.='c']"
          , "//result/doc[4]/str[@name='id'][.='x']"
      );

      assertQ("useConfiguredElevatedOrder=false",
          req(baseParams, "sort", "str_s1 asc,id desc", "useConfiguredElevatedOrder", "false")
          , "//*[@numFound='4']"
          , "//result/doc[1]/str[@name='id'][.='x']"//group1
          , "//result/doc[2]/str[@name='id'][.='a']"//group1
          , "//result/doc[3]/str[@name='id'][.='c']"
          , "//result/doc[4]/str[@name='id'][.='b']"
      );

      booster.forceElevation = true;
      assertQ(req(baseParams, "sort", "id asc")
          , "//*[@numFound='4']"
          , "//result/doc[1]/str[@name='id'][.='a']"
          , "//result/doc[2]/str[@name='id'][.='x']"
          , "//result/doc[3]/str[@name='id'][.='b']"
          , "//result/doc[4]/str[@name='id'][.='c']"
      );

      booster.forceElevation = true;
      assertQ("useConfiguredElevatedOrder=false and forceElevation",
          req(baseParams, "sort", "id desc", "useConfiguredElevatedOrder", "false")
          , "//*[@numFound='4']"
          , "//result/doc[1]/str[@name='id'][.='x']" // force elevated
          , "//result/doc[2]/str[@name='id'][.='a']" // force elevated
          , "//result/doc[3]/str[@name='id'][.='c']"
          , "//result/doc[4]/str[@name='id'][.='b']"
      );

      //Test exclusive (not to be confused with exclusion)
      booster.setTopQueryResults(reader, query, false, new String[]{"x", "a"}, new String[]{});
      assertQ(req(baseParams, "exclusive", "true")
          , "//*[@numFound='2']"
          , "//result/doc[1]/str[@name='id'][.='x']"
          , "//result/doc[2]/str[@name='id'][.='a']"
      );

      // Test exclusion
      booster.setTopQueryResults(reader, query, false, new String[]{"x"}, new String[]{"a"});
      assertQ(req(baseParams)
          , "//*[@numFound='3']"
          , "//result/doc[1]/str[@name='id'][.='x']"
          , "//result/doc[2]/str[@name='id'][.='c']"
          , "//result/doc[3]/str[@name='id'][.='b']"
      );


      // Test setting ids and excludes from http parameters

      booster.clearElevationProviderCache();
      assertQ("All five should make it", req(baseParams, "elevateIds", "x,y,z", "excludeIds", "b")
          , "//*[@numFound='5']"
          , "//result/doc[1]/str[@name='id'][.='x']"
          , "//result/doc[2]/str[@name='id'][.='y']"
          , "//result/doc[3]/str[@name='id'][.='z']"
          , "//result/doc[4]/str[@name='id'][.='c']"
          , "//result/doc[5]/str[@name='id'][.='a']"
      );

      assertQ("All four should make it", req(baseParams, "elevateIds", "x,z,y", "excludeIds", "b,c")
          , "//*[@numFound='4']"
          , "//result/doc[1]/str[@name='id'][.='x']"
          , "//result/doc[2]/str[@name='id'][.='z']"
          , "//result/doc[3]/str[@name='id'][.='y']"
          , "//result/doc[4]/str[@name='id'][.='a']"
      );

    } finally {
      delete();
    }
  }

  // write an elevation config file to boost some docs
  private void writeElevationConfigFile(File file, String query, String... ids) throws Exception {
    PrintWriter out = new PrintWriter(new OutputStreamWriter(new FileOutputStream(file), StandardCharsets.UTF_8));
    out.println("<?xml version=\"1.0\" encoding=\"UTF-8\" ?>");
    out.println("<elevate>");
    out.println("<query text=\"" + query + "\">");
    for (String id : ids) {
      out.println(" <doc id=\"" + id + "\"/>");
    }
    out.println("</query>");
    out.println("</elevate>");
    out.flush();
    out.close();

    log.info("OUT:" + file.getAbsolutePath());
  }

  @Test
  public void testElevationReloading() throws Exception {
    try {
      init("schema12.xml");
      String testfile = "data-elevation.xml";
      File configFile = new File(h.getCore().getDataDir(), testfile);
      writeElevationConfigFile(configFile, "aaa", "A");

      QueryElevationComponent comp = (QueryElevationComponent) h.getCore().getSearchComponent("elevate");
      NamedList<String> args = new NamedList<>();
      args.add(QueryElevationComponent.CONFIG_FILE, testfile);
      comp.init(args);
      comp.inform(h.getCore());

      QueryElevationComponent.ElevationProvider elevationProvider;

      try (SolrQueryRequest req = req()) {
        elevationProvider = comp.getElevationProvider(req.getSearcher().getIndexReader(), req.getCore());
        assertTrue(elevationProvider.getElevationForQuery("aaa").elevatedIds.contains(new BytesRef("A")));
        assertNull(elevationProvider.getElevationForQuery("bbb"));
      }

      // now change the file
      writeElevationConfigFile(configFile, "bbb", "B");

      // With no index change, we get the same index reader, so the elevationProviderCache returns the previous ElevationProvider without the change.
      try (SolrQueryRequest req = req()) {
        elevationProvider = comp.getElevationProvider(req.getSearcher().getIndexReader(), req.getCore());
        assertTrue(elevationProvider.getElevationForQuery("aaa").elevatedIds.contains(new BytesRef("A")));
        assertNull(elevationProvider.getElevationForQuery("bbb"));
      }

      // Index a new doc to get a new index reader.
      assertU(adoc("id", "10000"));
      assertU(commit());

      // Check that we effectively reload a new ElevationProvider for a different index reader (so two entries in elevationProviderCache).
      try (SolrQueryRequest req = req()) {
        elevationProvider = comp.getElevationProvider(req.getSearcher().getIndexReader(), req.getCore());
        assertNull(elevationProvider.getElevationForQuery("aaa"));
        assertTrue(elevationProvider.getElevationForQuery("bbb").elevatedIds.contains(new BytesRef("B")));
      }

      // Now change the config file again.
      writeElevationConfigFile(configFile, "ccc", "C");

      // Without index change, but calling a different method that clears the elevationProviderCache, so we should load a new ElevationProvider.
      int elevationRuleNumber = comp.loadElevationConfiguration(h.getCore());
      assertEquals(1, elevationRuleNumber);
      try (SolrQueryRequest req = req()) {
        elevationProvider = comp.getElevationProvider(req.getSearcher().getIndexReader(), req.getCore());
        assertNull(elevationProvider.getElevationForQuery("aaa"));
        assertNull(elevationProvider.getElevationForQuery("bbb"));
        assertTrue(elevationProvider.getElevationForQuery("ccc").elevatedIds.contains(new BytesRef("C")));
      }
    } finally {
      delete();
    }
  }

  @Test
  public void testWithLocalParam() throws Exception {
    try {
      init("schema11.xml");
      clearIndex();
      assertU(commit());
      assertU(adoc("id", "7", "text", "AAAA", "str_s", "a"));
      assertU(commit());

      assertQ("", req(CommonParams.Q, "AAAA", CommonParams.QT, "/elevate",
          CommonParams.FL, "id, score, [elevated]")
          , "//*[@numFound='1']"
          , "//result/doc[1]/str[@name='id'][.='7']"
          , "//result/doc[1]/bool[@name='[elevated]'][.='true']"
      );
      assertQ("", req(CommonParams.Q, "{!q.op=AND}AAAA", CommonParams.QT, "/elevate",
          CommonParams.FL, "id, score, [elevated]")
          , "//*[@numFound='1']"
          , "//result/doc[1]/str[@name='id'][.='7']"
          , "//result/doc[1]/bool[@name='[elevated]'][.='true']"
      );
      assertQ("", req(CommonParams.Q, "{!q.op=AND v='AAAA'}", CommonParams.QT, "/elevate",
          CommonParams.FL, "id, score, [elevated]")
          , "//*[@numFound='1']"
          , "//result/doc[1]/str[@name='id'][.='7']"
          , "//result/doc[1]/bool[@name='[elevated]'][.='true']"
      );
    } finally {
      delete();
    }
  }

  @Test
  public void testQuerySubsetMatching() throws Exception {
    try {
      init("schema12.xml");
      assertU(adoc("id", "1", "title", "XXXX", "str_s1", "a"));
      assertU(adoc("id", "2", "title", "YYYY", "str_s1", "b"));
      assertU(adoc("id", "3", "title", "ZZZZ", "str_s1", "c"));

      assertU(adoc("id", "4", "title", "XXXX XXXX", "str_s1", "x"));
      assertU(adoc("id", "5", "title", "YYYY YYYY", "str_s1", "y"));
      assertU(adoc("id", "6", "title", "XXXX XXXX", "str_s1", "z"));
      assertU(adoc("id", "7", "title", "AAAA", "str_s1", "a"));

      assertU(adoc("id", "10", "title", "RR", "str_s1", "r"));
      assertU(adoc("id", "11", "title", "SS", "str_s1", "r"));
      assertU(adoc("id", "12", "title", "TT", "str_s1", "r"));
      assertU(adoc("id", "13", "title", "UU", "str_s1", "r"));
      assertU(adoc("id", "14", "title", "VV", "str_s1", "r"));
      assertU(commit());

      // Exact matching.
      assertQ("", req(CommonParams.Q, "XXXX", CommonParams.QT, "/elevate",
          CommonParams.FL, "id, score, [elevated]"),
          "//*[@numFound='3']",
          "//result/doc[1]/str[@name='id'][.='1']",
          "//result/doc[2]/str[@name='id'][.='4']",
          "//result/doc[3]/str[@name='id'][.='6']",
          "//result/doc[1]/bool[@name='[elevated]'][.='true']",
          "//result/doc[2]/bool[@name='[elevated]'][.='false']",
          "//result/doc[3]/bool[@name='[elevated]'][.='false']"
      );

      // Exact matching.
      assertQ("", req(CommonParams.Q, "QQQQ EE", CommonParams.QT, "/elevate",
          CommonParams.FL, "id, score, [elevated]"),
          "//*[@numFound='0']"
      );

      // Subset matching.
      assertQ("", req(CommonParams.Q, "BB DD CC VV", CommonParams.QT, "/elevate",
          CommonParams.FL, "id, score, [elevated]"),
          "//*[@numFound='4']",
          "//result/doc[1]/str[@name='id'][.='10']",
          "//result/doc[2]/str[@name='id'][.='12']",
          "//result/doc[3]/str[@name='id'][.='11']",
          "//result/doc[4]/str[@name='id'][.='14']",
          "//result/doc[1]/bool[@name='[elevated]'][.='true']",
          "//result/doc[2]/bool[@name='[elevated]'][.='true']",
          "//result/doc[3]/bool[@name='[elevated]'][.='true']",
          "//result/doc[4]/bool[@name='[elevated]'][.='false']"
      );

      // Subset + exact matching.
      assertQ("", req(CommonParams.Q, "BB CC", CommonParams.QT, "/elevate",
          CommonParams.FL, "id, score, [elevated]"),
          "//*[@numFound='4']",
          "//result/doc[1]/str[@name='id'][.='13']",
          "//result/doc[2]/str[@name='id'][.='10']",
          "//result/doc[3]/str[@name='id'][.='12']",
          "//result/doc[4]/str[@name='id'][.='11']",
          "//result/doc[1]/bool[@name='[elevated]'][.='true']",
          "//result/doc[2]/bool[@name='[elevated]'][.='true']",
          "//result/doc[3]/bool[@name='[elevated]'][.='true']",
          "//result/doc[4]/bool[@name='[elevated]'][.='true']"
      );

      // Subset matching.
      assertQ("", req(CommonParams.Q, "AA BB DD CC AA", CommonParams.QT, "/elevate",
          CommonParams.FL, "id, score, [elevated]"),
          "//*[@numFound='4']",
          "//result/doc[1]/str[@name='id'][.='10']",
          "//result/doc[2]/str[@name='id'][.='12']",
          "//result/doc[3]/str[@name='id'][.='11']",
          "//result/doc[4]/str[@name='id'][.='14']",
          "//result/doc[1]/bool[@name='[elevated]'][.='true']",
          "//result/doc[2]/bool[@name='[elevated]'][.='true']",
          "//result/doc[3]/bool[@name='[elevated]'][.='true']",
          "//result/doc[4]/bool[@name='[elevated]'][.='true']"
      );

      // Subset matching.
      assertQ("", req(CommonParams.Q, "AA RR BB DD AA", CommonParams.QT, "/elevate",
          CommonParams.FL, "id, score, [elevated]"),
          "//*[@numFound='3']",
          "//result/doc[1]/str[@name='id'][.='12']",
          "//result/doc[2]/str[@name='id'][.='14']",
          "//result/doc[3]/str[@name='id'][.='10']",
          "//result/doc[1]/bool[@name='[elevated]'][.='true']",
          "//result/doc[2]/bool[@name='[elevated]'][.='true']",
          "//result/doc[3]/bool[@name='[elevated]'][.='false']"
      );

      // Subset matching.
      assertQ("", req(CommonParams.Q, "AA BB EE", CommonParams.QT, "/elevate",
          CommonParams.FL, "id, score, [elevated]")
          , "//*[@numFound='0']"
          );
    } finally {
      delete();
    }
  }

  @Test
  public void testElevatedIds() throws Exception {
    try {
      init("schema12.xml");
      SolrCore core = h.getCore();

      NamedList<String> args = new NamedList<>();
      args.add(QueryElevationComponent.FIELD_TYPE, "text");
      args.add(QueryElevationComponent.CONFIG_FILE, "elevate.xml");

      QueryElevationComponent comp = new QueryElevationComponent();
      comp.init(args);
      comp.inform(core);

      SolrQueryRequest req = req();
      IndexReader reader = req.getSearcher().getIndexReader();
      QueryElevationComponent.ElevationProvider elevationProvider = comp.getElevationProvider(reader, core);
      req.close();

      assertEquals(toIdSet("1"), elevationProvider.getElevationForQuery("xxxx").elevatedIds);
      assertEquals(toIdSet("10", "11", "12"), elevationProvider.getElevationForQuery("bb DD CC vv").elevatedIds);
      assertEquals(toIdSet("10", "11", "12", "13"), elevationProvider.getElevationForQuery("BB Cc").elevatedIds);
      assertEquals(toIdSet("10", "11", "12", "14"), elevationProvider.getElevationForQuery("aa bb dd cc aa").elevatedIds);
    } finally {
      delete();
    }
  }

  private static Set<BytesRef> toIdSet(String... ids) {
    return Arrays.stream(ids).map(BytesRef::new).collect(Collectors.toSet());
  }
}

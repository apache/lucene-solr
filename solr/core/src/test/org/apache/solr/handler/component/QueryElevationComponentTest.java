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

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.util.BytesRef;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.GroupParams;
import org.apache.solr.common.params.MapSolrParams;
import org.apache.solr.common.params.QueryElevationParams;
import org.apache.solr.util.FileUtils;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.component.QueryElevationComponent.ElevationObj;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.request.SolrQueryRequest;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;


public class QueryElevationComponentTest extends SolrTestCaseJ4 {


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
    File elevateDataFile = new File(initCoreDataDir, "elevate-data.xml");
    FileUtils.copyFile(elevateFile, elevateDataFile);


    initCore(config,schema);
    clearIndex();
    assertU(commit());
  }

  private void delete() throws Exception {
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
          , "//result/doc[1]/float[@name='id'][.='7.0']"
          , "//result/doc[2]/float[@name='id'][.='8.0']"
          , "//result/doc[3]/float[@name='id'][.='9.0']",
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

              , groups +"/lst[1]//doc[1]/float[@name='id'][.='6.0']"
              , groups +"/lst[1]//doc[1]/bool[@name='[elevated]'][.='false']"
              , groups +"/lst[1]//doc[2]/float[@name='id'][.='66.0']"
              , groups +"/lst[1]//doc[2]/bool[@name='[elevated]'][.='false']"

              , groups +"/lst[2]//doc[1]/float[@name='id'][.='7.0']"
              , groups +"/lst[2]//doc[1]/bool[@name='[elevated]'][.='false']"
              , groups +"/lst[2]//doc[2]/float[@name='id'][.='77.0']"
              , groups +"/lst[2]//doc[2]/bool[@name='[elevated]'][.='false']"

              , groups +"/lst[3]//doc[1]/float[@name='id'][.='2.0']"
              , groups +"/lst[3]//doc[1]/bool[@name='[elevated]'][.='false']"
              , groups +"/lst[3]//doc[2]/float[@name='id'][.='22.0']"
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

              , groups +"/lst[1]//doc[1]/float[@name='id'][.='7.0']"
              , groups +"/lst[1]//doc[1]/bool[@name='[elevated]'][.='true']"
              , groups +"/lst[1]//doc[2]/float[@name='id'][.='77.0']"
              , groups +"/lst[1]//doc[2]/bool[@name='[elevated]'][.='false']"

              , groups +"/lst[2]//doc[1]/float[@name='id'][.='6.0']"
              , groups +"/lst[2]//doc[1]/bool[@name='[elevated]'][.='false']"
              , groups +"/lst[2]//doc[2]/float[@name='id'][.='66.0']"
              , groups +"/lst[2]//doc[2]/bool[@name='[elevated]'][.='false']"

              , groups +"/lst[3]//doc[1]/float[@name='id'][.='2.0']"
              , groups +"/lst[3]//doc[1]/bool[@name='[elevated]'][.='false']"
              , groups +"/lst[3]//doc[2]/float[@name='id'][.='22.0']"
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

              , groups +"/lst[1]//doc[1]/float[@name='id'][.='2.0']"
              , groups +"/lst[1]//doc[1]/bool[@name='[elevated]'][.='false']"
              , groups +"/lst[1]//doc[2]/float[@name='id'][.='22.0']"
              , groups +"/lst[1]//doc[2]/bool[@name='[elevated]'][.='false']"

              , groups +"/lst[2]//doc[1]/float[@name='id'][.='6.0']"
              , groups +"/lst[2]//doc[1]/bool[@name='[elevated]'][.='false']"
              , groups +"/lst[2]//doc[2]/float[@name='id'][.='66.0']"
              , groups +"/lst[2]//doc[2]/bool[@name='[elevated]'][.='false']"

              , groups +"/lst[3]//doc[1]/float[@name='id'][.='7.0']"
              , groups +"/lst[3]//doc[1]/bool[@name='[elevated]'][.='true']"
              , groups +"/lst[3]//doc[2]/float[@name='id'][.='77.0']"
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

              , groups +"/lst[1]//doc[1]/float[@name='id'][.='7.0']"
              , groups +"/lst[1]//doc[1]/bool[@name='[elevated]'][.='true']"
              , groups +"/lst[1]//doc[2]/float[@name='id'][.='77.0']"
              , groups +"/lst[1]//doc[2]/bool[@name='[elevated]'][.='false']"

              , groups +"/lst[2]//doc[1]/float[@name='id'][.='2.0']"
              , groups +"/lst[2]//doc[1]/bool[@name='[elevated]'][.='false']"
              , groups +"/lst[2]//doc[2]/float[@name='id'][.='22.0']"
              , groups +"/lst[2]//doc[2]/bool[@name='[elevated]'][.='false']"

              , groups +"/lst[3]//doc[1]/float[@name='id'][.='6.0']"
              , groups +"/lst[3]//doc[1]/bool[@name='[elevated]'][.='false']"
              , groups +"/lst[3]//doc[2]/float[@name='id'][.='66.0']"
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

              , groups +"/lst[1]//doc[1]/float[@name='id'][.='22.0']"
              , groups +"/lst[1]//doc[1]/bool[@name='[elevated]'][.='false']"
              , groups +"/lst[1]//doc[2]/float[@name='id'][.='2.0']"
              , groups +"/lst[1]//doc[2]/bool[@name='[elevated]'][.='false']"

              , groups +"/lst[2]//doc[1]/float[@name='id'][.='66.0']"
              , groups +"/lst[2]//doc[1]/bool[@name='[elevated]'][.='false']"
              , groups +"/lst[2]//doc[2]/float[@name='id'][.='6.0']"
              , groups +"/lst[2]//doc[2]/bool[@name='[elevated]'][.='false']"

              , groups +"/lst[3]//doc[1]/float[@name='id'][.='77.0']"
              , groups +"/lst[3]//doc[1]/bool[@name='[elevated]'][.='false']"
              , groups +"/lst[3]//doc[2]/float[@name='id'][.='7.0']"
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

              , groups +"/lst[1]//doc[1]/float[@name='id'][.='7.0']"
              , groups +"/lst[1]//doc[1]/bool[@name='[elevated]'][.='true']"
              , groups +"/lst[1]//doc[2]/float[@name='id'][.='77.0']"
              , groups +"/lst[1]//doc[2]/bool[@name='[elevated]'][.='false']"

              , groups +"/lst[2]//doc[1]/float[@name='id'][.='22.0']"
              , groups +"/lst[2]//doc[1]/bool[@name='[elevated]'][.='false']"
              , groups +"/lst[2]//doc[2]/float[@name='id'][.='2.0']"
              , groups +"/lst[2]//doc[2]/bool[@name='[elevated]'][.='false']"

              , groups +"/lst[3]//doc[1]/float[@name='id'][.='66.0']"
              , groups +"/lst[3]//doc[1]/bool[@name='[elevated]'][.='false']"
              , groups +"/lst[3]//doc[2]/float[@name='id'][.='6.0']"
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
              ,"//result/doc[1]/int[@name='id'][.='7']"
              ,"//result/doc[2]/int[@name='id'][.='8']"
              ,"//result/doc[3]/int[@name='id'][.='9']",
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
      Map<String, ElevationObj> map = comp.getElevationMap(reader, core);
      req.close();

      // Make sure the boosts loaded properly
      assertEquals(7, map.size());
      assertEquals(1, map.get("XXXX").priority.size());
      assertEquals(2, map.get("YYYY").priority.size());
      assertEquals(3, map.get("ZZZZ").priority.size());
      assertEquals(null, map.get("xxxx"));
      assertEquals(null, map.get("yyyy"));
      assertEquals(null, map.get("zzzz"));

      // Now test the same thing with a lowercase filter: 'lowerfilt'
      args = new NamedList<>();
      args.add(QueryElevationComponent.FIELD_TYPE, "lowerfilt");
      args.add(QueryElevationComponent.CONFIG_FILE, "elevate.xml");

      comp = new QueryElevationComponent();
      comp.init(args);
      comp.inform(core);
      map = comp.getElevationMap(reader, core);
      assertEquals(7, map.size());
      assertEquals(null, map.get("XXXX"));
      assertEquals(null, map.get("YYYY"));
      assertEquals(null, map.get("ZZZZ"));
      assertEquals(1, map.get("xxxx").priority.size());
      assertEquals(2, map.get("yyyy").priority.size());
      assertEquals(3, map.get("zzzz").priority.size());

      assertEquals("xxxx", comp.getAnalyzedQuery("XXXX"));
      assertEquals("xxxxyyyy", comp.getAnalyzedQuery("XXXX YYYY"));

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
      
      assertU(adoc("id", "8", "title", "QQQQ", "str_s1", "q"));
      assertU(adoc("id", "9", "title", "QQQQ QQQQ", "str_s1", "r"));
      assertU(adoc("id", "10", "title", "QQQQ QQQQ QQQQ", "str_s1", "s"));
      
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
          , "//result/doc[1]/str[@name='id'][.='8']"
          , "//result/doc[2]/str[@name='id'][.='9']"
          , "//result/doc[3]/str[@name='id'][.='10']"
      );
      assertQ("", req(CommonParams.Q, "QQQQ", CommonParams.QT, "/elevate",
          QueryElevationParams.MARK_EXCLUDES, "true",
          "indent", "true",
          CommonParams.FL, "id, score, [excluded]")
          , "//*[@numFound='3']"
          , "//result/doc[1]/str[@name='id'][.='8']"
          , "//result/doc[2]/str[@name='id'][.='9']"
          , "//result/doc[3]/str[@name='id'][.='10']",
          "//result/doc[1]/bool[@name='[excluded]'][.='false']",
          "//result/doc[2]/bool[@name='[excluded]'][.='false']",
          "//result/doc[3]/bool[@name='[excluded]'][.='true']"
      );
    } finally {
      delete();
    }
  }

  @Test
  public void testSorting() throws Exception {
    try {
      init("schema12.xml");
      assertU(adoc("id", "a", "title", "ipod", "str_s1", "a"));
      assertU(adoc("id", "b", "title", "ipod ipod", "str_s1", "b"));
      assertU(adoc("id", "c", "title", "ipod ipod ipod", "str_s1", "c"));

      assertU(adoc("id", "x", "title", "boosted", "str_s1", "x"));
      assertU(adoc("id", "y", "title", "boosted boosted", "str_s1", "y"));
      assertU(adoc("id", "z", "title", "boosted boosted boosted", "str_s1", "z"));
      assertU(commit());

      String query = "title:ipod";

      Map<String, String> args = new HashMap<>();
      args.put(CommonParams.Q, query);
      args.put(CommonParams.QT, "/elevate");
      args.put(CommonParams.FL, "id,score");
      args.put("indent", "true");
      //args.put( CommonParams.FL, "id,title,score" );
      SolrQueryRequest req = new LocalSolrQueryRequest(h.getCore(), new MapSolrParams(args));
      IndexReader reader = req.getSearcher().getIndexReader();
      QueryElevationComponent booster = (QueryElevationComponent) req.getCore().getSearchComponent("elevate");

      assertQ("Make sure standard sort works as expected", req
          , "//*[@numFound='3']"
          , "//result/doc[1]/str[@name='id'][.='a']"
          , "//result/doc[2]/str[@name='id'][.='b']"
          , "//result/doc[3]/str[@name='id'][.='c']"
      );

      // Explicitly set what gets boosted
      booster.elevationCache.clear();
      booster.setTopQueryResults(reader, query, new String[]{"x", "y", "z"}, null);


      assertQ("All six should make it", req
          , "//*[@numFound='6']"
          , "//result/doc[1]/str[@name='id'][.='x']"
          , "//result/doc[2]/str[@name='id'][.='y']"
          , "//result/doc[3]/str[@name='id'][.='z']"
          , "//result/doc[4]/str[@name='id'][.='a']"
          , "//result/doc[5]/str[@name='id'][.='b']"
          , "//result/doc[6]/str[@name='id'][.='c']"
      );

      booster.elevationCache.clear();

      // now switch the order:
      booster.setTopQueryResults(reader, query, new String[]{"a", "x"}, null);
      assertQ("All four should make it", req
          , "//*[@numFound='4']"
          , "//result/doc[1]/str[@name='id'][.='a']"
          , "//result/doc[2]/str[@name='id'][.='x']"
          , "//result/doc[3]/str[@name='id'][.='b']"
          , "//result/doc[4]/str[@name='id'][.='c']"
      );

      // Test reverse sort
      args.put(CommonParams.SORT, "score asc");
      assertQ("All four should make it", req
          , "//*[@numFound='4']"
          , "//result/doc[4]/str[@name='id'][.='a']"
          , "//result/doc[3]/str[@name='id'][.='x']"
          , "//result/doc[2]/str[@name='id'][.='b']"
          , "//result/doc[1]/str[@name='id'][.='c']"
      );

      // Try normal sort by 'id'
      // default 'forceBoost' should be false
      assertEquals(false, booster.forceElevation);
      args.put(CommonParams.SORT, "str_s1 asc");
      assertQ(null, req
          , "//*[@numFound='4']"
          , "//result/doc[1]/str[@name='id'][.='a']"
          , "//result/doc[2]/str[@name='id'][.='b']"
          , "//result/doc[3]/str[@name='id'][.='c']"
          , "//result/doc[4]/str[@name='id'][.='x']"
      );
      args.put(CommonParams.SORT, "id asc");
      assertQ(null, req
          , "//*[@numFound='4']"
          , "//result/doc[1]/str[@name='id'][.='a']"
          , "//result/doc[2]/str[@name='id'][.='b']"
          , "//result/doc[3]/str[@name='id'][.='c']"
          , "//result/doc[4]/str[@name='id'][.='x']"
      );

      booster.forceElevation = true;
      args.put(CommonParams.SORT, "id asc");
      assertQ(null, req
          , "//*[@numFound='4']"
          , "//result/doc[1]/str[@name='id'][.='a']"
          , "//result/doc[2]/str[@name='id'][.='x']"
          , "//result/doc[3]/str[@name='id'][.='b']"
          , "//result/doc[4]/str[@name='id'][.='c']"
      );

      //Test exclusive (not to be confused with exclusion)
      args.put(QueryElevationParams.EXCLUSIVE, "true");
      booster.setTopQueryResults(reader, query, new String[]{"x", "a"}, new String[]{});
      assertQ(null, req
          , "//*[@numFound='2']"
          , "//result/doc[1]/str[@name='id'][.='x']"
          , "//result/doc[2]/str[@name='id'][.='a']"
      );

      // Test exclusion
      booster.elevationCache.clear();
      args.remove(CommonParams.SORT);
      args.remove(QueryElevationParams.EXCLUSIVE);
      booster.setTopQueryResults(reader, query, new String[]{"x"}, new String[]{"a"});
      assertQ(null, req
          , "//*[@numFound='3']"
          , "//result/doc[1]/str[@name='id'][.='x']"
          , "//result/doc[2]/str[@name='id'][.='b']"
          , "//result/doc[3]/str[@name='id'][.='c']"
      );


      // Test setting ids and excludes from http parameters

      booster.elevationCache.clear();
      args.put(QueryElevationParams.IDS, "x,y,z");
      args.put(QueryElevationParams.EXCLUDE, "b");

      assertQ("All five should make it", req
          , "//*[@numFound='5']"
          , "//result/doc[1]/str[@name='id'][.='x']"
          , "//result/doc[2]/str[@name='id'][.='y']"
          , "//result/doc[3]/str[@name='id'][.='z']"
          , "//result/doc[4]/str[@name='id'][.='a']"
          , "//result/doc[5]/str[@name='id'][.='c']"
      );

      args.put(QueryElevationParams.IDS, "x,z,y");
      args.put(QueryElevationParams.EXCLUDE, "b,c");

      assertQ("All four should make it", req
          , "//*[@numFound='4']"
          , "//result/doc[1]/str[@name='id'][.='x']"
          , "//result/doc[2]/str[@name='id'][.='z']"
          , "//result/doc[3]/str[@name='id'][.='y']"
          , "//result/doc[4]/str[@name='id'][.='a']"
      );

      req.close();
    } finally {
      delete();
    }
  }

  // write a test file to boost some docs
  private void writeFile(File file, String query, String... ids) throws Exception {
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
      File f = new File(h.getCore().getDataDir(), testfile);
      writeFile(f, "aaa", "A");

      QueryElevationComponent comp = (QueryElevationComponent) h.getCore().getSearchComponent("elevate");
      NamedList<String> args = new NamedList<>();
      args.add(QueryElevationComponent.CONFIG_FILE, testfile);
      comp.init(args);
      comp.inform(h.getCore());

      SolrQueryRequest req = req();
      IndexReader reader = req.getSearcher().getIndexReader();
      Map<String, ElevationObj> map = comp.getElevationMap(reader, h.getCore());
      assertTrue(map.get("aaa").priority.containsKey(new BytesRef("A")));
      assertNull(map.get("bbb"));
      req.close();

      // now change the file
      writeFile(f, "bbb", "B");
      assertU(adoc("id", "10000")); // will get same reader if no index change
      assertU(commit());

      req = req();
      reader = req.getSearcher().getIndexReader();
      map = comp.getElevationMap(reader, h.getCore());
      assertNull(map.get("aaa"));
      assertTrue(map.get("bbb").priority.containsKey(new BytesRef("B")));
      req.close();
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
          , "//result/doc[1]/float[@name='id'][.='7.0']"
          , "//result/doc[1]/bool[@name='[elevated]'][.='true']"
      );
      assertQ("", req(CommonParams.Q, "{!q.op=AND}AAAA", CommonParams.QT, "/elevate",
          CommonParams.FL, "id, score, [elevated]")
          , "//*[@numFound='1']"
          , "//result/doc[1]/float[@name='id'][.='7.0']"
          , "//result/doc[1]/bool[@name='[elevated]'][.='true']"
      );
      assertQ("", req(CommonParams.Q, "{!q.op=AND v='AAAA'}", CommonParams.QT, "/elevate",
          CommonParams.FL, "id, score, [elevated]")
          , "//*[@numFound='1']"
          , "//result/doc[1]/float[@name='id'][.='7.0']"
          , "//result/doc[1]/bool[@name='[elevated]'][.='true']"
      );
    } finally {
      delete();
    }
  }
}

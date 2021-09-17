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
package org.apache.solr;

import org.apache.solr.client.solrj.impl.BinaryResponseParser;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.GroupParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.Utils;
import org.apache.solr.index.LogDocMergePolicyFactory;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrRequestInfo;
import org.apache.solr.response.BinaryResponseWriter;
import org.apache.solr.response.ResultContext;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.schema.IndexSchema;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class TestGroupingSearch extends SolrTestCaseJ4 {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static final String FOO_STRING_FIELD = "foo_s1";
  public static final String FOO_STRING_DOCVAL_FIELD = "foo_sdv";
  public static final String SMALL_STRING_FIELD = "small_s1";
  public static final String SMALL_INT_FIELD = "small_i";
  static final String EMPTY_FACETS = "'facet_ranges':{},'facet_intervals':{},'facet_heatmaps':{}";

  @BeforeClass
  public static void beforeTests() throws Exception {
    // we need DVs on point fields to compute stats & facets
    if (Boolean.getBoolean(NUMERIC_POINTS_SYSPROP)) System.setProperty(NUMERIC_DOCVALUES_SYSPROP,"true");
    
    // force LogDocMergePolicy so that we get a predictable doc order
    // when doing unsorted group collection
    systemSetPropertySolrTestsMergePolicyFactory(LogDocMergePolicyFactory.class.getName());

    System.setProperty("enable.update.log", "false"); // schema12 doesn't support _version_
    initCore("solrconfig.xml", "schema12.xml");
  }

  @AfterClass
  public static void afterTests() {
    systemClearPropertySolrTestsMergePolicyFactory();
  }

  @Before
  public void cleanIndex() {
    assertU(delQ("*:*"));
    assertU(commit());
  }

  @Test
  public void testGroupingGroupSortingScore_basic() {
    assertU(add(doc("id", "1", "id_i", "1", "name", "author1", "title", "a book title", "group_i", "1")));
    assertU(add(doc("id", "2", "id_i", "2", "name", "author1", "title", "the title", "group_i", "2")));
    assertU(add(doc("id", "3", "id_i", "3", "name", "author2", "title", "a book title", "group_i", "1")));
    assertU(add(doc("id", "4", "id_i", "4", "name", "author2", "title", "title", "group_i", "2")));
    assertU(add(doc("id", "5", "id_i", "5", "name", "author3", "title", "the title of a title", "group_i", "1")));
    assertU(commit());

    // function based query for predictable scores not affect by similarity
    assertQ(req("q","{!func}id_i", "group", "true", "group.field","name", "fl", "id, score")
            ,"//lst[@name='grouped']/lst[@name='name']"
            ,"*[count(//arr[@name='groups']/lst) = 3]"

            ,"//arr[@name='groups']/lst[1]/str[@name='groupValue'][.='author3']"
            ,"//arr[@name='groups']/lst[1]/result[@numFound='1']"
            ,"//arr[@name='groups']/lst[1]/result/doc/*[@name='id'][.='5']"

            ,"//arr[@name='groups']/lst[2]/str[@name='groupValue'][.='author2']"
            ,"//arr[@name='groups']/lst[2]/result[@numFound='2']"
            ,"//arr[@name='groups']/lst[2]/result/doc/*[@name='id'][.='4']"

            ,"//arr[@name='groups']/lst[3]/str[@name='groupValue'][.='author1']"
            ,"//arr[@name='groups']/lst[3]/result[@numFound='2']"
            ,"//arr[@name='groups']/lst[3]/result/doc/*[@name='id'][.='2']"

            );

    assertQ(req("q", "title:title", "group", "true", "group.field", "group_i")
        , "//lst[@name='grouped']/lst[@name='group_i']"
        , "*[count(//arr[@name='groups']/lst) = 2]"

        , "//arr[@name='groups']/lst[1]/int[@name='groupValue'][.='2']"
        , "//arr[@name='groups']/lst[1]/result[@numFound='2']"
        , "//arr[@name='groups']/lst[1]/result/doc/*[@name='id'][.='4']"

        , "//arr[@name='groups']/lst[2]/int[@name='groupValue'][.='1']"
        , "//arr[@name='groups']/lst[2]/result[@numFound='3']"
        , "//arr[@name='groups']/lst[2]/result/doc/*[@name='id'][.='5']"
    );

    SolrException exception = expectThrows(SolrException.class, () -> {
      h.query(req("q", "title:title", "group", "true", "group.field", "group_i", "group.offset", "-1"));
    });
    assertEquals(SolrException.ErrorCode.BAD_REQUEST.code, exception.code());
    assertEquals("'group.offset' parameter cannot be negative", exception.getMessage());

    // for group.main=true and group.format=simple, group.offset is not consumed
    assertQ(req("q", "title:title", "group", "true", "group.field", "group_i",
        "group.offset", "-1", "group.format", "simple"));
    assertQ(req("q", "title:title", "group", "true", "group.field", "group_i",
        "group.offset", "-1", "group.main", "true"));
  }

  @Test
  public void testGroupingGroupSortingScore_withTotalGroupCount() {
    assertU(add(doc("id", "1", "id_i", "1", "name", "author1", "title", "a book title", "group_i", "1")));
    assertU(add(doc("id", "2", "id_i", "2", "name", "author1", "title", "the title", "group_i", "2")));
    assertU(add(doc("id", "3", "id_i", "3", "name", "author2", "title", "a book title", "group_i", "1")));
    assertU(add(doc("id", "4", "id_i", "4", "name", "author2", "title", "title", "group_i", "2")));
    assertU(add(doc("id", "5", "id_i", "5", "name", "author3", "title", "the title of a title", "group_i", "1")));
    assertU(commit());

    // function based query for predictable scores not affect by similarity
    assertQ(req("q","{!func}id_i", "group", "true", "group.field","name", "group.ngroups", "true")
            ,"//lst[@name='grouped']/lst[@name='name']"
            ,"//lst[@name='grouped']/lst[@name='name']/int[@name='matches'][.='5']"
            ,"//lst[@name='grouped']/lst[@name='name']/int[@name='ngroups'][.='3']"
            ,"*[count(//arr[@name='groups']/lst) = 3]"

            ,"//arr[@name='groups']/lst[1]/str[@name='groupValue'][.='author3']"
            ,"//arr[@name='groups']/lst[1]/result[@numFound='1']"
            ,"//arr[@name='groups']/lst[1]/result/doc/*[@name='id'][.='5']"
            
            ,"//arr[@name='groups']/lst[2]/str[@name='groupValue'][.='author2']"
            ,"//arr[@name='groups']/lst[2]/result[@numFound='2']"
            ,"//arr[@name='groups']/lst[2]/result/doc/*[@name='id'][.='4']"

            ,"//arr[@name='groups']/lst[3]/str[@name='groupValue'][.='author1']"
            ,"//arr[@name='groups']/lst[3]/result[@numFound='2']"
            ,"//arr[@name='groups']/lst[3]/result/doc/*[@name='id'][.='2']"

            );

    // function based query for predictable scores not affect by similarity
    assertQ(req("q", "{!func}id_i", "group", "true", "group.field", "group_i", "group.ngroups", "true")
        , "//lst[@name='grouped']/lst[@name='group_i']/int[@name='matches'][.='5']"
        , "//lst[@name='grouped']/lst[@name='group_i']/int[@name='ngroups'][.='2']"
        , "*[count(//arr[@name='groups']/lst) = 2]"

        , "//arr[@name='groups']/lst[1]/int[@name='groupValue'][.='1']"
        , "//arr[@name='groups']/lst[1]/result[@numFound='3']"
        , "//arr[@name='groups']/lst[1]/result/doc/*[@name='id'][.='5']"
            
        , "//arr[@name='groups']/lst[2]/int[@name='groupValue'][.='2']"
        , "//arr[@name='groups']/lst[2]/result[@numFound='2']"
        , "//arr[@name='groups']/lst[2]/result/doc/*[@name='id'][.='4']"

    );
  }

  @Test
  public void testGroupingGroupSortingScore_basicWithGroupSortEqualToSort() {
    assertU(add(doc("id", "1", "id_i", "1", "name", "author1", "title", "a book title")));
    assertU(add(doc("id", "2", "id_i", "2", "name", "author1", "title", "the title")));
    assertU(add(doc("id", "3", "id_i", "3", "name", "author2", "title", "a book title")));
    assertU(add(doc("id", "4", "id_i", "4", "name", "author2", "title", "title")));
    assertU(add(doc("id", "5", "id_i", "5", "name", "author3", "title", "the title of a title")));
    assertU(commit());

    // function based query for predictable scores not affect by similarity
    assertQ(req("q", "{!func}id_i", "group", "true", "group.field", "name",
                "sort", "score desc", "group.sort", "score desc")
            
        , "//arr[@name='groups']/lst[1]/str[@name='groupValue'][.='author3']"
        , "//arr[@name='groups']/lst[1]/result[@numFound='1']"
        , "//arr[@name='groups']/lst[1]/result/doc/*[@name='id'][.='5']"
            
        , "//arr[@name='groups']/lst[2]/str[@name='groupValue'][.='author2']"
        , "//arr[@name='groups']/lst[2]/result[@numFound='2']"
        , "//arr[@name='groups']/lst[2]/result/doc/*[@name='id'][.='4']"

        , "//arr[@name='groups']/lst[3]/str[@name='groupValue'][.='author1']"
        , "//arr[@name='groups']/lst[3]/result[@numFound='2']"
        , "//arr[@name='groups']/lst[3]/result/doc/*[@name='id'][.='2']"

    );
  }

  @Test
  public void testGroupingGroupSortingWeight() {
    assertU(add(doc("id", "1","name", "author1", "weight", "12.1")));
    assertU(add(doc("id", "2","name", "author1", "weight", "2.1")));
    assertU(add(doc("id", "3","name", "author2", "weight", "0.1")));
    assertU(add(doc("id", "4","name", "author2", "weight", "0.11")));
    assertU(commit());

    assertQ(req("q", "*:*", "group", "true", "group.field", "name", "sort", "id asc", "group.sort", "weight desc")
        , "*[count(//arr[@name='groups']/lst) = 2]"
        , "//arr[@name='groups']/lst[1]/str[@name='groupValue'][.='author1']"
        //        ,"//arr[@name='groups']/lst[1]/int[@name='matches'][.='2']"
        , "//arr[@name='groups']/lst[1]/result[@numFound='2']"
        , "//arr[@name='groups']/lst[1]/result/doc/*[@name='id'][.='1']"

        , "//arr[@name='groups']/lst[2]/str[@name='groupValue'][.='author2']"
        //        ,"//arr[@name='groups']/lst[2]/int[@name='matches'][.='2']"
        , "//arr[@name='groups']/lst[2]/result[@numFound='2']"
        , "//arr[@name='groups']/lst[2]/result/doc/*[@name='id'][.='4']"
    );
  }

  @Test
  public void testGroupingNoQuery() {
    assertU(add(doc("id", "1","name", "author1", "weight", "12.1")));
    assertU(commit());

    assertQ(req( "group", "true", "group.query", "")
            ,"//lst[2]/lst[@name='']/result[@numFound='0']"
    );
  }

  @Test
  public void testGroupingSimpleFormatArrayIndexOutOfBoundsException() throws Exception {
    assertU(add(doc("id", "1")));
    assertU(add(doc("id", "2")));
    assertU(add(doc("id", "3")));
    assertU(commit());

    assertJQ(
        req("q", "*:*", "start", "1", "group", "true", "group.field", "id", "group.main", "true"),
        "/response=={'numFound':3,'start':1,'numFoundExact':true,'docs':[{'id':'2'},{'id':'3'}]}"
    );
    assertJQ(
        req("q", "*:*", "start", "1", "rows", "1", "group", "true", "group.field", "id", "group.main", "true"),
        "/response=={'numFound':3,'start':1,'numFoundExact':true,'docs':[{'id':'2'}]}"
    );
  }

  @Test
  public void testGroupingSimpleFormatStartBiggerThanRows() throws Exception {
    assertU(add(doc("id", "1")));
    assertU(add(doc("id", "2")));
    assertU(add(doc("id", "3")));
    assertU(add(doc("id", "4")));
    assertU(add(doc("id", "5")));
    assertU(commit());

    assertJQ(
        req("q", "*:*", "start", "2", "rows", "1", "group", "true", "group.field", "id", "group.main", "true"),
        "/response=={'numFound':5,'start':2,'numFoundExact':true,'docs':[{'id':'3'}]}"
    );
  }

  @Test
  public void testGroupingSimpleFormatArrayIndexOutOfBoundsExceptionWithJavaBin() throws Exception {
    assertU(add(doc("id", "1", "nullfirst", "1")));
    assertU(add(doc("id", "2", "nullfirst", "1")));
    assertU(add(doc("id", "3", "nullfirst", "2")));
    assertU(add(doc("id", "4", "nullfirst", "2")));
    assertU(add(doc("id", "5", "nullfirst", "2")));
    assertU(add(doc("id", "6", "nullfirst", "3")));
    assertU(commit());

    SolrQueryRequest request =
        req("q", "*:*","group", "true", "group.field", "nullfirst", "group.main", "true", "wt", "javabin", "start", "4", "rows", "10");

    SolrQueryResponse response = new SolrQueryResponse();
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    try {
      SolrRequestInfo.setRequestInfo(new SolrRequestInfo(request, response));
      String handlerName = request.getParams().get(CommonParams.QT);
      h.getCore().execute(h.getCore().getRequestHandler(handlerName), request, response);
      BinaryResponseWriter responseWriter = new BinaryResponseWriter();
      responseWriter.write(out, request, response);
    } finally {
      request.close();
      SolrRequestInfo.clearRequestInfo();
    }

    assertEquals(6, ((ResultContext) response.getResponse()).getDocList().matches());
    new BinaryResponseParser().processResponse(new ByteArrayInputStream(out.toByteArray()), "");
    out.close();
  }

  @Test
  public void testGroupingWithTimeAllowed() throws Exception {
    assertU(add(doc("id", "1")));
    assertU(add(doc("id", "2")));
    assertU(add(doc("id", "3")));
    assertU(add(doc("id", "4")));
    assertU(add(doc("id", "5")));
    assertU(commit());

    // Just checking if no errors occur
    assertJQ(req("q", "*:*", "group", "true", "group.query", "id:1", "group.query", "id:2", "timeAllowed", "1"));
  }

  @Test
  public void testGroupingSortByFunction() throws Exception {
    assertU(add(doc("id", "1", "value1_i", "1", "value2_i", "1", "store", "45.18014,-93.87742")));
    assertU(add(doc("id", "2", "value1_i", "1", "value2_i", "2", "store", "45.18014,-93.87743")));
    assertU(add(doc("id", "3", "value1_i", "1", "value2_i", "3", "store", "45.18014,-93.87744")));
    assertU(add(doc("id", "4", "value1_i", "1", "value2_i", "4", "store", "45.18014,-93.87745")));
    assertU(add(doc("id", "5", "value1_i", "1", "value2_i", "5", "store", "45.18014,-93.87746")));
    assertU(commit());

    assertJQ(
        req("q", "*:*", "sort", "sum(value1_i, value2_i) desc", "rows", "1", "group", "true", "group.field", "id", "fl", "id"),
        "/grouped=={'id':{'matches':5,'groups':[{'groupValue':'5','doclist':{'numFound':1,'start':0,numFoundExact:true,'docs':[{'id':'5'}]}}]}}"
    );

    assertJQ(
        req("q", "*:*", "sort", "geodist(45.18014,-93.87742,store) asc", "rows", "1", "group", "true", "group.field", "id", "fl", "id"),
        "/grouped=={'id':{'matches':5,'groups':[{'groupValue':'1','doclist':{'numFound':1,'start':0,numFoundExact:true,'docs':[{'id':'1'}]}}]}}"
    );
  }

  @Test
  public void testGroupingGroupedBasedFaceting() throws Exception {
    assertU(add(doc("id", "1", "value1_s1", "1", "value2_i", "1", "value3_s1", "a", "value4_i", "1")));
    assertU(add(doc("id", "2", "value1_s1", "1", "value2_i", "2", "value3_s1", "a", "value4_i", "1")));
    assertU(commit());
    assertU(add(doc("id", "3", "value1_s1", "2", "value2_i", "3", "value3_s1", "b", "value4_i", "2")));
    assertU(add(doc("id", "4", "value1_s1", "1", "value2_i", "4", "value3_s1", "a", "value4_i", "1")));
    assertU(add(doc("id", "5", "value1_s1", "2", "value2_i", "5", "value3_s1", "b", "value4_i", "2")));
    assertU(commit());

    // Facet counts based on documents
    SolrQueryRequest req = req("q", "*:*", "sort", "value2_i asc", "rows", "1", "group", "true", "group.field",
        "value1_s1", "fl", "id", "facet", "true", "facet.field", "value3_s1", "group.truncate", "false");
    assertJQ(
        req,
        "/grouped=={'value1_s1':{'matches':5,'groups':[{'groupValue':'1','doclist':{'numFound':3,'start':0,numFoundExact:true,'docs':[{'id':'1'}]}}]}}",
        "/facet_counts=={'facet_queries':{},'facet_fields':{'value3_s1':['a',3,'b',2]}," + EMPTY_FACETS + "}"
    );

    // Facet counts based on groups
    req = req("q", "*:*", "sort", "value2_i asc", "rows", "1", "group", "true", "group.field",
        "value1_s1", "fl", "id", "facet", "true", "facet.field", "value3_s1", "group.truncate", "true");
    assertJQ(
        req,
        "/grouped=={'value1_s1':{'matches':5,'groups':[{'groupValue':'1','doclist':{'numFound':3,'start':0,numFoundExact:true,'docs':[{'id':'1'}]}}]}}",
        "/facet_counts=={'facet_queries':{},'facet_fields':{'value3_s1':['a',1,'b',1]}," + EMPTY_FACETS + "}"
    );

    // Facet counts based on groups and with group.func. This should trigger FunctionAllGroupHeadsCollector
    req = req("q", "*:*", "sort", "value2_i asc", "rows", "1", "group", "true", "group.func",
        "strdist(1,value1_s1,edit)", "fl", "id", "facet", "true", "facet.field", "value3_s1", "group.truncate", "true");
    assertJQ(
        req,
        "/grouped=={'strdist(1,value1_s1,edit)':{'matches':5,'groups':[{'groupValue':1.0,'doclist':{'numFound':3,'start':0,numFoundExact:true,'docs':[{'id':'1'}]}}]}}",
        "/facet_counts=={'facet_queries':{},'facet_fields':{'value3_s1':['a',1,'b',1]}," + EMPTY_FACETS + "}"
    );

    // Facet counts based on groups without sort on an int field.
    req = req("q", "*:*", "rows", "1", "group", "true", "group.field", "value4_i", "fl", "id", "facet", "true",
        "facet.field", "value3_s1", "group.truncate", "true");
    assertJQ(
        req,
        "/grouped=={'value4_i':{'matches':5,'groups':[{'groupValue':1,'doclist':{'numFound':3,'start':0,numFoundExact:true,'docs':[{'id':'1'}]}}]}}",
        "/facet_counts=={'facet_queries':{},'facet_fields':{'value3_s1':['a',1,'b',1]}," + EMPTY_FACETS + "}"
    );

    // Multi select facets AND group.truncate=true
    req = req("q", "*:*", "rows", "1", "group", "true", "group.field", "value4_i", "fl", "id", "facet", "true",
        "facet.field", "{!ex=v}value3_s1", "group.truncate", "true", "fq", "{!tag=v}value3_s1:b");
    assertJQ(
        req,
        "/grouped=={'value4_i':{'matches':2,'groups':[{'groupValue':2,'doclist':{'numFound':2,'start':0,numFoundExact:true,'docs':[{'id':'3'}]}}]}}",
        "/facet_counts=={'facet_queries':{},'facet_fields':{'value3_s1':['a',1,'b',1]}," + EMPTY_FACETS + "}"
    );

    // Multi select facets AND group.truncate=false
    req = req("q", "*:*", "rows", "1", "group", "true", "group.field", "value4_i", "fl", "id", "facet", "true",
        "facet.field", "{!ex=v}value3_s1", "group.truncate", "false", "fq", "{!tag=v}value3_s1:b");
    assertJQ(
        req,
        "/grouped=={'value4_i':{'matches':2,'groups':[{'groupValue':2,'doclist':{'numFound':2,'start':0,numFoundExact:true,'docs':[{'id':'3'}]}}]}}",
        "/facet_counts=={'facet_queries':{},'facet_fields':{'value3_s1':['a',3,'b',2]}," + EMPTY_FACETS + "}"
    );

    // Multi select facets AND group.truncate=true
    req = req("q", "*:*", "rows", "1", "group", "true", "group.func", "sub(value4_i,1)", "fl", "id", "facet", "true",
        "facet.field", "{!ex=v}value3_s1", "group.truncate", "true", "fq", "{!tag=v}value3_s1:b");
    assertJQ(
        req,
        "/grouped=={'sub(value4_i,1)':{'matches':2,'groups':[{'groupValue':1.0,'doclist':{'numFound':2,'start':0,numFoundExact:true,'docs':[{'id':'3'}]}}]}}",
        "/facet_counts=={'facet_queries':{},'facet_fields':{'value3_s1':['a',1,'b',1]}," + EMPTY_FACETS + "}"
    );
  }

  @Test
  public void testGroupingGroupedBasedFacetingWithTaggedFilter() throws Exception {
    assertU(add(doc("id", "1", "cat_sI", "a", "bday", "2012-11-20T00:00:00Z")));
    assertU(add(doc("id", "2", "cat_sI", "b", "bday", "2012-11-21T00:00:00Z")));
    assertU(add(doc("id", "3", "cat_sI", "a", "bday", "2012-11-20T00:00:00Z")));
    assertU(add(doc("id", "4", "cat_sI", "b", "bday", "2013-01-15T00:00:00Z")));
    assertU(add(doc("id", "5", "cat_sI", "a", "bday", "2013-01-14T00:00:00Z")));
    assertU(commit());

    // Facet counts based on groups
    SolrQueryRequest req = req("q", "*:*", "rows", "1", "group", "true", "group.field", "cat_sI",
        "sort", "cat_sI asc", "fl", "id", "fq", "{!tag=chk}bday:[2012-12-18T00:00:00Z TO 2013-01-17T23:59:59Z]",
        "facet", "true", "group.truncate", "true", "group.sort", "bday desc",
        "facet.query", "{!ex=chk key=LW1}bday:[2013-01-11T00:00:00Z TO 2013-01-17T23:59:59Z]",
        "facet.query", "{!ex=chk key=LM1}bday:[2012-12-18T00:00:00Z TO 2013-01-17T23:59:59Z]",
        "facet.query", "{!ex=chk key=LM3}bday:[2012-10-18T00:00:00Z TO 2013-01-17T23:59:59Z]");
    assertJQ(
        req,
        "/grouped=={'cat_sI':{'matches':2,'groups':[{'groupValue':'a','doclist':{'numFound':1,'start':0,numFoundExact:true,'docs':[{'id':'5'}]}}]}}",
        "/facet_counts=={'facet_queries':{'LW1':2,'LM1':2,'LM3':2},'facet_fields':{}," + EMPTY_FACETS + "}"
    );
  }

  static String f = "foo_i";
  static String f2 = "foo2_i";

  public static void createIndex() {
    assertU(adoc("id","1", f,"5",  f2,"4"));
    assertU(adoc("id","2", f,"4",  f2,"2"));
    assertU(adoc("id","3", f,"3",  f2,"7"));
    assertU(commit());
    assertU(adoc("id","4", f,"2",  f2,"6"));
    assertU(adoc("id","5", f,"1",  f2,"2"));
    assertU(adoc("id","6", f,"3",  f2,"2"));
    assertU(adoc("id","7", f,"2",  f2,"3"));
    assertU(commit());
    assertU(adoc("id","8", f,"1",  f2,"10"));
    assertU(adoc("id","9", f,"2",  f2,"1"));
    assertU(commit());    
    assertU(adoc("id","10", f,"1", f2,"3"));
    assertU(commit());
  }

  @Test
  public void testGroupAPI() throws Exception {
    createIndex();
    String filt = f + ":[* TO *]";

    assertQ(req("fq",filt,  "q","{!func}"+f2, "group","true", "group.field",f)
        ,"/response/lst[@name='grouped']/lst[@name='"+f+"']/arr[@name='groups']"
    );

    assertJQ(req("fq",filt,  "q","{!func}"+f2, "group","true", "group.field",f, "fl","id")
      ,"/responseHeader/status==0"                         // exact match
      ,"/responseHeader=={'_SKIP_':'QTime', 'status':0}"   // partial match by skipping some elements
      ,"/responseHeader=={'_MATCH_':'status', 'status':0}" // partial match by only including some elements
      ,"/grouped=={'"+f+"':{'matches':10,'groups':[\n" +
              "{'groupValue':1,'doclist':{'numFound':3,'start':0,numFoundExact:true,'docs':[{'id':'8'}]}}," +
              "{'groupValue':3,'doclist':{'numFound':2,'start':0,numFoundExact:true,'docs':[{'id':'3'}]}}," +
              "{'groupValue':2,'doclist':{'numFound':3,'start':0,numFoundExact:true,'docs':[{'id':'4'}]}}," +
              "{'groupValue':5,'doclist':{'numFound':1,'start':0,numFoundExact:true,'docs':[{'id':'1'}]}}," +
              "{'groupValue':4,'doclist':{'numFound':1,'start':0,numFoundExact:true,'docs':[{'id':'2'}]}}" +
            "]}}"
    );

    // test that filtering cuts down the result set
    assertJQ(req("fq",filt,  "q","{!func}"+f2, "group","true", "group.field",f, "fl","id", "fq",f+":2")
      ,"/grouped=={'"+f+"':{'matches':3,'groups':[" +
            "{'groupValue':2,'doclist':{'numFound':3,'start':0,numFoundExact:true,'docs':[{'id':'4'}]}}" +
            "]}}"
    );

    // test limiting the number of groups returned
    assertJQ(req("fq",filt,  "q","{!func}"+f2, "group","true", "group.field",f, "fl","id", "rows","2")
      ,"/grouped=={'"+f+"':{'matches':10,'groups':[" +
              "{'groupValue':1,'doclist':{'numFound':3,'start':0,numFoundExact:true,'docs':[{'id':'8'}]}}," +
              "{'groupValue':3,'doclist':{'numFound':2,'start':0,numFoundExact:true,'docs':[{'id':'3'}]}}" +
            "]}}"
    );

    // test offset into group list
    assertJQ(req("fq",filt,  "q","{!func}"+f2, "group","true", "group.field",f, "fl","id", "rows","1", "start","1")
      ,"/grouped=={'"+f+"':{'matches':10,'groups':[" +
              "{'groupValue':3,'doclist':{'numFound':2,'start':0,numFoundExact:true,'docs':[{'id':'3'}]}}" +
            "]}}"
    );

    // test big offset into group list
     assertJQ(req("fq",filt,  "q","{!func}"+f2, "group","true", "group.field",f, "fl","id", "rows","1", "start","100")
      ,"/grouped=={'"+f+"':{'matches':10,'groups':[" +
            "]}}"
    );

    // test increasing the docs per group returned
    assertJQ(req("fq",filt,  "q","{!func}"+f2, "group","true", "group.field",f, "fl","id", "rows","2", "group.limit","3")
      ,"/grouped=={'"+f+"':{'matches':10,'groups':[" +
            "{'groupValue':1,'doclist':{'numFound':3,'start':0,numFoundExact:true,'docs':[{'id':'8'},{'id':'10'},{'id':'5'}]}}," +
            "{'groupValue':3,'doclist':{'numFound':2,'start':0,numFoundExact:true,'docs':[{'id':'3'},{'id':'6'}]}}" +
          "]}}"
    );

    // test offset into each group
    assertJQ(req("fq",filt,  "q","{!func}"+f2, "group","true", "group.field",f, "fl","id", "rows","2", "group.limit","3", "group.offset","1")
      ,"/grouped=={'"+f+"':{'matches':10,'groups':[" +
            "{'groupValue':1,'doclist':{'numFound':3,'start':1,'numFoundExact':true,'docs':[{'id':'10'},{'id':'5'}]}}," +
            "{'groupValue':3,'doclist':{'numFound':2,'start':1,'numFoundExact':true,'docs':[{'id':'6'}]}}" +
          "]}}"
    );

    // test big offset into each group
     assertJQ(req("fq",filt,  "q","{!func}"+f2, "group","true", "group.field",f, "fl","id", "rows","2", "group.limit","3", "group.offset","10")
      ,"/grouped=={'"+f+"':{'matches':10,'groups':[" +
            "{'groupValue':1,'doclist':{'numFound':3,'start':10,'numFoundExact':true,'docs':[]}}," +
            "{'groupValue':3,'doclist':{'numFound':2,'start':10,'numFoundExact':true,'docs':[]}}" +
          "]}}"
    );

    // test adding in scores
    assertJQ(req("fq",filt,  "q","{!func}"+f2, "group","true", "group.field",f, "fl","id,score", "rows","2", "group.limit","2", "indent","off")
      ,"/grouped/"+f+"/groups==" +
            "[" +
              "{'groupValue':1,'doclist':{'numFound':3,'start':0,numFoundExact:true,'maxScore':10.0,'docs':[{'id':'8','score':10.0},{'id':'10','score':3.0}]}}," +
              "{'groupValue':3,'doclist':{'numFound':2,'start':0,numFoundExact:true,'maxScore':7.0,'docs':[{'id':'3','score':7.0},{'id':'6','score':2.0}]}}" +
            "]"

    );

    // test function (functions are currently all float - this may change)
    String func = "add("+f+","+f+")";
    assertJQ(req("fq",filt,  "q","{!func}"+f2, "group","true", "group.func", func  , "fl","id", "rows","2")
      ,"/grouped=={'"+func+"':{'matches':10,'groups':[" +
              "{'groupValue':2.0,'doclist':{'numFound':3,'start':0,numFoundExact:true,'docs':[{'id':'8'}]}}," +
              "{'groupValue':6.0,'doclist':{'numFound':2,'start':0,numFoundExact:true,'docs':[{'id':'3'}]}}" +
            "]}}"
    );

    // test that faceting works with grouping
    assertJQ(req("fq",filt,  "q","{!func}"+f2, "group","true", "group.field",f, "fl","id"
                 ,"facet","true", "facet.field",f)
      ,"/grouped/"+f+"/matches==10"
      ,"/facet_counts/facet_fields/"+f+"==['1',3, '2',3, '3',2, '4',1, '5',1]"
    );

    // test that grouping works with debugging
    assertJQ(req("fq",filt,  "q","{!func}"+f2, "group","true", "group.field",f, "fl","id"
                 ,"debugQuery","true")
      ,"/grouped/"+f+"/matches==10"
      ,"/debug/explain/8=="
      ,"/debug/explain/2=="
    );

     ///////////////////////// group.query
    assertJQ(req("fq",filt,  "q","{!func}"+f2, "group","true", "group.query","id:[2 TO 5]", "fl","id", "group.limit","3")
       ,"/grouped=={'id:[2 TO 5]':{'matches':10," +
           "'doclist':{'numFound':4,'start':0,numFoundExact:true,'docs':[{'id':'3'},{'id':'4'},{'id':'2'}]}}}"
    );

    // group.query that matches nothing
    assertJQ(req("fq",filt,  
                 "q","{!func}"+f2, 
                 "group","true", 
                 "group.query","id:[2 TO 5]", 
                 "group.query","id:1000", 
                 "fl","id", 
                 "group.limit","3")
             ,"/grouped/id:[2 TO 5]=={'matches':10,'doclist':{'numFound':4,'start':0,numFoundExact:true,'docs':[{'id':'3'},{'id':'4'},{'id':'2'}]}}"
             ,"/grouped/id:1000=={'matches':10,'doclist':{'numFound':0,'start':0,numFoundExact:true,'docs':[]}}"
    );

    // group.query and sort
    assertJQ(req("fq",filt,  "q","{!func}"+f2, "group","true", "group.query",f+":1", "fl","id,score", "rows","2", "group.limit","2", "sort",f+" desc, score desc", "indent","off")
        ,"/grouped/"+f+":1==" +
            "{'matches':10,'doclist':{'numFound':3,'start':0,numFoundExact:true,'maxScore':10.0,'docs':[{'id':'8','score':10.0},{'id':'10','score':3.0}]}},"
    );
    // group.query with fl=score and default sort
    assertJQ(req("fq",filt,  "q","{!func}"+f2, "group","true", "group.query",f+":1", "fl","id,score", "rows","2", "group.limit","2", "sort", "score desc", "indent","off")
        ,"/grouped/"+f+":1==" +
            "{'matches':10,'doclist':{'numFound':3,'start':0,numFoundExact:true,'maxScore':10.0,'docs':[{'id':'8','score':10.0},{'id':'10','score':3.0}]}},"
    );
    assertJQ(req("fq",filt,  "q","{!func}"+f2, "group","true", "group.query",f+":1", "fl","id", "rows","2", "group.limit","2", "indent","off")
        ,"/grouped/"+f+":1==" +
            "{'matches':10,'doclist':{'numFound':3,'start':0,numFoundExact:true,'docs':[{'id':'8'},{'id':'10'}]}},"
    );

    // group.query and offset
    assertJQ(req("fq",filt,  "q","{!func}"+f2, "group","true", "group.query","id:[2 TO 5]", "fl","id", "group.limit","3", "group.offset","2")
       ,"/grouped=={'id:[2 TO 5]':{'matches':10," +
           "'doclist':{'numFound':4,'start':2,'numFoundExact':true,'docs':[{'id':'2'},{'id':'5'}]}}}"
    );

    // group.query and big offset
    assertJQ(req("fq",filt,  "q","{!func}"+f2, "group","true", "group.query","id:[2 TO 5]", "fl","id", "group.limit","3", "group.offset","10")
       ,"/grouped=={'id:[2 TO 5]':{'matches':10," +
           "'doclist':{'numFound':4,'start':10,'numFoundExact':true,'docs':[]}}}"
    );

    ///////////////////////// group.query as main result
    assertJQ(req("fq",filt,  "q","{!func}"+f2, "group","true", "group.query","id:[2 TO 5]", "fl","id", "rows","3", "group.main","true")
       ,"/response=={'numFound':4,'start':0,numFoundExact:true,'docs':[{'id':'3'},{'id':'4'},{'id':'2'}]}"
    );

    // group.query and offset
    assertJQ(req("fq",filt,  "q","{!func}"+f2, "group","true", "group.query","id:[2 TO 5]", "fl","id", "rows","3", "start","2", "group.main","true")
       ,"/response=={'numFound':4,'start':2,'numFoundExact':true,'docs':[{'id':'2'},{'id':'5'}]}"
    );

    // group.query and big offset
    assertJQ(req("fq",filt,  "q","{!func}"+f2, "group","true", "group.query","id:[2 TO 5]", "fl","id", "rows","3", "start","10", "group.main","true")
       ,"/response=={'numFound':4,'start':10,'numFoundExact':true,'docs':[]}"
    );


    // multiple at once
    assertJQ(req("fq",filt,  "q","{!func}"+f2, "group","true",
        "group.query","id:[2 TO 5]",
        "group.query","id:[5 TO 5]",
        "group.field",f,
        "rows","1",
        "fl","id", "group.limit","2")
       ,"/grouped/id:[2 TO 5]=={'matches':10,'doclist':{'numFound':4,'start':0,numFoundExact:true,'docs':[{'id':'3'},{'id':'4'}]}}"
       ,"/grouped/id:[5 TO 5]=={'matches':10,'doclist':{'numFound':1,'start':0,numFoundExact:true,'docs':[{'id':'5'}]}}"        
       ,"/grouped/"+f+"=={'matches':10,'groups':[{'groupValue':1,'doclist':{'numFound':3,'start':0,numFoundExact:true,'docs':[{'id':'8'},{'id':'10'}]}}]}"
    );


    ///////////////////////// group.field as main result
    assertJQ(req("fq",filt,  "q","{!func}"+f2, "group","true", "group.field",f, "fl","id", "group.main","true")
        ,"/response=={'numFound':10,'start':0,numFoundExact:true,'docs':[{'id':'8'},{'id':'3'},{'id':'4'},{'id':'1'},{'id':'2'}]}"
    );
    // test that rows limits #docs
    assertJQ(req("fq",filt,  "q","{!func}"+f2, "group","true", "group.field",f, "fl","id", "rows","3", "group.main","true")
        ,"/response=={'numFound':10,'start':0,numFoundExact:true,'docs':[{'id':'8'},{'id':'3'},{'id':'4'}]}"
    );
    // small  offset
    assertJQ(req("fq",filt,  "q","{!func}"+f2, "group","true", "group.field",f, "fl","id", "rows","2", "start","1", "group.main","true")
        ,"/response=={'numFound':10,'start':1,'numFoundExact':true,'docs':[{'id':'3'},{'id':'4'}]}"
    );
    // large offset
    assertJQ(req("fq",filt,  "q","{!func}"+f2, "group","true", "group.field",f, "fl","id", "rows","2", "start","20", "group.main","true")
        ,"/response=={'numFound':10,'start':20,'numFoundExact':true,'docs':[]}"
    );
    // group.limit>1
    assertJQ(req("fq",filt,  "q","{!func}"+f2, "group","true", "group.field",f, "fl","id", "rows","3", "group.limit","2", "group.main","true")
        ,"/response=={'numFound':10,'start':0,numFoundExact:true,'docs':[{'id':'8'},{'id':'10'},{'id':'3'}]}"
    );
    // group.limit>1 with start>0
    assertJQ(req("fq",filt,  "q","{!func}"+f2, "group","true", "group.field",f, "fl","id", "rows","3", "start","1", "group.limit","2", "group.main","true")
        ,"/response=={'numFound':10,'start':1,'numFoundExact':true,'docs':[{'id':'10'},{'id':'3'},{'id':'6'}]}"
    );

    ///////////////////////// group.format == simple
    assertJQ(req("fq", filt, "q", "{!func}" + f2, "group", "true", "group.field", f, "fl", "id", "rows", "3", "start", "1", "group.limit", "2", "group.format", "simple")
        , "/grouped/foo_i=={'matches':10,'doclist':"
        + "{'numFound':10,'start':1,'numFoundExact':true,'docs':[{'id':'10'},{'id':'3'},{'id':'6'}]}}"
    );

    //////////////////////// grouping where main query matches nothing
    assertJQ(req("fq", filt, "q", "bogus_s:nothing", "group", "true", "group.field", f, "fl", "id", "group.limit", "2", "group.format", "simple")
        , "/grouped/foo_i=={'matches':0,'doclist':{'numFound':0,'start':0,numFoundExact:true,'docs':[]}}"
    );
    assertJQ(req("fq",filt,  "q","bogus_s:nothing", "group","true",
        "group.query","id:[2 TO 5]",
        "group.query","id:[5 TO 5]",
        "group.field",f,
        "rows","1",
        "fl","id", "group.limit","2")
       ,"/grouped/id:[2 TO 5]=={'matches':0,'doclist':{'numFound':0,'start':0,numFoundExact:true,'docs':[]}}"
       ,"/grouped/id:[5 TO 5]=={'matches':0,'doclist':{'numFound':0,'start':0,numFoundExact:true,'docs':[]}}"        
       ,"/grouped/"+f+"=={'matches':0,'groups':[]}"
    );
    assertJQ(req("fq",filt,  
                 "q","bogus_s:nothing", 
                 "group","true", 
                 "group.query","id:[2 TO 5]", 
                 "group.query","id:1000", 
                 "fl","id", 
                 "group.limit","3")
             ,"/grouped/id:[2 TO 5]=={'matches':0,'doclist':{'numFound':0,'start':0,numFoundExact:true,'docs':[]}}"
             ,"/grouped/id:1000=={'matches':0,'doclist':{'numFound':0,'start':0,numFoundExact:true,'docs':[]}}"
    );

  }

  @Test
  public void testGroupingNonIndexedOrStoredDocValues() throws Exception {
    // test-case from SOLR-4647
    assertU(add(doc("id", "1", FOO_STRING_DOCVAL_FIELD, "a", "bday", "2012-11-20T00:00:00Z")));
    assertU(add(doc("id", "2", FOO_STRING_DOCVAL_FIELD, "b", "bday", "2012-11-21T00:00:00Z")));
    assertU(add(doc("id", "3", FOO_STRING_DOCVAL_FIELD, "a", "bday", "2012-11-20T00:00:00Z")));
    assertU(add(doc("id", "4", FOO_STRING_DOCVAL_FIELD, "b", "bday", "2013-01-15T00:00:00Z")));
    assertU(add(doc("id", "5", FOO_STRING_DOCVAL_FIELD, "a", "bday", "2013-01-14T00:00:00Z")));
    assertU(commit());

    // Facet counts based on groups
    SolrQueryRequest req = req("q", "*:*", "rows", "1", "group", "true", "group.field", FOO_STRING_DOCVAL_FIELD,
        "sort", FOO_STRING_DOCVAL_FIELD + " asc", "fl", "id",
        "fq", "{!tag=chk}bday:[2012-12-18T00:00:00Z TO 2013-01-17T23:59:59Z]",
        "facet", "true", "group.truncate", "true", "group.sort", "bday desc",
        "facet.query", "{!ex=chk key=LW1}bday:[2013-01-11T00:00:00Z TO 2013-01-17T23:59:59Z]",
        "facet.query", "{!ex=chk key=LM1}bday:[2012-12-18T00:00:00Z TO 2013-01-17T23:59:59Z]",
        "facet.query", "{!ex=chk key=LM3}bday:[2012-10-18T00:00:00Z TO 2013-01-17T23:59:59Z]");
    assertJQ(
        req,
        "/grouped=={'"+FOO_STRING_DOCVAL_FIELD+"':{'matches':2,'groups':[{'groupValue':'a','doclist':{'numFound':1,'start':0,numFoundExact:true,'docs':[{'id':'5'}]}}]}}",
        "/facet_counts=={'facet_queries':{'LW1':2,'LM1':2,'LM3':2},'facet_fields':{}," + EMPTY_FACETS + "}"
    );
  }

  @Test
  public void testGroupingOnDateField() throws Exception {
    assertU(add(doc("id", "1",  "date_dt", "2012-11-20T00:00:00Z")));
    assertU(add(doc("id", "2",  "date_dt", "2012-11-21T00:00:00Z")));
    assertU(commit());

    assertU(add(doc("id", "3",  "date_dt", "2012-11-20T00:00:00Z")));
    assertU(add(doc("id", "4",  "date_dt", "2013-01-15T00:00:00Z")));
    assertU(add(doc("id", "5")));
    assertU(commit());

    ModifiableSolrParams params = params("q", "*:*", "group.limit", "10",
        "group", "true", "fl", "id", "group.ngroups", "true");

    assertJQ(req(params, "group.field", "date_dt", "sort", "id asc"),
        "/grouped=={'date_dt':{'matches':5,'ngroups':4, 'groups':" +
            "[{'groupValue':'2012-11-20T00:00:00Z','doclist':{'numFound':2,'start':0,numFoundExact:true,'docs':[{'id':'1'},{'id':'3'}]}}," +
            "{'groupValue':'2012-11-21T00:00:00Z','doclist':{'numFound':1,'start':0,numFoundExact:true,'docs':[{'id':'2'}]}}," +
            "{'groupValue':'2013-01-15T00:00:00Z','doclist':{'numFound':1,'start':0,numFoundExact:true,'docs':[{'id':'4'}]}}," +
            "{'groupValue':null,'doclist':{'numFound':1,'start':0,numFoundExact:true,'docs':[{'id':'5'}]}}" +
            "]}}"
    );
  }

  @Test
  public void testRandomGrouping() throws Exception {
    /**
     updateJ("{\"add\":{\"doc\":{\"id\":\"77\"}}}", params("commit","true"));
     assertJQ(req("q","id:77"), "/response/numFound==1");

     Doc doc = createDocObj(types);
     updateJ(toJSON(doc), params("commit","true"));

     assertJQ(req("q","id:"+doc.id), "/response/numFound==1");
    **/

    int indexIter=atLeast(10);  // make >0 to enable test
    int queryIter=atLeast(50);

    while (--indexIter >= 0) {

      int indexSize = random().nextInt(25 * RANDOM_MULTIPLIER);
//indexSize=2;
      List<FldType> types = new ArrayList<>();
      types.add(new FldType("id",ONE_ONE, new SVal('A','Z',4,4)));
      types.add(new FldType("score_f",ONE_ONE, new FVal(1,100)));  // field used to score
      types.add(new FldType("foo_i",ZERO_ONE, new IRange(0,indexSize)));
      types.add(new FldType(FOO_STRING_FIELD,ONE_ONE, new SVal('a','z',1,2)));
      types.add(new FldType(SMALL_STRING_FIELD,ZERO_ONE, new SVal('a',(char)('c'+indexSize/10),1,1)));
      types.add(new FldType(SMALL_INT_FIELD,ZERO_ONE, new IRange(0,5+indexSize/10)));

      // non-stored non-indexed docValue enabled fields
      types.add(new FldType("score_ff",ONE_ONE, new FVal(1,100)));
      types.add(new FldType("foo_ii",ZERO_ONE, new IRange(0,indexSize)));
      types.add(new FldType(FOO_STRING_DOCVAL_FIELD,ONE_ONE, new SVal('a','z',3,7)));
      types.add(new FldType("foo_bdv", ZERO_ONE, new BVal()));

      clearIndex();
      @SuppressWarnings({"rawtypes"})
      Map<Comparable, Doc> model = indexDocs(types, null, indexSize);
      //System.out.println("############### model=" + model);

      // test with specific docs
      if (false) {
        clearIndex();
        model.clear();
        Doc d1 = createDoc(types);
        d1.getValues(SMALL_STRING_FIELD).set(0,"c");
        d1.getValues(SMALL_INT_FIELD).set(0,5);
        d1.order = 0;
        updateJ(toJSON(d1), params("commit","true"));
        model.put(d1.id, d1);

        d1 = createDoc(types);
        d1.getValues(SMALL_STRING_FIELD).set(0,"b");
        d1.getValues(SMALL_INT_FIELD).set(0,5);
        d1.order = 1;
        updateJ(toJSON(d1), params("commit","false"));
        model.put(d1.id, d1);

        d1 = createDoc(types);
        d1.getValues(SMALL_STRING_FIELD).set(0,"c");
        d1.getValues(SMALL_INT_FIELD).set(0,5);
        d1.order = 2;
        updateJ(toJSON(d1), params("commit","false"));
        model.put(d1.id, d1);

        d1 = createDoc(types);
        d1.getValues(SMALL_STRING_FIELD).set(0,"c");
        d1.getValues(SMALL_INT_FIELD).set(0,5);
        d1.order = 3;
        updateJ(toJSON(d1), params("commit","false"));
        model.put(d1.id, d1);

        d1 = createDoc(types);
        d1.getValues(SMALL_STRING_FIELD).set(0,"b");
        d1.getValues(SMALL_INT_FIELD).set(0,2);
        d1.order = 4;
        updateJ(toJSON(d1), params("commit","true"));
        model.put(d1.id, d1);
      }


      for (int qiter=0; qiter<queryIter; qiter++) {
        String groupField = types.get(random().nextInt(types.size())).fname;

        int rows = random().nextInt(10)==0 ? random().nextInt(model.size()+2) : random().nextInt(11)-1;
        int start = random().nextInt(5)==0 ? random().nextInt(model.size()+2) : random().nextInt(5); // pick a small start normally for better coverage
        int group_limit = random().nextInt(10)==0 ? random().nextInt(model.size()+2) : random().nextInt(11)-1;    
        int group_offset = random().nextInt(10)==0 ? random().nextInt(model.size()+2) : random().nextInt(2); // pick a small start normally for better coverage

        IndexSchema schema = h.getCore().getLatestSchema();
        
        String[] stringSortA = new String[1];
        Comparator<Doc> sortComparator = createSort(schema, types, stringSortA);
        String sortStr = stringSortA[0];
        Comparator<Doc> groupComparator = random().nextBoolean() ? sortComparator : createSort(schema, types, stringSortA);
        String groupSortStr = stringSortA[0];

        // since groupSortStr defaults to sortStr, we need to normalize null to "score desc" if
        // sortStr != null.
        if (groupSortStr == null && groupSortStr != sortStr) {
          groupSortStr = "score desc";
        }
        
         // Test specific case
        if (false) {
          groupField=SMALL_INT_FIELD;
          sortComparator=createComparator(Arrays.asList(createComparator(SMALL_STRING_FIELD, true, true, false, true)));
          sortStr = SMALL_STRING_FIELD + " asc";
          groupComparator = createComparator(Arrays.asList(createComparator(SMALL_STRING_FIELD, true, true, false, false)));
          groupSortStr = SMALL_STRING_FIELD + " asc";
          rows=1; start=0; group_offset=1; group_limit=1;
        }

        @SuppressWarnings({"rawtypes"})
        Map<Comparable, Grp> groups = groupBy(model.values(), groupField);

        // first sort the docs in each group
        for (Grp grp : groups.values()) {
          Collections.sort(grp.docs, groupComparator);
        }

        // now sort the groups

        // if sort != group.sort, we need to find the max doc by "sort"
        if (groupComparator != sortComparator) {
          for (Grp grp : groups.values()) grp.setMaxDoc(sortComparator); 
        }

        List<Grp> sortedGroups = new ArrayList<>(groups.values());
        Collections.sort(sortedGroups,  groupComparator==sortComparator ? createFirstDocComparator(sortComparator) : createMaxDocComparator(sortComparator));

        boolean includeNGroups = random().nextBoolean();
        Object modelResponse = buildGroupedResult(schema, sortedGroups, start, rows, group_offset, group_limit, includeNGroups);

        boolean truncateGroups = random().nextBoolean();
        Map<String, Integer> facetCounts = new TreeMap<>();
        if (truncateGroups) {
          for (Grp grp : sortedGroups) {
            Doc doc = grp.docs.get(0);
            if (doc.getValues(FOO_STRING_FIELD) == null) {
              continue;
            }

            String key = doc.getFirstValue(FOO_STRING_FIELD).toString();
            boolean exists = facetCounts.containsKey(key);
            int count = exists ? facetCounts.get(key) : 0;
            facetCounts.put(key, ++count);
          }
        } else {
          for (Doc doc : model.values()) {
            if (doc.getValues(FOO_STRING_FIELD) == null) {
              continue;
            }

            for (@SuppressWarnings({"rawtypes"})Comparable field : doc.getValues(FOO_STRING_FIELD)) {
              String key = field.toString();
              boolean exists = facetCounts.containsKey(key);
              int count = exists ? facetCounts.get(key) : 0;
              facetCounts.put(key, ++count);
            }
          }
        }
        @SuppressWarnings({"rawtypes"})
        List<Comparable> expectedFacetResponse = new ArrayList<>();
        for (Map.Entry<String, Integer> stringIntegerEntry : facetCounts.entrySet()) {
          expectedFacetResponse.add(stringIntegerEntry.getKey());
          expectedFacetResponse.add(stringIntegerEntry.getValue());
        }

        int randomPercentage = random().nextInt(101);
        // TODO: create a random filter too
        SolrQueryRequest req = req("group","true","wt","json","indent","true", "echoParams","all", "q","{!func}score_f", "group.field",groupField
            ,sortStr==null ? "nosort":"sort", sortStr ==null ? "": sortStr, "fl", "*,score_ff,foo_ii,foo_bdv," + FOO_STRING_DOCVAL_FIELD // only docValued fields are not returned by default
            ,(groupSortStr == null || groupSortStr == sortStr) ? "noGroupsort":"group.sort", groupSortStr==null ? "": groupSortStr
            ,"rows",""+rows, "start",""+start, "group.offset",""+group_offset, "group.limit",""+group_limit,
            GroupParams.GROUP_CACHE_PERCENTAGE, Integer.toString(randomPercentage), GroupParams.GROUP_TOTAL_COUNT, includeNGroups ? "true" : "false",
            "facet", "true", "facet.sort", "index", "facet.limit", "-1", "facet.field", FOO_STRING_FIELD,
            GroupParams.GROUP_TRUNCATE, truncateGroups ? "true" : "false", "facet.mincount", "1", "facet.method", "fcs" // to avoid FC insanity
        );

        String strResponse = h.query(req);

        Object realResponse = Utils.fromJSONString(strResponse);
        String err = JSONTestUtil.matchObj("/grouped/" + groupField, realResponse, modelResponse);
        if (err != null) {
          log.error("GROUPING MISMATCH ({}}): {}\n\trequest={}\n\tresult={}\n\texpected={}\n\tsorted_model={}"
              , queryIter, err, req, strResponse, Utils.toJSONString(modelResponse), sortedGroups);

          // re-execute the request... good for putting a breakpoint here for debugging
          String rsp = h.query(req);

          fail(err);
        }

        // assert post / pre grouping facets
        err = JSONTestUtil.matchObj("/facet_counts/facet_fields/"+FOO_STRING_FIELD, realResponse, expectedFacetResponse);
        if (err != null) {
          log.error("GROUPING MISMATCH ({}): {}\n\trequest={}\n\tresult={}\n\texpected={}"
              , queryIter, err, req, strResponse, Utils.toJSONString(expectedFacetResponse));
          // re-execute the request... good for putting a breakpoint here for debugging
          h.query(req);
          fail(err);
        }
      } // end query iter
    } // end index iter

  }
  
  @Test
  public void testGroupWithMinExactHitCount() throws Exception {
    final int NUM_DOCS = 20;
    for (int i = 0; i < NUM_DOCS ; i++) {
      assertU(adoc("id", String.valueOf(i), FOO_STRING_FIELD, "Book1"));
      assertU(commit());
    }
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("q", FOO_STRING_FIELD + ":Book1");
    assertQ(req(params, CommonParams.MIN_EXACT_COUNT, "2", CommonParams.ROWS, "2")
        ,"/response/result[@numFoundExact='false']"
    );
    params.set("group", true);
    params.set("group.field", FOO_STRING_FIELD);
    assertQ(req(params)
        ,"/response/lst[@name='grouped']/lst[@name='"+FOO_STRING_FIELD+"']/arr[@name='groups']/lst[1]/result[@numFoundExact='true']"
    );
    
    assertQ(req(params, CommonParams.MIN_EXACT_COUNT, "2", CommonParams.ROWS, "2")
        ,"/response/lst[@name='grouped']/lst[@name='"+FOO_STRING_FIELD+"']/arr[@name='groups']/lst[1]/result[@numFoundExact='true']"
    );
    
    
  }

  public static Object buildGroupedResult(IndexSchema schema, List<Grp> sortedGroups, int start, int rows, int group_offset, int group_limit, boolean includeNGroups) {
    Map<String,Object> result = new LinkedHashMap<>();

    long matches = 0;
    for (Grp grp : sortedGroups) {
      matches += grp.docs.size();
    }
    result.put("matches", matches);
    if (includeNGroups) {
      result.put("ngroups", sortedGroups.size());
    }
    List<Map<String,Object>> groupList = new ArrayList<>();
    result.put("groups", groupList);

    for (int i=start; i<sortedGroups.size(); i++) {
      if (rows != -1 && groupList.size() >= rows) break;  // directly test rather than calculating, so we can catch any calc errors in the real code
      Map<String,Object> group = new LinkedHashMap<>();
      groupList.add(group);

      Grp grp = sortedGroups.get(i);
      group.put("groupValue", grp.groupValue);

      Map<String,Object> resultSet = new LinkedHashMap<>();
      group.put("doclist", resultSet);
      resultSet.put("numFound", grp.docs.size());
      resultSet.put("start", group_offset);
      resultSet.put("numFoundExact", true);
      List<Map<String,Object>> docs = new ArrayList<>();
      resultSet.put("docs", docs);
      for (int j=group_offset; j<grp.docs.size(); j++) {
        if (group_limit != -1 && docs.size() >= group_limit) break;
        docs.add( grp.docs.get(j).toObject(schema) );
      }
    }

    return result;
  }


  public static Comparator<Grp> createMaxDocComparator(final Comparator<Doc> docComparator) {
    return (o1, o2) -> {
      // all groups should have at least one doc
      Doc d1 = o1.maxDoc;
      Doc d2 = o2.maxDoc;
      return docComparator.compare(d1, d2);
    };
  }

  public static Comparator<Grp> createFirstDocComparator(final Comparator<Doc> docComparator) {
    return (o1, o2) -> {
      // all groups should have at least one doc
      Doc d1 = o1.docs.get(0);
      Doc d2 = o2.docs.get(0);
      return docComparator.compare(d1, d2);
    };
  }

  @SuppressWarnings({"rawtypes"})
  public static Map<Comparable, Grp> groupBy(Collection<Doc> docs, String field) {
    Map<Comparable, Grp> groups = new HashMap<>();
    for (Doc doc : docs) {
      List<Comparable> vals = doc.getValues(field);
      if (vals == null) {
        Grp grp = groups.get(null);
        if (grp == null) {
          grp = new Grp();
          grp.groupValue = null;
          grp.docs = new ArrayList<>();
          groups.put(null, grp);
        }
        grp.docs.add(doc);
      } else {
        for (Comparable val : vals) {

          Grp grp = groups.get(val);
          if (grp == null) {
            grp = new Grp();
            grp.groupValue = val;
            grp.docs = new ArrayList<>();
            groups.put(grp.groupValue, grp);
          }
          grp.docs.add(doc);
        }
      }
    }
    return groups;
  }


  public static class Grp {
    @SuppressWarnings({"rawtypes"})
    public Comparable groupValue;
    public List<Doc> docs;
    public Doc maxDoc;  // the document highest according to the "sort" param


    public void setMaxDoc(Comparator<Doc> comparator) {
      Doc[] arr = docs.toArray(new Doc[docs.size()]);
      Arrays.sort(arr, comparator);
      maxDoc = arr.length > 0 ? arr[0] : null;
    }

    @Override
    public String toString() {
      return "{groupValue="+groupValue+",docs="+docs+"}";
    }
  }
}



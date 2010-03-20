package org.apache.solr.handler.component;
/**
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
import org.apache.solr.core.SolrCore;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.TermsParams;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.request.SolrRequestHandler;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.*;
import java.util.regex.Pattern;

/**
 *
 *
 **/
public class TermsComponentTest extends SolrTestCaseJ4 {

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig.xml","schema.xml");

    assertNull(h.validateUpdate(adoc("id", "0", "lowerfilt", "a", "standardfilt", "a", "foo_i","1")));
    assertNull(h.validateUpdate(adoc("id", "1", "lowerfilt", "a", "standardfilt", "aa", "foo_i","1")));
    assertNull(h.validateUpdate(adoc("id", "2", "lowerfilt", "aa", "standardfilt", "aaa", "foo_i","2")));
    assertNull(h.validateUpdate(adoc("id", "3", "lowerfilt", "aaa", "standardfilt", "abbb")));
    assertNull(h.validateUpdate(adoc("id", "4", "lowerfilt", "ab", "standardfilt", "b")));
    assertNull(h.validateUpdate(adoc("id", "5", "lowerfilt", "abb", "standardfilt", "bb")));
    assertNull(h.validateUpdate(adoc("id", "6", "lowerfilt", "abc", "standardfilt", "bbbb")));
    assertNull(h.validateUpdate(adoc("id", "7", "lowerfilt", "b", "standardfilt", "c")));
    assertNull(h.validateUpdate(adoc("id", "8", "lowerfilt", "baa", "standardfilt", "cccc")));
    assertNull(h.validateUpdate(adoc("id", "9", "lowerfilt", "bbb", "standardfilt", "ccccc")));

    assertNull(h.validateUpdate(adoc("id", "10", "standardfilt", "ddddd")));
    assertNull(h.validateUpdate(adoc("id", "11", "standardfilt", "ddddd")));
    assertNull(h.validateUpdate(adoc("id", "12", "standardfilt", "ddddd")));
    assertNull(h.validateUpdate(adoc("id", "13", "standardfilt", "ddddd")));
    assertNull(h.validateUpdate(adoc("id", "14", "standardfilt", "d")));
    assertNull(h.validateUpdate(adoc("id", "15", "standardfilt", "d")));
    assertNull(h.validateUpdate(adoc("id", "16", "standardfilt", "d")));

    assertNull(h.validateUpdate(adoc("id", "17", "standardfilt", "snake")));
    assertNull(h.validateUpdate(adoc("id", "18", "standardfilt", "spider")));
    assertNull(h.validateUpdate(adoc("id", "19", "standardfilt", "shark")));
    assertNull(h.validateUpdate(adoc("id", "20", "standardfilt", "snake")));
    assertNull(h.validateUpdate(adoc("id", "21", "standardfilt", "snake")));
    assertNull(h.validateUpdate(adoc("id", "22", "standardfilt", "shark")));
    
    assertNull(h.validateUpdate(commit()));
  }

  @Test
  public void testEmptyLower() throws Exception {
    SolrCore core = h.getCore();
    TermsComponent tc = (TermsComponent) core.getSearchComponent("termsComp");
    assertTrue("tc is null and it shouldn't be", tc != null);

    ModifiableSolrParams params = new ModifiableSolrParams();
    params.add(TermsParams.TERMS, "true");
    params.add(TermsParams.TERMS_FIELD, "lowerfilt");
    //no lower bound
    params.add(TermsParams.TERMS_UPPER, "b");
    params.add(TermsParams.TERMS_LIMIT, String.valueOf(50));
    SolrRequestHandler handler;
    SolrQueryResponse rsp;
    NamedList values;
    NamedList terms;
    handler = core.getRequestHandler("/terms");
    assertTrue("handler is null and it shouldn't be", handler != null);
    rsp = new SolrQueryResponse();
    rsp.add("responseHeader", new SimpleOrderedMap());
    handler.handleRequest(new LocalSolrQueryRequest(core, params), rsp);
    values = rsp.getValues();
    terms = (NamedList) ((NamedList) values.get("terms")).get("lowerfilt");

    assertTrue("terms Size: " + terms.size() + " is not: " + 6, terms.size() == 6);
    assertTrue("a is null and it shouldn't be", terms.get("a") != null);
    assertTrue("aa is null and it shouldn't be", terms.get("aa") != null);
    assertTrue("aaa is null and it shouldn't be", terms.get("aaa") != null);
    assertTrue("ab is null and it shouldn't be", terms.get("ab") != null);
    assertTrue("abb is null and it shouldn't be", terms.get("abb") != null);
    assertTrue("abc is null and it shouldn't be", terms.get("abc") != null);
  }

  @Test
  public void testNoField() throws Exception {
    SolrCore core = h.getCore();
    TermsComponent tc = (TermsComponent) core.getSearchComponent("termsComp");
    assertTrue("tc is null and it shouldn't be", tc != null);

    ModifiableSolrParams params = new ModifiableSolrParams();
    params.add(TermsParams.TERMS, "true");
    //no lower bound
    params.add(TermsParams.TERMS_LOWER, "d");
    params.add(TermsParams.TERMS_LIMIT, String.valueOf(50));
    SolrRequestHandler handler;
    SolrQueryResponse rsp;

    handler = core.getRequestHandler("/terms");
    assertTrue("handler is null and it shouldn't be", handler != null);
    rsp = new SolrQueryResponse();
    rsp.add("responseHeader", new SimpleOrderedMap());
    handler.handleRequest(new LocalSolrQueryRequest(core, params), rsp);
    Exception exception = rsp.getException();
    assertTrue("exception is null and it shouldn't be", exception != null);
  }

  @Test
  public void testMultipleFields() throws Exception {
    SolrCore core = h.getCore();
    TermsComponent tc = (TermsComponent) core.getSearchComponent("termsComp");
    assertTrue("tc is null and it shouldn't be", tc != null);

    ModifiableSolrParams params = new ModifiableSolrParams();
    params.add(TermsParams.TERMS, "true");
    params.add(TermsParams.TERMS_FIELD, "lowerfilt", "standardfilt");
    //no lower bound
    params.add(TermsParams.TERMS_UPPER, "b");
    params.add(TermsParams.TERMS_LIMIT, String.valueOf(50));
    SolrRequestHandler handler;
    SolrQueryResponse rsp;
    NamedList values;
    NamedList terms;
    handler = core.getRequestHandler("/terms");
    assertTrue("handler is null and it shouldn't be", handler != null);
    rsp = new SolrQueryResponse();
    rsp.add("responseHeader", new SimpleOrderedMap());
    handler.handleRequest(new LocalSolrQueryRequest(core, params), rsp);
    values = rsp.getValues();
    NamedList tmp = (NamedList) values.get("terms");
    assertTrue("tmp Size: " + tmp.size() + " is not: " + 2, tmp.size() == 2);
    terms = (NamedList) tmp.get("lowerfilt");
    assertTrue("terms Size: " + terms.size() + " is not: " + 6, terms.size() == 6);
    terms = (NamedList) tmp.get("standardfilt");
    assertTrue("terms Size: " + terms.size() + " is not: " + 4, terms.size() == 4);
  }

  @Test
  public void testUnlimitedRows() throws Exception {
    SolrCore core = h.getCore();
    TermsComponent tc = (TermsComponent) core.getSearchComponent("termsComp");
    assertTrue("tc is null and it shouldn't be", tc != null);

    ModifiableSolrParams params = new ModifiableSolrParams();
    params.add(TermsParams.TERMS, "true");
    params.add(TermsParams.TERMS_FIELD, "lowerfilt", "standardfilt");
    //no lower bound, upper bound or rows
    params.add(TermsParams.TERMS_LIMIT, String.valueOf(-1));
    SolrRequestHandler handler;
    SolrQueryResponse rsp;
    NamedList values;
    NamedList terms;
    handler = core.getRequestHandler("/terms");
    assertTrue("handler is null and it shouldn't be", handler != null);
    rsp = new SolrQueryResponse();
    rsp.add("responseHeader", new SimpleOrderedMap());
    handler.handleRequest(new LocalSolrQueryRequest(core, params), rsp);
    values = rsp.getValues();
    terms = (NamedList) ((NamedList) values.get("terms")).get("lowerfilt");
    assertTrue("terms Size: " + terms.size() + " is not: " + 9, terms.size() == 9);

  }

  @Test
  public void testPrefix() throws Exception {
    SolrCore core = h.getCore();
    TermsComponent tc = (TermsComponent) core.getSearchComponent("termsComp");
    assertTrue("tc is null and it shouldn't be", tc != null);

    ModifiableSolrParams params = new ModifiableSolrParams();
    params.add(TermsParams.TERMS, "true");
    params.add(TermsParams.TERMS_FIELD, "lowerfilt", "standardfilt");
    params.add(TermsParams.TERMS_LOWER,  "aa");
    params.add(TermsParams.TERMS_LOWER_INCLUSIVE, "false");
    params.add(TermsParams.TERMS_PREFIX_STR, "aa");
    params.add(TermsParams.TERMS_UPPER, "b");
    params.add(TermsParams.TERMS_LIMIT, String.valueOf(50));
    SolrRequestHandler handler;
    SolrQueryResponse rsp;
    NamedList values;
    NamedList terms;
    handler = core.getRequestHandler("/terms");
    assertTrue("handler is null and it shouldn't be", handler != null);
    rsp = new SolrQueryResponse();
    rsp.add("responseHeader", new SimpleOrderedMap());
    handler.handleRequest(new LocalSolrQueryRequest(core, params), rsp);
    values = rsp.getValues();
    terms = (NamedList) ((NamedList) values.get("terms")).get("lowerfilt");
    assertTrue("terms Size: " + terms.size() + " is not: " + 1, terms.size() == 1);
    Object value = terms.get("aaa");
    assertTrue("value is null and it shouldn't be", value != null);
  }

  @Test
  public void testRegexp() throws Exception {
    SolrCore core = h.getCore();
    TermsComponent tc = (TermsComponent) core.getSearchComponent("termsComp");
    assertTrue("tc is null and it shouldn't be", tc != null);

    ModifiableSolrParams params = new ModifiableSolrParams();
    params.add(TermsParams.TERMS, "true");
    params.add(TermsParams.TERMS_FIELD, "standardfilt");
    params.add(TermsParams.TERMS_LOWER,  "bb");
    params.add(TermsParams.TERMS_LOWER_INCLUSIVE, "false");
    params.add(TermsParams.TERMS_REGEXP_STR, "b.*");
    params.add(TermsParams.TERMS_UPPER, "bbbb");
    params.add(TermsParams.TERMS_UPPER_INCLUSIVE, "true");
    params.add(TermsParams.TERMS_LIMIT, String.valueOf(50));
    SolrRequestHandler handler;
    SolrQueryResponse rsp;
    NamedList values;
    NamedList terms;
    handler = core.getRequestHandler("/terms");
    assertTrue("handler is null and it shouldn't be", handler != null);
    rsp = new SolrQueryResponse();
    rsp.add("responseHeader", new SimpleOrderedMap());
    handler.handleRequest(new LocalSolrQueryRequest(core, params), rsp);
    values = rsp.getValues();
    terms = (NamedList) ((NamedList) values.get("terms")).get("standardfilt");
    assertEquals("terms Size: " + terms.size() + " is not: 1", 1, terms.size());
  }

  @Test
  public void testRegexpFlagParsing() {
      ModifiableSolrParams params = new ModifiableSolrParams();
      params.add(TermsParams.TERMS_REGEXP_FLAG, "case_insensitive", "literal", "comments", "multiline", "unix_lines",
              "unicode_case", "dotall", "canon_eq");
      int flags = new TermsComponent().resolveRegexpFlags(params);
      int expected = Pattern.CASE_INSENSITIVE | Pattern.LITERAL | Pattern.COMMENTS | Pattern.MULTILINE | Pattern.UNIX_LINES
              | Pattern.UNICODE_CASE | Pattern.DOTALL | Pattern.CANON_EQ;
      assertEquals(expected, flags);
  }

  @Test
  public void testRegexpWithFlags() throws Exception {
    SolrCore core = h.getCore();
    TermsComponent tc = (TermsComponent) core.getSearchComponent("termsComp");
    assertTrue("tc is null and it shouldn't be", tc != null);

    ModifiableSolrParams params = new ModifiableSolrParams();
    params.add(TermsParams.TERMS, "true");
    params.add(TermsParams.TERMS_FIELD, "standardfilt");
    params.add(TermsParams.TERMS_LOWER,  "bb");
    params.add(TermsParams.TERMS_LOWER_INCLUSIVE, "false");
    params.add(TermsParams.TERMS_REGEXP_STR, "B.*");
    params.add(TermsParams.TERMS_REGEXP_FLAG, "case_insensitive");
    params.add(TermsParams.TERMS_UPPER, "bbbb");
    params.add(TermsParams.TERMS_UPPER_INCLUSIVE, "true");
    params.add(TermsParams.TERMS_LIMIT, String.valueOf(50));
    SolrRequestHandler handler;
    SolrQueryResponse rsp;
    NamedList values;
    NamedList terms;
    handler = core.getRequestHandler("/terms");
    assertTrue("handler is null and it shouldn't be", handler != null);
    rsp = new SolrQueryResponse();
    rsp.add("responseHeader", new SimpleOrderedMap());
    handler.handleRequest(new LocalSolrQueryRequest(core, params), rsp);
    values = rsp.getValues();
    terms = (NamedList) ((NamedList) values.get("terms")).get("standardfilt");
    assertEquals("terms Size: " + terms.size() + " is not: 1", 1, terms.size());
  }

  @Test
  public void testSortCount() throws Exception {
    SolrCore core = h.getCore();
    TermsComponent tc = (TermsComponent) core.getSearchComponent("termsComp");
    assertTrue("tc is null and it shouldn't be", tc != null);

    ModifiableSolrParams params = new ModifiableSolrParams();
    params.add(TermsParams.TERMS, "true");
    params.add(TermsParams.TERMS_FIELD, "standardfilt");
    params.add(TermsParams.TERMS_LOWER,  "s");
    params.add(TermsParams.TERMS_LOWER_INCLUSIVE, "false");
    params.add(TermsParams.TERMS_PREFIX_STR, "s");
    params.add(TermsParams.TERMS_SORT, TermsParams.TERMS_SORT_COUNT);
    
    SolrRequestHandler handler;
    SolrQueryResponse rsp;
    NamedList values;
    NamedList terms;
    handler = core.getRequestHandler("/terms");
    assertTrue("handler is null and it shouldn't be", handler != null);
    rsp = new SolrQueryResponse();
    rsp.add("responseHeader", new SimpleOrderedMap());
    handler.handleRequest(new LocalSolrQueryRequest(core, params), rsp);
    values = rsp.getValues();
    terms = (NamedList) ((NamedList) values.get("terms")).get("standardfilt");
    assertTrue("terms Size: " + terms.size() + " is not: " + 3, terms.size() == 3);
    assertTrue("Item 0 name is not 'snake'", terms.getName(0).equals("snake"));
    assertTrue("Item 0 frequency is not '3'", (Integer) terms.getVal(0) == 3);
    assertTrue("Item 1 name is not 'shark'", terms.getName(1).equals("shark"));
    assertTrue("Item 1 frequency is not '2'", (Integer) terms.getVal(1) == 2);
    assertTrue("Item 2 name is not 'spider'", terms.getName(2).equals("spider"));
    assertTrue("Item 2 frequency is not '1'", (Integer) terms.getVal(2) == 1);    
  }

  @Test
  public void testSortIndex() throws Exception {
    SolrCore core = h.getCore();
    TermsComponent tc = (TermsComponent) core.getSearchComponent("termsComp");
    assertTrue("tc is null and it shouldn't be", tc != null);

    ModifiableSolrParams params = new ModifiableSolrParams();
    params.add(TermsParams.TERMS, "true");
    params.add(TermsParams.TERMS_FIELD, "standardfilt");
    params.add(TermsParams.TERMS_LOWER,  "s");
    params.add(TermsParams.TERMS_LOWER_INCLUSIVE, "false");
    params.add(TermsParams.TERMS_PREFIX_STR, "s");
    params.add(TermsParams.TERMS_SORT, TermsParams.TERMS_SORT_INDEX);
    
    SolrRequestHandler handler;
    SolrQueryResponse rsp;
    NamedList values;
    NamedList terms;
    handler = core.getRequestHandler("/terms");
    assertTrue("handler is null and it shouldn't be", handler != null);
    rsp = new SolrQueryResponse();
    rsp.add("responseHeader", new SimpleOrderedMap());
    handler.handleRequest(new LocalSolrQueryRequest(core, params), rsp);
    values = rsp.getValues();
    terms = (NamedList) ((NamedList) values.get("terms")).get("standardfilt");
    assertTrue("terms Size: " + terms.size() + " is not: " + 3, terms.size() == 3);
    assertTrue("Item 0 name is not 'shark' it is " + terms.getName(0), terms.getName(0).equals("shark"));
    assertTrue("Item 0 frequency is not '2'", (Integer) terms.getVal(0) == 2);
    assertTrue("Item 1 name is not 'snake', it is " + terms.getName(1), terms.getName(1).equals("snake"));
    assertTrue("Item 1 frequency is not '3'", (Integer) terms.getVal(1) == 3);
    assertTrue("Item 2 name is not 'spider', it is " + terms.getName(2), terms.getName(2).equals("spider"));
    assertTrue("Item 2 frequency is not '1'", (Integer) terms.getVal(2) == 1);    
  }
  
  @Test
  public void testPastUpper() throws Exception {
    SolrCore core = h.getCore();
    TermsComponent tc = (TermsComponent) core.getSearchComponent("termsComp");
    assertTrue("tc is null and it shouldn't be", tc != null);

    ModifiableSolrParams params = new ModifiableSolrParams();
    params.add(TermsParams.TERMS, "true");
    params.add(TermsParams.TERMS_FIELD, "lowerfilt");
    //no upper bound, lower bound doesn't exist
    params.add(TermsParams.TERMS_LOWER, "d");
    params.add(TermsParams.TERMS_LIMIT, String.valueOf(50));
    SolrRequestHandler handler;
    SolrQueryResponse rsp;
    NamedList values;
    NamedList terms;
    handler = core.getRequestHandler("/terms");
    assertTrue("handler is null and it shouldn't be", handler != null);
    rsp = new SolrQueryResponse();
    rsp.add("responseHeader", new SimpleOrderedMap());
    handler.handleRequest(new LocalSolrQueryRequest(core, params), rsp);
    values = rsp.getValues();
    terms = (NamedList) ((NamedList) values.get("terms")).get("lowerfilt");
    assertTrue("terms Size: " + terms.size() + " is not: " + 0, terms.size() == 0);
  }

  @Test
  public void testLowerExclusive() throws Exception {
    SolrCore core = h.getCore();
    TermsComponent tc = (TermsComponent) core.getSearchComponent("termsComp");
    assertTrue("tc is null and it shouldn't be", tc != null);
    //test where the lower is an actual term
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.add(TermsParams.TERMS, "true");
    params.add(TermsParams.TERMS_LOWER_INCLUSIVE, "false");
    params.add(TermsParams.TERMS_FIELD, "lowerfilt");
    params.add(TermsParams.TERMS_LOWER, "a");
    params.add(TermsParams.TERMS_UPPER, "b");
    params.add(TermsParams.TERMS_LIMIT, String.valueOf(50));
    SolrRequestHandler handler;
    SolrQueryResponse rsp;
    NamedList values;
    NamedList terms;
    handler = core.getRequestHandler("/terms");
    assertTrue("handler is null and it shouldn't be", handler != null);
    
    rsp = new SolrQueryResponse();
    rsp.add("responseHeader", new SimpleOrderedMap());
    handler.handleRequest(new LocalSolrQueryRequest(core, params), rsp);
    values = rsp.getValues();
    terms = (NamedList) ((NamedList) values.get("terms")).get("lowerfilt");
    assertTrue("terms Size: " + terms.size() + " is not: " + 5, terms.size() == 5);
    assertTrue("aa is null and it shouldn't be", terms.get("aa") != null);
    assertTrue("ab is null and it shouldn't be", terms.get("ab") != null);
    assertTrue("aaa is null and it shouldn't be", terms.get("aaa") != null);
    assertTrue("abb is null and it shouldn't be", terms.get("abb") != null);
    assertTrue("abc is null and it shouldn't be", terms.get("abc") != null);
    assertTrue("a is not null", terms.get("a") == null);
    assertTrue("baa is not null", terms.get("baa") == null);

    //test where the lower is not a term
    params = new ModifiableSolrParams();
    params.add(TermsParams.TERMS, "true");
    params.add(TermsParams.TERMS_LOWER_INCLUSIVE, "false");
    params.add(TermsParams.TERMS_FIELD, "standardfilt");
    params.add(TermsParams.TERMS_LOWER, "cc");
    params.add(TermsParams.TERMS_UPPER, "d");
    params.add(TermsParams.TERMS_LIMIT, String.valueOf(50));
    rsp = new SolrQueryResponse();
    rsp.add("responseHeader", new SimpleOrderedMap());
    handler.handleRequest(new LocalSolrQueryRequest(core, params), rsp);
    values = rsp.getValues();
    terms = (NamedList) ((NamedList) values.get("terms")).get("standardfilt");
    assertTrue("terms Size: " + terms.size() + " is not: " + 2, terms.size() == 2);
  }

  @Test
  public void test() throws Exception {
    SolrCore core = h.getCore();
    TermsComponent tc = (TermsComponent) core.getSearchComponent("termsComp");
    assertTrue("tc is null and it shouldn't be", tc != null);

    ModifiableSolrParams params = new ModifiableSolrParams();
    params.add(TermsParams.TERMS, "true");
    params.add(TermsParams.TERMS_FIELD, "lowerfilt");
    params.add(TermsParams.TERMS_LOWER, "a");
    params.add(TermsParams.TERMS_UPPER, "b");
    params.add(TermsParams.TERMS_LIMIT, String.valueOf(50));
    SolrRequestHandler handler;
    SolrQueryResponse rsp;
    NamedList values;
    NamedList terms;
    handler = core.getRequestHandler("/terms");
    assertTrue("handler is null and it shouldn't be", handler != null);
    rsp = new SolrQueryResponse();
    rsp.add("responseHeader", new SimpleOrderedMap());
    handler.handleRequest(new LocalSolrQueryRequest(core, params), rsp);
    values = rsp.getValues();
    terms = (NamedList) ((NamedList) values.get("terms")).get("lowerfilt");
    assertTrue("terms Size: " + terms.size() + " is not: " + 6, terms.size() == 6);
    assertTrue("aa is null and it shouldn't be", terms.get("aa") != null);
    assertTrue("aaa is null and it shouldn't be", terms.get("aaa") != null);
    assertTrue("ab is null and it shouldn't be", terms.get("ab") != null);
    assertTrue("abb is null and it shouldn't be", terms.get("abb") != null);
    assertTrue("abc is null and it shouldn't be", terms.get("abc") != null);
    assertTrue("a is null", terms.get("a") != null);
    assertTrue("b is not null and it should be", terms.get("b") == null);

    params.add(TermsParams.TERMS_UPPER_INCLUSIVE, "true");
    rsp = new SolrQueryResponse();
    rsp.add("responseHeader", new SimpleOrderedMap());
    handler.handleRequest(new LocalSolrQueryRequest(core, params), rsp);
    values = rsp.getValues();
    terms = (NamedList) ((NamedList) values.get("terms")).get("lowerfilt");
    assertTrue("terms Size: " + terms.size() + " is not: " + 7, terms.size() == 7);
    assertTrue("aa is null and it shouldn't be", terms.get("aa") != null);
    assertTrue("ab is null and it shouldn't be", terms.get("ab") != null);
    assertTrue("aaa is null and it shouldn't be", terms.get("aaa") != null);
    assertTrue("abb is null and it shouldn't be", terms.get("abb") != null);
    assertTrue("abc is null and it shouldn't be", terms.get("abc") != null);
    assertTrue("b is null and it shouldn't be", terms.get("b") != null);
    assertTrue("a is null", terms.get("a") != null);
    assertTrue("baa is not null", terms.get("baa") == null);

    params = new ModifiableSolrParams();
    params.add(TermsParams.TERMS, "true");
    params.add(TermsParams.TERMS_FIELD, "lowerfilt");
    params.add(TermsParams.TERMS_LOWER, "a");
    params.add(TermsParams.TERMS_UPPER, "b");
    params.add(TermsParams.TERMS_RAW, "true");  // this should have no effect on a text field
    params.add(TermsParams.TERMS_LIMIT, String.valueOf(2));
    rsp = new SolrQueryResponse();
    rsp.add("responseHeader", new SimpleOrderedMap());
    handler.handleRequest(new LocalSolrQueryRequest(core, params), rsp);
    values = rsp.getValues();
    terms = (NamedList) ((NamedList) values.get("terms")).get("lowerfilt");
    assertTrue("terms Size: " + terms.size() + " is not: " + 2, terms.size() == 2);
    assertTrue("aa is null and it shouldn't be", terms.get("a") != null);
    assertTrue("aaa is null and it shouldn't be", terms.get("aa") != null);
    assertTrue("abb is not null", terms.get("abb") == null);
    assertTrue("abc is not null", terms.get("abc") == null);
    assertTrue("b is null and it shouldn't be", terms.get("b") == null);
    assertTrue("baa is not null", terms.get("baa") == null);

    params = new ModifiableSolrParams();
    params.add(TermsParams.TERMS, "true");
    params.add(TermsParams.TERMS_FIELD, "foo_i");
    rsp = new SolrQueryResponse();
    rsp.add("responseHeader", new SimpleOrderedMap());
    handler.handleRequest(new LocalSolrQueryRequest(core, params), rsp);
    values = rsp.getValues();
    terms = (NamedList) ((NamedList) values.get("terms")).get("foo_i");
    assertEquals(2,terms.get("1"));

    params.add("terms.raw","true");
    rsp = new SolrQueryResponse();
    rsp.add("responseHeader", new SimpleOrderedMap());
    handler.handleRequest(new LocalSolrQueryRequest(core, params), rsp);
    values = rsp.getValues();
    terms = (NamedList) ((NamedList) values.get("terms")).get("foo_i");
    assertTrue(terms.get("1") == null);

    // check something at the end of the index
    params.set(TermsParams.TERMS_FIELD, "zzz_i");
    rsp = new SolrQueryResponse();
    rsp.add("responseHeader", new SimpleOrderedMap());
    handler.handleRequest(new LocalSolrQueryRequest(core, params), rsp);
    values = rsp.getValues();
    terms = (NamedList) ((NamedList) values.get("terms")).get("zzz_i");
    assertTrue(terms.size() == 0);

  }

  @Test
  public void testMinMaxFreq() throws Exception {
    SolrCore core = h.getCore();
    TermsComponent tc = (TermsComponent) core.getSearchComponent("termsComp");
    assertTrue("tc is null and it shouldn't be", tc != null);
    SolrRequestHandler handler;
    SolrQueryResponse rsp;
    NamedList values;
    NamedList terms;
    handler = core.getRequestHandler("/terms");
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.add(TermsParams.TERMS, "true");
    params.add(TermsParams.TERMS_FIELD, "lowerfilt");
    params.add(TermsParams.TERMS_LIMIT, String.valueOf(50));
    // Tests TERMS_LOWER = "a" with freqmin = 2, freqmax = -1, terms.size() = 1
    params.add(TermsParams.TERMS_LOWER, "a");
    params.add(TermsParams.TERMS_MINCOUNT,String.valueOf(2));
    params.add(TermsParams.TERMS_MAXCOUNT,String.valueOf(TermsComponent.UNLIMITED_MAX_COUNT));
    rsp = new SolrQueryResponse();
    rsp.add("responseHeader", new SimpleOrderedMap());
    handler.handleRequest(new LocalSolrQueryRequest(core, params), rsp);
    values = rsp.getValues();
    terms = (NamedList) ((NamedList) values.get("terms")).get("lowerfilt");
    assertTrue("terms Size: " + terms.size() + " is not: " + 1, terms.size() == 1);

    params = new ModifiableSolrParams();
    params.add(TermsParams.TERMS, "true");
    params.add(TermsParams.TERMS_FIELD, "standardfilt");
    params.add(TermsParams.TERMS_LIMIT, String.valueOf(50));
    // Tests TERMS_LOWER = "a" with freqmin = 2, freqmax = -1, terms.size() = 1
    params.add(TermsParams.TERMS_LOWER, "d");
    params.add(TermsParams.TERMS_MINCOUNT,String.valueOf(2));
    params.add(TermsParams.TERMS_MAXCOUNT,String.valueOf(3));
    rsp = new SolrQueryResponse();
    rsp.add("responseHeader", new SimpleOrderedMap());
    handler.handleRequest(new LocalSolrQueryRequest(core, params), rsp);
    values = rsp.getValues();
    terms = (NamedList) ((NamedList) values.get("terms")).get("standardfilt");
    assertTrue("terms Size: " + terms.size() + " is not: " + 3, terms.size() == 3);
    Integer d = (Integer) terms.get("d");
    assertTrue(d + " does not equal: " + 3, d == 3);

  }
}

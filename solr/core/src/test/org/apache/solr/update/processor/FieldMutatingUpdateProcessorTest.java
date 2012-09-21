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

package org.apache.solr.update.processor;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.TreeSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Arrays;
import java.io.IOException;

import org.apache.solr.SolrTestCaseJ4;

import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;

import org.apache.solr.core.SolrCore;
import org.apache.solr.schema.IndexSchema;

import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;

import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.update.processor.UpdateRequestProcessor;
import org.apache.solr.update.processor.UpdateRequestProcessorChain;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Tests the basics of configuring FieldMutatingUpdateProcessors  
 * (mainly via TrimFieldUpdateProcessor) and the logic of other various 
 * subclasses.
 */
public class FieldMutatingUpdateProcessorTest extends UpdateProcessorTestBase {

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig-update-processor-chains.xml", "schema12.xml");
  }

  public void testComprehensive() throws Exception {

    final String countMe = "how long is this string?";
    final int count = countMe.length();

    processAdd("comprehensive", 
               doc(f("id", "1111"),
                   f("primary_author_s1",
                     "XXXX", "Adam", "Sam"),
                   f("all_authors_s1",
                     "XXXX", "Adam", "Sam"),
                   f("foo_is", countMe, new Integer(42)),
                   f("first_foo_l", countMe, new Integer(-34)),
                   f("max_foo_l", countMe, new Integer(-34)),
                   f("min_foo_l", countMe, new Integer(-34))));

    assertU(commit());

    assertQ(req("id:1111")
            ,"//str[@name='primary_author_s1'][.='XXXX']"
            ,"//str[@name='all_authors_s1'][.='XXXX; Adam; Sam']"
            ,"//arr[@name='foo_is']/int[1][.='"+count+"']"
            ,"//arr[@name='foo_is']/int[2][.='42']"
            ,"//long[@name='max_foo_l'][.='"+count+"']"
            ,"//long[@name='first_foo_l'][.='"+count+"']"
            ,"//long[@name='min_foo_l'][.='-34']"
            );
  }

  public void testTrimAll() throws Exception {
    SolrInputDocument d = null;

    d = processAdd("trim-all", 
                   doc(f("id", "1111"),
                       f("name", " Hoss ", new StringBuilder(" Man")),
                       f("foo_t", " some text ", "other Text\t"),
                       f("foo_d", new Integer(42)),
                       field("foo_s", 5.0F, " string ")));

    assertNotNull(d);

    // simple stuff
    assertEquals("string", d.getFieldValue("foo_s"));
    assertEquals(Arrays.asList("some text","other Text"), 
                 d.getFieldValues("foo_t"));
    assertEquals(Arrays.asList("Hoss","Man"), 
                 d.getFieldValues("name"));

    // slightly more interesting
    assertEquals("processor borked non string value", 
                 new Integer(42), d.getFieldValue("foo_d"));
    assertEquals("wrong boost", 
                 5.0F, d.getField("foo_s").getBoost(), 0.0F);
  }

  public void testTrimFields() throws Exception {
    for (String chain : Arrays.asList("trim-fields", "trim-fields-arr")) {
      SolrInputDocument d = null;
      d = processAdd(chain,
                     doc(f("id", "1111"),
                         f("name", " Hoss ", " Man"),
                         f("foo_t", " some text ", "other Text\t"),
                         f("foo_s", " string ")));
      
      assertNotNull(d);
      
      assertEquals(" string ", d.getFieldValue("foo_s"));
      assertEquals(Arrays.asList("some text","other Text"), 
                   d.getFieldValues("foo_t"));
      assertEquals(Arrays.asList("Hoss","Man"), 
                   d.getFieldValues("name"));
    }
  }

  public void testTrimField() throws Exception {
    SolrInputDocument d = null;
    d = processAdd("trim-field", 
                   doc(f("id", "1111"),
                       f("name", " Hoss ", " Man"),
                       f("foo_t", " some text ", "other Text\t"),
                       f("foo_s", " string ")));

    assertNotNull(d);

    assertEquals(" string ", d.getFieldValue("foo_s"));
    assertEquals(Arrays.asList("some text","other Text"), 
                 d.getFieldValues("foo_t"));
    assertEquals(Arrays.asList(" Hoss "," Man"), 
                 d.getFieldValues("name"));
  }

  public void testTrimRegex() throws Exception {
    SolrInputDocument d = null;
    d = processAdd("trim-field-regexes", 
                   doc(f("id", "1111"),
                       f("foo_t", " string1 "),
                       f("foozat_s", " string2 "),
                       f("bar_t", " string3 "),
                       f("bar_s", " string4 ")));

    assertNotNull(d);

    assertEquals("string1", d.getFieldValue("foo_t"));
    assertEquals("string2", d.getFieldValue("foozat_s"));
    assertEquals(" string3 ", d.getFieldValue("bar_t"));
    assertEquals("string4", d.getFieldValue("bar_s"));

  }

  public void testTrimTypes() throws Exception {
    SolrInputDocument d = null;
    d = processAdd("trim-types", 
                   doc(f("id", "1111"),
                       f("foo_sw", " string0 "),
                       f("name", " string1 "),
                       f("title", " string2 "),
                       f("bar_t", " string3 "),
                       f("bar_s", " string4 ")));

    assertNotNull(d);

    assertEquals("string0", d.getFieldValue("foo_sw"));
    assertEquals("string1", d.getFieldValue("name"));
    assertEquals("string2", d.getFieldValue("title"));
    assertEquals(" string3 ", d.getFieldValue("bar_t"));
    assertEquals(" string4 ", d.getFieldValue("bar_s"));

  }

  public void testTrimClasses() throws Exception {
    SolrInputDocument d = null;
    d = processAdd("trim-classes", 
                   doc(f("id", "1111"),
                       f("foo_t", " string1 "),
                       f("foo_s", " string2 "),
                       f("bar_dt", " string3 "),
                       f("bar_pdt", " string4 ")));

    assertNotNull(d);

    assertEquals(" string1 ", d.getFieldValue("foo_t"));
    assertEquals("string2", d.getFieldValue("foo_s"));
    assertEquals("string3", d.getFieldValue("bar_dt"));
    assertEquals("string4", d.getFieldValue("bar_pdt"));

  }

  public void testTrimMultipleRules() throws Exception {
    SolrInputDocument d = null;
    d = processAdd("trim-multi", 
                   doc(f("id", "1111"),
                       f("foo_t", " string1 "),
                       f("foo_s", " string2 "),
                       f("bar_dt", " string3 "),
                       f("foo_pdt", " string4 ")));

    assertNotNull(d);

    assertEquals(" string1 ", d.getFieldValue("foo_t"));
    assertEquals("string2", d.getFieldValue("foo_s"));
    assertEquals(" string3 ", d.getFieldValue("bar_dt"));
    assertEquals("string4", d.getFieldValue("foo_pdt"));

  }

  public void testTrimExclusions() throws Exception {
    SolrInputDocument d = null;
    d = processAdd("trim-most", 
                   doc(f("id", "1111"),
                       f("foo_t", " string1 "),
                       f("foo_s", " string2 "),
                       f("bar_dt", " string3 "),
                       f("foo_pdt", " string4 ")));

    assertNotNull(d);

    assertEquals(" string1 ", d.getFieldValue("foo_t"));
    assertEquals("string2", d.getFieldValue("foo_s"));
    assertEquals("string3", d.getFieldValue("bar_dt"));
    assertEquals("string4", d.getFieldValue("foo_pdt"));

    d = processAdd("trim-many", 
                   doc(f("id", "1111"),
                       f("foo_t", " string1 "),
                       f("foo_s", " string2 "),
                       f("bar_dt", " string3 "),
                       f("bar_HOSS_s", " string4 "),
                       f("foo_pdt", " string5 "),
                       f("foo_HOSS_pdt", " string6 ")));

    assertNotNull(d);

    assertEquals("string1", d.getFieldValue("foo_t"));
    assertEquals("string2", d.getFieldValue("foo_s"));
    assertEquals("string3", d.getFieldValue("bar_dt"));
    assertEquals(" string4 ", d.getFieldValue("bar_HOSS_s"));
    assertEquals("string5", d.getFieldValue("foo_pdt"));
    assertEquals(" string6 ", d.getFieldValue("foo_HOSS_pdt"));

    d = processAdd("trim-few", 
                   doc(f("id", "1111"),
                       f("foo_t", " string1 "),
                       f("foo_s", " string2 "),
                       f("bar_dt", " string3 "),
                       f("bar_HOSS_s", " string4 "),
                       f("foo_pdt", " string5 "),
                       f("foo_HOSS_pdt", " string6 ")));

    assertNotNull(d);

    assertEquals("string1", d.getFieldValue("foo_t"));
    assertEquals("string2", d.getFieldValue("foo_s"));
    assertEquals(" string3 ", d.getFieldValue("bar_dt"));
    assertEquals(" string4 ", d.getFieldValue("bar_HOSS_s"));
    assertEquals(" string5 ", d.getFieldValue("foo_pdt"));
    assertEquals(" string6 ", d.getFieldValue("foo_HOSS_pdt"));

    d = processAdd("trim-some", 
                   doc(f("id", "1111"),
                       f("foo_t", " string1 "),
                       f("foo_s", " string2 "),
                       f("bar_dt", " string3 "),
                       f("bar_HOSS_s", " string4 "),
                       f("foo_pdt", " string5 "),
                       f("foo_HOSS_pdt", " string6 ")));

    assertNotNull(d);

    assertEquals("string1", d.getFieldValue("foo_t"));
    assertEquals("string2", d.getFieldValue("foo_s"));
    assertEquals("string3", d.getFieldValue("bar_dt"));
    assertEquals("string4", d.getFieldValue("bar_HOSS_s"));
    assertEquals("string5", d.getFieldValue("foo_pdt"));
    assertEquals(" string6 ", d.getFieldValue("foo_HOSS_pdt"));
  }

  public void testRemoveBlanks() throws Exception {
    SolrInputDocument d = null;
    d = processAdd("remove-all-blanks", 
                   doc(f("id", "1111"),
                       f("foo_s", "string1", ""),
                       f("bar_dt", "string2", "", "string3"),
                       f("yak_t", ""),
                       f("foo_d", new Integer(42))));

    assertNotNull(d);

    assertEquals(Arrays.asList("string1"),
                 d.getFieldValues("foo_s"));
    assertEquals(Arrays.asList("string2","string3"),
                 d.getFieldValues("bar_dt"));
    assertFalse("shouldn't be any values for yak_t",
                d.containsKey("yak_t"));
    assertEquals("processor borked non string value", 
                 new Integer(42), d.getFieldValue("foo_d"));
   
  }

  public void testStrLength() throws Exception {
    SolrInputDocument d = null;
    d = processAdd("length-none", 
                   doc(f("id", "1111"),
                       f("foo_s", "string1", "string222"),
                       f("bar_dt", "string3"),
                       f("yak_t", ""),
                       f("foo_d", new Integer(42))));

    assertNotNull(d);

    assertEquals(Arrays.asList("string1","string222"),
                 d.getFieldValues("foo_s"));
    assertEquals("string3", d.getFieldValue("bar_dt"));
    assertEquals("", d.getFieldValue("yak_t"));
    assertEquals("processor borked non string value", 
                 new Integer(42), d.getFieldValue("foo_d"));
   
    d = processAdd("length-some", 
                   doc(f("id", "1111"),
                       f("foo_s", "string1", "string222"),
                       f("bar_dt", "string3"),
                       f("yak_t", ""),
                       f("foo_d", new Integer(42))));

    assertNotNull(d);

    assertEquals(Arrays.asList(new Integer(7), new Integer(9)),
                               d.getFieldValues("foo_s"));
    assertEquals("string3", d.getFieldValue("bar_dt"));
    assertEquals(new Integer(0), d.getFieldValue("yak_t"));
    assertEquals("processor borked non string value", 
                 new Integer(42), d.getFieldValue("foo_d"));
  }

  public void testRegexReplace() throws Exception {
    SolrInputDocument d = null;
    d = processAdd("regex-replace", 
                   doc(f("id", "doc1"),
                       f("content", "This is         a text\t with a lot\n     of whitespace"),
                       f("title", "This\ttitle     has a lot of    spaces")));
    
    assertNotNull(d);
    
    assertEquals("ThisXisXaXtextXwithXaXlotXofXwhitespace", 
                 d.getFieldValue("content"));
    assertEquals("ThisXtitleXhasXaXlotXofXspaces", 
                 d.getFieldValue("title"));
  }
 
  public void testFirstValue() throws Exception {
    SolrInputDocument d = null;

    d = processAdd("first-value", 
                   doc(f("id", "1111"),
                       f("foo_s", "string1", "string222"),
                       f("bar_s", "string3"),
                       f("yak_t", "string4", "string5")));

    assertNotNull(d);

    assertEquals(Arrays.asList("string1"),
                 d.getFieldValues("foo_s"));
    assertEquals(Arrays.asList("string3"), 
                 d.getFieldValues("bar_s"));
    assertEquals(Arrays.asList("string4", "string5"), 
                 d.getFieldValues("yak_t"));
  }

  public void testLastValue() throws Exception {
    SolrInputDocument d = null;

    // basics

    d = processAdd("last-value", 
                   doc(f("id", "1111"),
                       f("foo_s", "string1", "string222"),
                       f("bar_s", "string3"),
                       f("yak_t", "string4", "string5")));

    assertNotNull(d);

    assertEquals(Arrays.asList("string222"),
                 d.getFieldValues("foo_s"));
    assertEquals(Arrays.asList("string3"), 
                 d.getFieldValues("bar_s"));
    assertEquals(Arrays.asList("string4", "string5"), 
                 d.getFieldValues("yak_t"));

    // test optimizations (and force test of defaults)

    SolrInputField special = null;

    // test something that's definitely a SortedSet

    special = new SolrInputField("foo_s");
    special.setValue(new TreeSet<String>
                     (Arrays.asList("ggg", "first", "last", "hhh")), 1.2F);
    
    d = processAdd("last-value", 
                   doc(f("id", "1111"),
                       special));

    assertNotNull(d);

    assertEquals("last", d.getFieldValue("foo_s"));

    // test something that's definitely a List
    
    special = new SolrInputField("foo_s");
    special.setValue(Arrays.asList("first", "ggg", "hhh", "last"), 1.2F);
    
    d = processAdd("last-value", 
                   doc(f("id", "1111"),
                       special));

    assertNotNull(d);

    assertEquals("last", d.getFieldValue("foo_s"));

    // test something that is definitely not a List or SortedSet 
    // (ie: get default behavior of Collection using iterator)

    special = new SolrInputField("foo_s");
    special.setValue(new LinkedHashSet<String>
                     (Arrays.asList("first", "ggg", "hhh", "last")), 1.2F);
    
    d = processAdd("last-value", 
                   doc(f("id", "1111"),
                       special));

    assertNotNull(d);

    assertEquals("last", d.getFieldValue("foo_s"));
    
   
  }

  public void testMinValue() throws Exception {
    SolrInputDocument d = null;

    d = processAdd("min-value", 
                   doc(f("id", "1111"),
                       f("foo_s", "zzz", "aaa", "bbb"),
                       f("foo_i", 42, 128, -3),
                       f("bar_s", "aaa"),
                       f("yak_t", "aaa", "bbb")));

    assertNotNull(d);

    assertEquals(Arrays.asList("aaa"),
                 d.getFieldValues("foo_s"));
    assertEquals(Arrays.asList(-3),
                 d.getFieldValues("foo_i"));
    assertEquals(Arrays.asList("aaa"), 
                 d.getFieldValues("bar_s"));
    assertEquals(Arrays.asList("aaa", "bbb"), 
                 d.getFieldValues("yak_t"));
   
    // failure when un-comparable

    SolrException error = null;
    try {
      ignoreException(".*Unable to mutate field.*");
      d = processAdd("min-value", 
                     doc(f("id", "1111"),
                         f("foo_s", "zzz", new Integer(42), "bbb"),
                         f("bar_s", "aaa"),
                         f("yak_t", "aaa", "bbb")));
    } catch (SolrException e) {
      error = e;
    } finally {
      resetExceptionIgnores();
    }
    assertNotNull("no error on un-comparable values", error);
    assertTrue("error doesn't mention field name",
               0 <= error.getMessage().indexOf("foo_s"));
  }

  public void testMaxValue() throws Exception {
    SolrInputDocument d = null;

    d = processAdd("max-value", 
                   doc(f("id", "1111"),
                       f("foo_s", "zzz", "aaa", "bbb"),
                       f("foo_i", 42, 128, -3),
                       f("bar_s", "aaa"),
                       f("yak_t", "aaa", "bbb")));

    assertNotNull(d);

    assertEquals(Arrays.asList("zzz"),
                 d.getFieldValues("foo_s"));
    assertEquals(Arrays.asList(128),
                 d.getFieldValues("foo_i"));
    assertEquals(Arrays.asList("aaa"), 
                 d.getFieldValues("bar_s"));
    assertEquals(Arrays.asList("aaa", "bbb"), 
                 d.getFieldValues("yak_t"));
   
    // failure when un-comparable

    SolrException error = null;
    try {
      ignoreException(".*Unable to mutate field.*");
      d = processAdd("min-value", 
                     doc(f("id", "1111"),
                         f("foo_s", "zzz", new Integer(42), "bbb"),
                         f("bar_s", "aaa"),
                         f("yak_t", "aaa", "bbb")));
    } catch (SolrException e) {
      error = e;
    } finally {
      resetExceptionIgnores();
    }
    assertNotNull("no error on un-comparable values", error);
    assertTrue("error doesn't mention field name",
               0 <= error.getMessage().indexOf("foo_s"));
  }
  

  public void testHtmlStrip() throws Exception {
    SolrInputDocument d = null;

    d = processAdd("html-strip", 
                   doc(f("id", "1111"),
                       f("html_s", "<body>hi &amp; bye", "aaa", "bbb"),
                       f("bar_s", "<body>hi &amp; bye")));

    assertNotNull(d);

    assertEquals(Arrays.asList("hi & bye", "aaa", "bbb"),
                 d.getFieldValues("html_s"));
    assertEquals("<body>hi &amp; bye", d.getFieldValue("bar_s"));
   
  }

  public void testTruncate() throws Exception {
    SolrInputDocument d = null;

    d = processAdd("truncate", 
                   doc(f("id", "1111"),
                       f("trunc", "123456789", "", 42, "abcd")));

    assertNotNull(d);

    assertEquals(Arrays.asList("12345", "", 42, "abcd"),
                 d.getFieldValues("trunc"));
  }

  public void testIgnore() throws Exception {

    IndexSchema schema = h.getCore().getSchema();
    assertNull("test expects 'foo_giberish' to not be a valid field, looks like schema was changed out from under us",
               schema.getFieldTypeNoEx("foo_giberish"));
    assertNotNull("test expects 't_raw' to be a valid field, looks like schema was changed out from under us",
                  schema.getFieldTypeNoEx("t_raw"));
    assertNotNull("test expects 'foo_s' to be a valid field, looks like schema was changed out from under us",
                  schema.getFieldTypeNoEx("foo_s"));
 
    SolrInputDocument d = null;
    
    d = processAdd("ignore-not-in-schema",       
                   doc(f("id", "1111"),
                       f("foo_giberish", "123456789", "", 42, "abcd"),
                       f("t_raw", "123456789", "", 42, "abcd"),
                       f("foo_s", "hoss")));
    
    assertNotNull(d);
    assertFalse(d.containsKey("foo_giberish"));
    assertEquals(Arrays.asList("123456789", "", 42, "abcd"), 
                 d.getFieldValues("t_raw"));
    assertEquals("hoss", d.getFieldValue("foo_s"));

    d = processAdd("ignore-some",
                   doc(f("id", "1111"),
                       f("foo_giberish", "123456789", "", 42, "abcd"),
                       f("t_raw", "123456789", "", 42, "abcd"),
                       f("foo_s", "hoss")));

    assertNotNull(d);
    assertEquals(Arrays.asList("123456789", "", 42, "abcd"), 
                 d.getFieldValues("foo_giberish"));
    assertFalse(d.containsKey("t_raw"));
    assertEquals("hoss", d.getFieldValue("foo_s"));
    

  }

  public void testCloneField() throws Exception {

    SolrInputDocument d = null;

    // regardless of chain, all of these should be equivilent
    for (String chain : Arrays.asList("clone-single", "clone-multi", 
                                      "clone-array","clone-selector" )) {

      // simple clone
      d = processAdd(chain,       
                     doc(f("id", "1111"),
                         f("source0_s", "NOT COPIED"),
                         f("source1_s", "123456789", "", 42, "abcd")));
      assertNotNull(chain, d);
      assertEquals(chain,
                   Arrays.asList("123456789", "", 42, "abcd"), 
                   d.getFieldValues("source1_s"));
      assertEquals(chain,
                   Arrays.asList("123456789", "", 42, "abcd"), 
                   d.getFieldValues("dest_s"));

      // append to existing values, preserve boost
      d = processAdd(chain,       
                     doc(f("id", "1111"),
                         field("dest_s", 2.3f, "orig1", "orig2"),
                         f("source0_s", "NOT COPIED"),
                         f("source1_s", "123456789", "", 42, "abcd")));
      assertNotNull(chain, d);
      assertEquals(chain,
                   Arrays.asList("123456789", "", 42, "abcd"), 
                   d.getFieldValues("source1_s"));
      assertEquals(chain,
                   Arrays.asList("orig1", "orig2", "123456789", "", 42, "abcd"),
                   d.getFieldValues("dest_s"));
      assertEquals(chain + ": dest boost changed", 
                   2.3f, d.getField("dest_s").getBoost(), 0.0f);
    }

    // should be equivilent for any chain matching source1_s and source2_s
    for (String chain : Arrays.asList("clone-multi",
                                      "clone-array","clone-selector" )) {

      // simple clone
      d = processAdd(chain,       
                     doc(f("id", "1111"),
                         f("source0_s", "NOT COPIED"),
                         f("source1_s", "123456789", "", 42, "abcd"),
                         f("source2_s", "xxx", 999)));
      assertNotNull(chain, d);
      assertEquals(chain,
                   Arrays.asList("123456789", "", 42, "abcd"), 
                   d.getFieldValues("source1_s"));
      assertEquals(chain,
                   Arrays.asList("xxx", 999),
                   d.getFieldValues("source2_s"));
      assertEquals(chain,
                   Arrays.asList("123456789", "", 42, "abcd", "xxx", 999), 
                   d.getFieldValues("dest_s"));

      // append to existing values, preserve boost
      d = processAdd(chain,       
                     doc(f("id", "1111"),
                         field("dest_s", 2.3f, "orig1", "orig2"),
                         f("source0_s", "NOT COPIED"),
                         f("source1_s", "123456789", "", 42, "abcd"),
                         f("source2_s", "xxx", 999)));
      assertNotNull(chain, d);
      assertEquals(chain,
                   Arrays.asList("123456789", "", 42, "abcd"), 
                   d.getFieldValues("source1_s"));
      assertEquals(chain,
                   Arrays.asList("xxx", 999),
                   d.getFieldValues("source2_s"));
      assertEquals(chain,
                   Arrays.asList("orig1", "orig2", 
                                 "123456789", "", 42, "abcd",
                                 "xxx", 999),
                   d.getFieldValues("dest_s"));
      assertEquals(chain + ": dest boost changed", 
                   2.3f, d.getField("dest_s").getBoost(), 0.0f);
    }
  }

  public void testCloneFieldExample() throws Exception {

    SolrInputDocument d = null;

    // test example from the javadocs
    d = processAdd("multiple-clones",       
                   doc(f("id", "1111"),
                       f("category", "misc"),
                       f("authors", "Isaac Asimov", "John Brunner"),
                       f("editors", "John W. Campbell"),
                       f("store1_price", 87),
                       f("store2_price", 78),
                       f("list_price", 1000)));
    assertNotNull(d);
    assertEquals("misc",d.getFieldValue("category"));
    assertEquals("misc",d.getFieldValue("category_s"));
    assertEquals(Arrays.asList("Isaac Asimov", "John Brunner"),
                 d.getFieldValues("authors"));
    assertEquals(Arrays.asList("John W. Campbell"),
                 d.getFieldValues("editors"));
    assertEquals(Arrays.asList("Isaac Asimov", "John Brunner", 
                               "John W. Campbell"),
                 d.getFieldValues("contributors"));
    assertEquals(87,d.getFieldValue("store1_price"));
    assertEquals(78,d.getFieldValue("store2_price"));
    assertEquals(1000,d.getFieldValue("list_price"));
    assertEquals(Arrays.asList(87, 78),
                 d.getFieldValues("all_prices"));

  } 

  public void testCountValues() throws Exception {

    SolrInputDocument d = null;

    // trivial 
    d = processAdd("count",       
                   doc(f("id", "1111"),
                       f("count_field", "aaa", "bbb", "ccc")));

    assertNotNull(d);
    assertEquals(3, d.getFieldValue("count_field"));

    // edge case: no values to count, means no count 
    // (use default if you want one)
    d = processAdd("count",       
                   doc(f("id", "1111")));

    assertNotNull(d);
    assertFalse(d.containsKey("count_field"));

    // typical usecase: clone and count 
    d = processAdd("clone-then-count",       
                   doc(f("id", "1111"),
                       f("category", "scifi", "war", "space"),
                       f("editors", "John W. Campbell"),
                       f("list_price", 1000)));
    assertNotNull(d);
    assertEquals(Arrays.asList("scifi", "war", "space"),
                 d.getFieldValues("category"));
    assertEquals(3,
                 d.getFieldValue("category_count"));
    assertEquals(Arrays.asList("John W. Campbell"),
                 d.getFieldValues("editors"));
    assertEquals(1000,d.getFieldValue("list_price"));

    // typical usecase: clone and count demonstrating default
    d = processAdd("clone-then-count",       
                   doc(f("id", "1111"),
                       f("editors", "Anonymous"),
                       f("list_price", 1000)));
    assertNotNull(d);
    assertEquals(0,
                 d.getFieldValue("category_count"));
    assertEquals(Arrays.asList("Anonymous"),
                 d.getFieldValues("editors"));
    assertEquals(1000,d.getFieldValue("list_price"));

    


  } 

  public void testCloneCombinations() throws Exception {

    SolrInputDocument d = null;

    // maxChars
    d = processAdd("clone-max-chars",
                   doc(f("id", "1111"),
                       f("field1", "text")));
    assertNotNull(d);
    assertEquals("text",d.getFieldValue("field1"));
    assertEquals("tex",d.getFieldValue("toField"));

    // move
    d = processAdd("clone-move",
                   doc(f("id", "1111"),
                       f("field1", "text")));
    assertNotNull(d);
    assertEquals("text",d.getFieldValue("toField"));
    assertFalse(d.containsKey("field1"));

    // replace
    d = processAdd("clone-replace",
                   doc(f("id", "1111"),
                       f("toField", "IGNORED"),
                       f("field1", "text")));
    assertNotNull(d);
    assertEquals("text", d.getFieldValue("field1"));
    assertEquals("text", d.getFieldValue("toField"));

    // append
    d = processAdd("clone-append",
                   doc(f("id", "1111"),
                       f("toField", "aaa"),
                       f("field1", "bbb"),
                       f("field2", "ccc")));
    assertNotNull(d);
    assertEquals("bbb", d.getFieldValue("field1"));
    assertEquals("ccc", d.getFieldValue("field2"));
    assertEquals("aaa; bbb; ccc", d.getFieldValue("toField"));
  } 

  public void testConcatDefaults() throws Exception {
    SolrInputDocument d = null;
    d = processAdd("concat-defaults", 
                   doc(f("id", "1111", "222"),
                       f("attr_foo", "string1", "string2"),
                       f("foo_s1", "string3", "string4"),
                       f("bar_dt", "string5", "string6"),
                       f("bar_HOSS_s", "string7", "string8"),
                       f("foo_d", new Integer(42))));

    assertNotNull(d);

    assertEquals("1111, 222", d.getFieldValue("id"));
    assertEquals(Arrays.asList("string1","string2"),
                 d.getFieldValues("attr_foo"));
    assertEquals("string3, string4", d.getFieldValue("foo_s1"));
    assertEquals(Arrays.asList("string5","string6"),
                 d.getFieldValues("bar_dt"));
    assertEquals(Arrays.asList("string7","string8"),
                 d.getFieldValues("bar_HOSS_s"));
    assertEquals("processor borked non string value", 
                 new Integer(42), d.getFieldValue("foo_d"));
   
  }

  public void testConcatExplicit() throws Exception {
    doSimpleDelimTest("concat-field", ", ");
  }
  public void testConcatExplicitWithDelim() throws Exception {
    doSimpleDelimTest("concat-type-delim", "; ");
  }
  private void doSimpleDelimTest(final String chain, final String delim) 
    throws Exception {

    SolrInputDocument d = null;
    d = processAdd(chain, 
                   doc(f("id", "1111"),
                       f("foo_t", "string1", "string2"),
                       f("foo_d", new Integer(42)),
                       field("foo_s", 3.0F, "string3", "string4")));

    assertNotNull(d);

    assertEquals(Arrays.asList("string1","string2"), 
                 d.getFieldValues("foo_t"));
    assertEquals("string3" + delim + "string4", d.getFieldValue("foo_s"));

    // slightly more interesting
    assertEquals("processor borked non string value", 
                 new Integer(42), d.getFieldValue("foo_d"));
    assertEquals("wrong boost", 
                 3.0F, d.getField("foo_s").getBoost(), 0.0F);
  }

}

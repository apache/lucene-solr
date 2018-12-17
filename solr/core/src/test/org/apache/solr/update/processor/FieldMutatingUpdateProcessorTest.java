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

import java.util.LinkedHashSet;
import java.util.TreeSet;
import java.util.Arrays;

import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;
import org.apache.solr.schema.IndexSchema;
import org.junit.BeforeClass;

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
                   f("foo_is", countMe, 42),
                   f("first_foo_l", countMe, -34),
                   f("max_foo_l", countMe, -34),
                   f("min_foo_l", countMe, -34)));

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
                       f("foo_d", 42),
                       field("foo_s", " string ")));

    assertNotNull(d);

    // simple stuff
    assertEquals("string", d.getFieldValue("foo_s"));
    assertEquals(Arrays.asList("some text","other Text"), 
                 d.getFieldValues("foo_t"));
    assertEquals(Arrays.asList("Hoss","Man"), 
                 d.getFieldValues("name"));

    // slightly more interesting
    assertEquals("processor borked non string value", 
                 42, d.getFieldValue("foo_d"));
  }

  public void testUniqValues() throws Exception {
    final String chain = "uniq-values";
    SolrInputDocument d = null;
    d = processAdd(chain,
                   doc(f("id", "1111"),
                       f("name", "Hoss", "Man", "Hoss"),
                       f("uniq_1_s", "Hoss", "Man", "Hoss"),
                       f("uniq_2_s", "Foo", "Hoss", "Man", "Hoss", "Bar"),
                       f("uniq_3_s", 5.0F, 23, "string", 5.0F)));
    
    assertNotNull(d);
    
    assertEquals(Arrays.asList("Hoss", "Man", "Hoss"),
                 d.getFieldValues("name"));
    assertEquals(Arrays.asList("Hoss","Man"), 
                 d.getFieldValues("uniq_1_s"));
    assertEquals(Arrays.asList("Foo","Hoss","Man","Bar"),
                 d.getFieldValues("uniq_2_s"));
    assertEquals(Arrays.asList(5.0F, 23, "string"),
                 d.getFieldValues("uniq_3_s"));
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
                       f("bar_dt", " string3 ")));

    assertNotNull(d);

    assertEquals(" string1 ", d.getFieldValue("foo_t"));
    assertEquals("string2", d.getFieldValue("foo_s"));
    assertEquals("string3", d.getFieldValue("bar_dt"));
  }

  public void testTrimMultipleRules() throws Exception {
    SolrInputDocument d = null;
    d = processAdd("trim-multi", 
                   doc(f("id", "1111"),
                       f("foo_t", " string1 "),
                       f("foo_s", " string2 "),
                       f("bar_dt", " string3 ")));

    assertNotNull(d);

    assertEquals(" string1 ", d.getFieldValue("foo_t"));
    assertEquals("string2", d.getFieldValue("foo_s"));
    assertEquals(" string3 ", d.getFieldValue("bar_dt"));
  }

  public void testTrimExclusions() throws Exception {
    SolrInputDocument d = null;
    d = processAdd("trim-most", 
                   doc(f("id", "1111"),
                       f("foo_t", " string1 "),
                       f("foo_s", " string2 "),
                       f("bar_dt", " string3 ")));

    assertNotNull(d);

    assertEquals(" string1 ", d.getFieldValue("foo_t"));
    assertEquals("string2", d.getFieldValue("foo_s"));
    assertEquals("string3", d.getFieldValue("bar_dt"));

    d = processAdd("trim-many", 
                   doc(f("id", "1111"),
                       f("foo_t", " string1 "),
                       f("foo_s", " string2 "),
                       f("bar_dt", " string3 "),
                       f("bar_HOSS_s", " string4 ")));

    assertNotNull(d);

    assertEquals("string1", d.getFieldValue("foo_t"));
    assertEquals("string2", d.getFieldValue("foo_s"));
    assertEquals("string3", d.getFieldValue("bar_dt"));
    assertEquals(" string4 ", d.getFieldValue("bar_HOSS_s"));

    d = processAdd("trim-few", 
                   doc(f("id", "1111"),
                       f("foo_t", " string1 "),
                       f("foo_s", " string2 "),
                       f("bar_dt", " string3 "),
                       f("bar_HOSS_s", " string4 ")));

    assertNotNull(d);

    assertEquals("string1", d.getFieldValue("foo_t"));
    assertEquals("string2", d.getFieldValue("foo_s"));
    assertEquals(" string3 ", d.getFieldValue("bar_dt"));
    assertEquals(" string4 ", d.getFieldValue("bar_HOSS_s"));

    d = processAdd("trim-some", 
                   doc(f("id", "1111"),
                       f("foo_t", " string1 "),
                       f("foo_s", " string2 "),
                       f("bar_dt", " string3 "),
                       f("bar_HOSS_s", " string4 ")));

    assertNotNull(d);

    assertEquals("string1", d.getFieldValue("foo_t"));
    assertEquals("string2", d.getFieldValue("foo_s"));
    assertEquals("string3", d.getFieldValue("bar_dt"));
    assertEquals("string4", d.getFieldValue("bar_HOSS_s"));
  }

  public void testRemoveBlanks() throws Exception {
    SolrInputDocument d = null;
    d = processAdd("remove-all-blanks", 
                   doc(f("id", "1111"),
                       f("foo_s", "string1", ""),
                       f("bar_dt", "string2", "", "string3"),
                       f("yak_t", ""),
                       f("foo_d", 42)));

    assertNotNull(d);

    assertEquals(Arrays.asList("string1"),
                 d.getFieldValues("foo_s"));
    assertEquals(Arrays.asList("string2","string3"),
                 d.getFieldValues("bar_dt"));
    assertFalse("shouldn't be any values for yak_t",
                d.containsKey("yak_t"));
    assertEquals("processor borked non string value", 
                 42, d.getFieldValue("foo_d"));
   
  }

  public void testStrLength() throws Exception {
    SolrInputDocument d = null;
    d = processAdd("length-none", 
                   doc(f("id", "1111"),
                       f("foo_s", "string1", "string222"),
                       f("bar_dt", "string3"),
                       f("yak_t", ""),
                       f("foo_d", 42)));

    assertNotNull(d);

    assertEquals(Arrays.asList("string1","string222"),
                 d.getFieldValues("foo_s"));
    assertEquals("string3", d.getFieldValue("bar_dt"));
    assertEquals("", d.getFieldValue("yak_t"));
    assertEquals("processor borked non string value", 
                 42, d.getFieldValue("foo_d"));
   
    d = processAdd("length-some", 
                   doc(f("id", "1111"),
                       f("foo_s", "string1", "string222"),
                       f("bar_dt", "string3"),
                       f("yak_t", ""),
                       f("foo_d", 42)));

    assertNotNull(d);

    assertEquals(Arrays.asList(7, 9),
                               d.getFieldValues("foo_s"));
    assertEquals("string3", d.getFieldValue("bar_dt"));
    assertEquals(0, d.getFieldValue("yak_t"));
    assertEquals("processor borked non string value", 
                 42, d.getFieldValue("foo_d"));
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

    // literalReplacement = true
    d = processAdd("regex-replace-literal-true",
        doc(f("id", "doc2"),
            f("content", "Let's try this one"),
            f("title", "Let's try try this one")));

    assertNotNull(d);

    assertEquals("Let's <$1> this one",
        d.getFieldValue("content"));
    assertEquals("Let's <$1> <$1> this one",
        d.getFieldValue("title"));

    // literalReplacement is not specified, defaults to true
    d = processAdd("regex-replace-literal-default-true",
        doc(f("id", "doc3"),
            f("content", "Let's try this one"),
            f("title", "Let's try try this one")));

    assertNotNull(d);

    assertEquals("Let's <$1> this one",
        d.getFieldValue("content"));
    assertEquals("Let's <$1> <$1> this one",
        d.getFieldValue("title"));

    // if user passes literalReplacement as a string param instead of boolean
    d = processAdd("regex-replace-literal-str-true",
        doc(f("id", "doc4"),
            f("content", "Let's try this one"),
            f("title", "Let's try try this one")));

    assertNotNull(d);

    assertEquals("Let's <$1> this one",
        d.getFieldValue("content"));
    assertEquals("Let's <$1> <$1> this one",
        d.getFieldValue("title"));

    // This is with literalReplacement = false
    d = processAdd("regex-replace-literal-false",
        doc(f("id", "doc5"),
            f("content", "Let's try this one"),
            f("title", "Let's try try this one")));

    assertNotNull(d);

    assertEquals("Let's <try> this one",
        d.getFieldValue("content"));
    assertEquals("Let's <try> <try> this one",
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
    special.setValue(new TreeSet<>
                     (Arrays.asList("ggg", "first", "last", "hhh")));
    
    d = processAdd("last-value", 
                   doc(f("id", "1111"),
                       special));

    assertNotNull(d);

    assertEquals("last", d.getFieldValue("foo_s"));

    // test something that's definitely a List
    
    special = new SolrInputField("foo_s");
    special.setValue(Arrays.asList("first", "ggg", "hhh", "last"));
    
    d = processAdd("last-value", 
                   doc(f("id", "1111"),
                       special));

    assertNotNull(d);

    assertEquals("last", d.getFieldValue("foo_s"));

    // test something that is definitely not a List or SortedSet 
    // (ie: get default behavior of Collection using iterator)

    special = new SolrInputField("foo_s");
    special.setValue(new LinkedHashSet<>
                     (Arrays.asList("first", "ggg", "hhh", "last")));
    
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
                         f("foo_s", "zzz", 42, "bbb"),
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
                         f("foo_s", "zzz", 42, "bbb"),
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

    IndexSchema schema = h.getCore().getLatestSchema();
    assertNull("test expects 'foo_giberish' to not be a valid field, looks like schema was changed out from under us",
               schema.getFieldTypeNoEx("foo_giberish"));
    assertNull("test expects 'bar_giberish' to not be a valid field, looks like schema was changed out from under us",
               schema.getFieldTypeNoEx("bar_giberish"));
    assertNotNull("test expects 't_raw' to be a valid field, looks like schema was changed out from under us",
                  schema.getFieldTypeNoEx("t_raw"));
    assertNotNull("test expects 'foo_s' to be a valid field, looks like schema was changed out from under us",
                  schema.getFieldTypeNoEx("foo_s"));
 
    SolrInputDocument d = null;
    
    d = processAdd("ignore-not-in-schema",       
                   doc(f("id", "1111"),
                       f("bar_giberish", "123456789", "", 42, "abcd"),
                       f("foo_giberish", "123456789", "", 42, "abcd"),
                       f("t_raw", "123456789", "", 42, "abcd"),
                       f("foo_s", "hoss")));
    
    assertNotNull(d);
    assertFalse(d.containsKey("bar_giberish"));
    assertFalse(d.containsKey("foo_giberish"));
    assertEquals(Arrays.asList("123456789", "", 42, "abcd"), 
                 d.getFieldValues("t_raw"));
    assertEquals("hoss", d.getFieldValue("foo_s"));

    d = processAdd("ignore-some",
                   doc(f("id", "1111"),
                       f("foo_giberish", "123456789", "", 42, "abcd"),
                       f("bar_giberish", "123456789", "", 42, "abcd"),
                       f("t_raw", "123456789", "", 42, "abcd"),
                       f("foo_s", "hoss")));

    assertNotNull(d);
    assertEquals(Arrays.asList("123456789", "", 42, "abcd"), 
                 d.getFieldValues("foo_giberish"));
    assertEquals(Arrays.asList("123456789", "", 42, "abcd"), 
                 d.getFieldValues("bar_giberish"));
    assertFalse(d.containsKey("t_raw"));
    assertEquals("hoss", d.getFieldValue("foo_s"));

    d = processAdd("ignore-not-in-schema-explicit-selector",
                   doc(f("id", "1111"),
                       f("foo_giberish", "123456789", "", 42, "abcd"),
                       f("bar_giberish", "123456789", "", 42, "abcd"),
                       f("t_raw", "123456789", "", 42, "abcd"),
                       f("foo_s", "hoss")));
    assertNotNull(d);
    assertFalse(d.containsKey("foo_giberish"));
    assertFalse(d.containsKey("bar_giberish"));
    assertEquals(Arrays.asList("123456789", "", 42, "abcd"),
                 d.getFieldValues("t_raw"));
    assertEquals("hoss", d.getFieldValue("foo_s"));

    d = processAdd("ignore-not-in-schema-and-foo-name-prefix",
                   doc(f("id", "1111"),
                       f("foo_giberish", "123456789", "", 42, "abcd"),
                       f("bar_giberish", "123456789", "", 42, "abcd"),
                       f("t_raw", "123456789", "", 42, "abcd"),
                       f("foo_s", "hoss")));
    assertNotNull(d);
    assertFalse(d.containsKey("foo_giberish"));
    assertEquals(Arrays.asList("123456789", "", 42, "abcd"),
                 d.getFieldValues("bar_giberish"));
    assertEquals(Arrays.asList("123456789", "", 42, "abcd"),
                 d.getFieldValues("t_raw"));
    assertEquals("hoss", d.getFieldValue("foo_s"));

    d = processAdd("ignore-foo-name-prefix-except-not-schema",
                   doc(f("id", "1111"),
                       f("foo_giberish", "123456789", "", 42, "abcd"),
                       f("bar_giberish", "123456789", "", 42, "abcd"),
                       f("t_raw", "123456789", "", 42, "abcd"),
                       f("foo_s", "hoss")));
    assertNotNull(d);
    assertEquals(Arrays.asList("123456789", "", 42, "abcd"),
                 d.getFieldValues("foo_giberish"));
    assertEquals(Arrays.asList("123456789", "", 42, "abcd"),
                 d.getFieldValues("bar_giberish"));
    assertEquals(Arrays.asList("123456789", "", 42, "abcd"),
                 d.getFieldValues("t_raw"));
    assertFalse(d.containsKey("foo_s"));

    d = processAdd("ignore-in-schema",
                   doc(f("id", "1111"),
                       f("foo_giberish", "123456789", "", 42, "abcd"),
                       f("bar_giberish", "123456789", "", 42, "abcd"),
                       f("t_raw", "123456789", "", 42, "abcd"),
                       f("foo_s", "hoss")));
    assertNotNull(d);
    assertTrue(d.containsKey("foo_giberish"));
    assertTrue(d.containsKey("bar_giberish"));
    assertFalse(d.containsKey("id"));
    assertFalse(d.containsKey("t_raw"));
    assertFalse(d.containsKey("foo_s"));

    d = processAdd("ignore-not-in-schema-explicit-str-selector",
                   doc(f("id", "1111"),
                       f("foo_giberish", "123456789", "", 42, "abcd"),
                       f("bar_giberish", "123456789", "", 42, "abcd"),
                       f("t_raw", "123456789", "", 42, "abcd"),
                       f("foo_s", "hoss")));
    assertNotNull(d);
    assertFalse(d.containsKey("foo_giberish"));
    assertFalse(d.containsKey("bar_giberish"));
    assertEquals(Arrays.asList("123456789", "", 42, "abcd"),
                 d.getFieldValues("t_raw"));
    assertEquals("hoss", d.getFieldValue("foo_s"));

    d = processAdd("ignore-in-schema-str-selector",
                   doc(f("id", "1111"),
                       f("foo_giberish", "123456789", "", 42, "abcd"),
                       f("bar_giberish", "123456789", "", 42, "abcd"),
                       f("t_raw", "123456789", "", 42, "abcd"),
                       f("foo_s", "hoss")));
    assertNotNull(d);
    assertTrue(d.containsKey("foo_giberish"));
    assertTrue(d.containsKey("bar_giberish"));
    assertFalse(d.containsKey("id"));
    assertFalse(d.containsKey("t_raw"));
    assertFalse(d.containsKey("foo_s"));

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

  public void testConcatDefaults() throws Exception {
    SolrInputDocument d = null;
    d = processAdd("concat-defaults", 
                   doc(f("id", "1111", "222"),
                       f("attr_foo", "string1", "string2"),
                       f("foo_s1", "string3", "string4"),
                       f("bar_dt", "string5", "string6"),
                       f("bar_HOSS_s", "string7", "string8"),
                       f("foo_d", 42)));

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
                 42, d.getFieldValue("foo_d"));
   
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
                       f("foo_d", 42),
                       field("foo_s", "string3", "string4")));

    assertNotNull(d);

    assertEquals(Arrays.asList("string1","string2"), 
                 d.getFieldValues("foo_t"));
    assertEquals("string3" + delim + "string4", d.getFieldValue("foo_s"));

    // slightly more interesting
    assertEquals("processor borked non string value", 
                 42, d.getFieldValue("foo_d"));
  }

}

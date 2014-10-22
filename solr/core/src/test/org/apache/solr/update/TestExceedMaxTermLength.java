package org.apache.solr.update;

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

import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.util.TestUtil;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.SolrTestCaseJ4;

import java.util.Locale;

import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestExceedMaxTermLength extends SolrTestCaseJ4 {

  public final static String TEST_SOLRCONFIG_NAME = "solrconfig.xml";
  public final static String TEST_SCHEMAXML_NAME = "schema11.xml";

  private final static int minTestTermLength = IndexWriter.MAX_TERM_LENGTH + 1;
  private final static int maxTestTermLength = IndexWriter.MAX_TERM_LENGTH * 2;

  @BeforeClass
  public static void beforeTests() throws Exception {
    initCore(TEST_SOLRCONFIG_NAME, TEST_SCHEMAXML_NAME);
  }

  @After
  public void cleanup() throws Exception {
    assertU(delQ("*:*"));
    assertU(commit());
  }

  @Test
  public void testExceededMaxTermLength(){

    // problematic field
    final String longFieldName = "cat";
    final String longFieldValue = TestUtil.randomSimpleString(random(),
        minTestTermLength,
        maxTestTermLength);

    final String okayFieldName = TestUtil.randomSimpleString(random(), 1, 50) + "_sS" ; //Dynamic field
    final String okayFieldValue = TestUtil.randomSimpleString(random(),
        minTestTermLength,
        maxTestTermLength);

    boolean includeOkayFields = random().nextBoolean();

    if(random().nextBoolean()) {
      //Use XML
      String doc;
      if(includeOkayFields) {
        doc = adoc("id", "1", longFieldName, longFieldValue, okayFieldName, okayFieldValue);
      } else {
        doc = adoc("id", "1", longFieldName, longFieldValue);
      }
      assertFailedU(doc);
    } else {
      //Use JSON
      try {
        if(includeOkayFields) {
          String jsonStr = "[{'id':'1','%s':'%s', '%s': '%s'}]";
          jsonStr = String.format(Locale.ROOT, jsonStr, longFieldName, longFieldValue, 
                                  okayFieldName, okayFieldValue);
          updateJ(json(jsonStr), null);
        } else {
          String jsonStr = "[{'id':'1','%s':'%s'}]";
          jsonStr = String.format(Locale.ROOT, jsonStr, longFieldName, longFieldValue);
          updateJ(json(jsonStr), null);
        }
      } catch (Exception e) {
        //expected
        String msg= e.getCause().getMessage();
        assertTrue(msg.contains("one immense term in field=\"cat\""));
      }

    }

    assertU(commit());

    assertQ(req("q", "*:*"), "//*[@numFound='0']");
  }

  @Test
  public void testExceededMaxTermLengthWithLimitingFilter(){

    // problematic field
    final String longFieldName = "cat_length";
    final String longFieldValue = TestUtil.randomSimpleString(random(),
        minTestTermLength,
        maxTestTermLength);

    final String okayFieldName = TestUtil.randomSimpleString(random(), 1, 50) + "_sS" ; //Dynamic field
    final String okayFieldValue = TestUtil.randomSimpleString(random(),
        minTestTermLength,
        maxTestTermLength);

    boolean includeOkayFields = random().nextBoolean();

    if(random().nextBoolean()) {
      //Use XML
      String doc;
      if(includeOkayFields) {
        doc = adoc("id", "1", longFieldName, longFieldValue, okayFieldName, okayFieldValue);
      } else {
        doc = adoc("id", "1", longFieldName, longFieldValue);
      }
      assertU(doc);
    } else {
      //Use JSON
      String jsonStr = null;
      try {
        if(includeOkayFields) {
          jsonStr = "[{'id':'1','%s':'%s', '%s': '%s'}]";
          jsonStr = String.format(Locale.ROOT, jsonStr, longFieldName, longFieldValue, 
                                  okayFieldName, okayFieldValue);
          updateJ(json(jsonStr), null);
        } else {
          jsonStr = "[{'id':'1','%s':'%s'}]";
          jsonStr = String.format(Locale.ROOT, jsonStr, longFieldName, longFieldValue);
          updateJ(json(jsonStr), null);
        }
      } catch (Exception e) {
        fail("Should not have failed adding doc " + jsonStr);
        String msg= e.getCause().getMessage();
        assertTrue(msg.contains("one immense term in field=\"cat\""));
      }

    }

    assertU(commit());

    assertQ(req("q", "*:*"), "//*[@numFound='1']");
  }
}

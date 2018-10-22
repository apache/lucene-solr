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
package org.apache.solr.search;

import org.apache.lucene.util.TestUtil;

import org.apache.solr.schema.SchemaField;
import org.apache.solr.schema.TrieIntField;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.SolrTestCaseJ4;

import org.junit.BeforeClass;

@Deprecated
public class TestTrieFacet extends SolrTestCaseJ4 {

  final static int MIN_VALUE = 20;
  final static int MAX_VALUE = 60;
  
  final static String TRIE_INT_P8_S_VALUED = "foo_ti1";
  final static String TRIE_INT_P8_M_VALUED = "foo_ti";
  
  final static String TRIE_INT_P0_S_VALUED = "foo_i1";
  final static String TRIE_INT_P0_M_VALUED = "foo_i";

  final static String[] M_VALUED = new String[] { TRIE_INT_P0_M_VALUED, TRIE_INT_P8_M_VALUED };
  final static String[] S_VALUED = new String[] { TRIE_INT_P0_S_VALUED, TRIE_INT_P8_S_VALUED };
  
  final static String[] P0 = new String[] { TRIE_INT_P0_M_VALUED, TRIE_INT_P0_S_VALUED };
  final static String[] P8 = new String[] { TRIE_INT_P8_M_VALUED, TRIE_INT_P8_S_VALUED };
  
  static int NUM_DOCS;

  private static TrieIntField assertCastFieldType(SchemaField f) {
    assertTrue("who changed the schema? test isn't valid: " + f.getName(),
                 f.getType() instanceof TrieIntField);
    return (TrieIntField) f.getType();
  }
  
  @BeforeClass
  public static void beforeClass() throws Exception {
    // we need DVs on point fields to compute stats & facets
    if (Boolean.getBoolean(NUMERIC_POINTS_SYSPROP)) System.setProperty(NUMERIC_DOCVALUES_SYSPROP,"true");

    initCore("solrconfig-tlog.xml","schema.xml");

    // don't break the test
    assertTrue("min value must be less then max value", MIN_VALUE < MAX_VALUE);
    assertTrue("min value must be greater then zero", 0 < MIN_VALUE);
    
    // sanity check no one breaks the schema out from under us...
    for (String f : M_VALUED) {
      SchemaField sf = h.getCore().getLatestSchema().getField(f);
      assertTrue("who changed the schema? test isn't valid: " + f, sf.multiValued());
    }
    
    for (String f : S_VALUED) {
      SchemaField sf = h.getCore().getLatestSchema().getField(f);
      assertFalse("who changed the schema? test isn't valid: " + f, sf.multiValued());
    }

    if (! Boolean.getBoolean(NUMERIC_POINTS_SYSPROP)) {
      for (String f : P0) {
        SchemaField sf = h.getCore().getLatestSchema().getField(f);
        assertEquals("who changed the schema? test isn't valid: " + f,
                     0, assertCastFieldType(sf).getPrecisionStep());
      }
      for (String f : P8) {
        SchemaField sf = h.getCore().getLatestSchema().getField(f);
        assertEquals("who changed the schema? test isn't valid: " + f,
                     8, assertCastFieldType(sf).getPrecisionStep());
      }
    }
    
    // we don't need a lot of docs -- at least one failure only had ~1000  
    NUM_DOCS = TestUtil.nextInt(random(), 200, 1500);

    { // ensure at least one doc has every valid value in the multivalued fields
      SolrInputDocument doc = sdoc("id", "0");
      for (int val = MIN_VALUE; val <= MAX_VALUE; val++) {
        for (String f : M_VALUED) {
          doc.addField(f, val);
        }
      }
      assertU(adoc(doc));
    }

    // randomized docs (note: starting at i=1)
    for (int i=1; i < NUM_DOCS; i++) {
      SolrInputDocument doc = sdoc("id", i+"");
      if (useField()) {
        int val = TestUtil.nextInt(random(), MIN_VALUE, MAX_VALUE);
        for (String f : S_VALUED) {
          doc.addField(f, val);
        }
      }
      if (useField()) {
        int numMulti = atLeast(1);
        while (0 < numMulti--) {
          int val = TestUtil.nextInt(random(), MIN_VALUE, MAX_VALUE);
          for (String f: M_VALUED) {
            doc.addField(f, val);
          }
        }
      }
      assertU(adoc(doc));
    }
    assertU(commit());
  }

  /** 
   * Similar to usually() but we want it to happen just as often regardless
   * of test multiplier and nightly status
   */
  private static boolean useField() {
    return 0 != TestUtil.nextInt(random(), 0, 9); 
  }

  private static void doTestNoZeros(final String field, final String method) throws Exception {

    assertQ("sanity check # docs in index: " + NUM_DOCS,
            req("q", "*:*", "rows", "0")
            ,"//result[@numFound="+NUM_DOCS+"]");
    assertQ("sanity check that no docs match 0 failed",
            req("q", field+":0", "rows", "0")
            ,"//result[@numFound=0]");
    assertQ("sanity check that no docs match [0 TO 0] failed",
            req("q", field+":[0 TO 0]", "rows", "0")
            ,"//result[@numFound=0]");
                
    assertQ("*:* facet with mincount 0 found unexpected 0 value",
            req("q", "*:*"
                ,"rows", "0"
                ,"indent","true"
                ,"facet", "true"
                ,"facet.field", field
                ,"facet.limit", "-1"
                ,"facet.mincount", "0"
                ,"facet.method", method
                )
            // trivial sanity check we're at least getting facet counts in output
            ,"*[count(//lst[@name='facet_fields']/lst[@name='"+field+"']/int)!=0]"
            // main point of test
            ,"*[count(//lst[@name='facet_fields']/lst[@name='"+field+"']/int[@name='0'])=0]"
            );
  }

  // enum
  public void testSingleValuedTrieP0_enum() throws Exception {
    doTestNoZeros(TRIE_INT_P0_S_VALUED, "enum");
  }
  public void testMultiValuedTrieP0_enum() throws Exception {
    doTestNoZeros(TRIE_INT_P0_M_VALUED, "enum");
  }
  public void testSingleValuedTrieP8_enum() throws Exception {
    doTestNoZeros(TRIE_INT_P8_S_VALUED, "enum");
  }
  public void testMultiValuedTrieP8_enum() throws Exception {
    doTestNoZeros(TRIE_INT_P8_M_VALUED, "enum");
  }

  // fc
  public void testSingleValuedTrieP0_fc() throws Exception {
    doTestNoZeros(TRIE_INT_P0_S_VALUED, "fc");
  }
  public void testMultiValuedTrieP0_fc() throws Exception {
    doTestNoZeros(TRIE_INT_P0_M_VALUED, "fc");
  }
  public void testSingleValuedTrieP8_fc() throws Exception {
    doTestNoZeros(TRIE_INT_P8_S_VALUED, "fc");
  }
  public void testMultiValuedTrieP8_fc() throws Exception {
    doTestNoZeros(TRIE_INT_P8_M_VALUED, "fc");
  }

  // fcs
  public void testSingleValuedTrieP0_fcs() throws Exception {
    doTestNoZeros(TRIE_INT_P0_S_VALUED, "fcs");
  }
  public void testMultiValuedTrieP0_fcs() throws Exception {
    doTestNoZeros(TRIE_INT_P0_M_VALUED, "fcs");
  }
  public void testSingleValuedTrieP8_fcs() throws Exception {
    doTestNoZeros(TRIE_INT_P8_S_VALUED, "fcs");
  }
  public void testMultiValuedTrieP8_fcs() throws Exception {
    doTestNoZeros(TRIE_INT_P8_M_VALUED, "fcs");
  }
  
}


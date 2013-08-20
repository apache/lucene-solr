package org.apache.solr.schema;

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

import org.apache.lucene.util.LuceneTestCase.SuppressCodecs;
import org.apache.solr.SolrTestCaseJ4;
import org.junit.BeforeClass;

/**
 * Tests things like sorting on docvalues with missing values
 */
@SuppressCodecs({"Lucene40", "Lucene41", "Lucene42"}) // old formats cannot represent missing values
public class DocValuesMissingTest extends SolrTestCaseJ4 {
  
  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig-basic.xml", "schema-docValuesMissing.xml");
  }
  
  @Override
  public void setUp() throws Exception {
    super.setUp();
    clearIndex();
    assertU(commit());
  }
  
  /** float with default lucene sort (treats as 0) */
  public void testFloatSort() throws Exception {
    assertU(adoc("id", "0")); // missing
    assertU(adoc("id", "1", "floatdv", "-1.3"));
    assertU(adoc("id", "2", "floatdv", "4.2"));
    assertU(commit());
    assertQ(req("q", "*:*", "sort", "floatdv asc"),
        "//result/doc[1]/str[@name='id'][.=1]",
        "//result/doc[2]/str[@name='id'][.=0]",
        "//result/doc[3]/str[@name='id'][.=2]");
    assertQ(req("q", "*:*", "sort", "floatdv desc"),
        "//result/doc[1]/str[@name='id'][.=2]",
        "//result/doc[2]/str[@name='id'][.=0]",
        "//result/doc[3]/str[@name='id'][.=1]");
  }
  
  /** float with sort missing always first */
  public void testFloatSortMissingFirst() throws Exception {
    assertU(adoc("id", "0")); // missing
    assertU(adoc("id", "1", "floatdv_missingfirst", "-1.3"));
    assertU(adoc("id", "2", "floatdv_missingfirst", "4.2"));
    assertU(commit());
    assertQ(req("q", "*:*", "sort", "floatdv_missingfirst asc"),
        "//result/doc[1]/str[@name='id'][.=0]",
        "//result/doc[2]/str[@name='id'][.=1]",
        "//result/doc[3]/str[@name='id'][.=2]");
    assertQ(req("q", "*:*", "sort", "floatdv_missingfirst desc"),
        "//result/doc[1]/str[@name='id'][.=0]",
        "//result/doc[2]/str[@name='id'][.=2]",
        "//result/doc[3]/str[@name='id'][.=1]");
  }
  
  /** float with sort missing always last */
  public void testFloatSortMissingLast() throws Exception {
    assertU(adoc("id", "0")); // missing
    assertU(adoc("id", "1", "floatdv_missinglast", "-1.3"));
    assertU(adoc("id", "2", "floatdv_missinglast", "4.2"));
    assertU(commit());
    assertQ(req("q", "*:*", "sort", "floatdv_missinglast asc"),
        "//result/doc[1]/str[@name='id'][.=1]",
        "//result/doc[2]/str[@name='id'][.=2]",
        "//result/doc[3]/str[@name='id'][.=0]");
    assertQ(req("q", "*:*", "sort", "floatdv_missinglast desc"),
        "//result/doc[1]/str[@name='id'][.=2]",
        "//result/doc[2]/str[@name='id'][.=1]",
        "//result/doc[3]/str[@name='id'][.=0]");
  }
  
  /** float function query based on missing */
  public void testFloatMissingFunction() throws Exception {
    assertU(adoc("id", "0")); // missing
    assertU(adoc("id", "1", "floatdv", "-1.3"));
    assertU(adoc("id", "2", "floatdv", "4.2"));
    assertU(commit());
    assertQ(req("q", "*:*", "fl", "e:exists(floatdv)", "sort", "id asc"),
        "//result/doc[1]/bool[@name='e'][.='false']",
        "//result/doc[2]/bool[@name='e'][.='true']",
        "//result/doc[3]/bool[@name='e'][.='true']");
  }
  
  /** float missing facet count */
  public void testFloatMissingFacet() throws Exception {
    assertU(adoc("id", "0")); // missing
    assertU(adoc("id", "1")); // missing
    assertU(adoc("id", "2", "floatdv", "-1.3"));
    assertU(adoc("id", "3", "floatdv", "4.2"));
    assertU(commit());
    assertQ(req("q", "*:*", "facet", "true", "facet.field", "floatdv", "facet.mincount", "1", "facet.missing", "true"),
        "//lst[@name='facet_fields']/lst[@name='floatdv']/int[@name='-1.3'][.=1]",
        "//lst[@name='facet_fields']/lst[@name='floatdv']/int[@name='4.2'][.=1]",
        "//lst[@name='facet_fields']/lst[@name='floatdv']/int[.=2]");
  }
  
  /** int with default lucene sort (treats as 0) */
  public void testIntSort() throws Exception {
    assertU(adoc("id", "0")); // missing
    assertU(adoc("id", "1", "intdv", "-1"));
    assertU(adoc("id", "2", "intdv", "4"));
    assertU(commit());
    assertQ(req("q", "*:*", "sort", "intdv asc"),
        "//result/doc[1]/str[@name='id'][.=1]",
        "//result/doc[2]/str[@name='id'][.=0]",
        "//result/doc[3]/str[@name='id'][.=2]");
    assertQ(req("q", "*:*", "sort", "intdv desc"),
        "//result/doc[1]/str[@name='id'][.=2]",
        "//result/doc[2]/str[@name='id'][.=0]",
        "//result/doc[3]/str[@name='id'][.=1]");
  }
  
  /** int with sort missing always first */
  public void testIntSortMissingFirst() throws Exception {
    assertU(adoc("id", "0")); // missing
    assertU(adoc("id", "1", "intdv_missingfirst", "-1"));
    assertU(adoc("id", "2", "intdv_missingfirst", "4"));
    assertU(commit());
    assertQ(req("q", "*:*", "sort", "intdv_missingfirst asc"),
        "//result/doc[1]/str[@name='id'][.=0]",
        "//result/doc[2]/str[@name='id'][.=1]",
        "//result/doc[3]/str[@name='id'][.=2]");
    assertQ(req("q", "*:*", "sort", "intdv_missingfirst desc"),
        "//result/doc[1]/str[@name='id'][.=0]",
        "//result/doc[2]/str[@name='id'][.=2]",
        "//result/doc[3]/str[@name='id'][.=1]");
  }
  
  /** int with sort missing always last */
  public void testIntSortMissingLast() throws Exception {
    assertU(adoc("id", "0")); // missing
    assertU(adoc("id", "1", "intdv_missinglast", "-1"));
    assertU(adoc("id", "2", "intdv_missinglast", "4"));
    assertU(commit());
    assertQ(req("q", "*:*", "sort", "intdv_missinglast asc"),
        "//result/doc[1]/str[@name='id'][.=1]",
        "//result/doc[2]/str[@name='id'][.=2]",
        "//result/doc[3]/str[@name='id'][.=0]");
    assertQ(req("q", "*:*", "sort", "intdv_missinglast desc"),
        "//result/doc[1]/str[@name='id'][.=2]",
        "//result/doc[2]/str[@name='id'][.=1]",
        "//result/doc[3]/str[@name='id'][.=0]");
  }
  
  /** int function query based on missing */
  public void testIntMissingFunction() throws Exception {
    assertU(adoc("id", "0")); // missing
    assertU(adoc("id", "1", "intdv", "-1"));
    assertU(adoc("id", "2", "intdv", "4"));
    assertU(commit());
    assertQ(req("q", "*:*", "fl", "e:exists(intdv)", "sort", "id asc"),
        "//result/doc[1]/bool[@name='e'][.='false']",
        "//result/doc[2]/bool[@name='e'][.='true']",
        "//result/doc[3]/bool[@name='e'][.='true']");
  }
  
  /** int missing facet count */
  public void testIntMissingFacet() throws Exception {
    assertU(adoc("id", "0")); // missing
    assertU(adoc("id", "1")); // missing
    assertU(adoc("id", "2", "intdv", "-1"));
    assertU(adoc("id", "3", "intdv", "4"));
    assertU(commit());
    assertQ(req("q", "*:*", "facet", "true", "facet.field", "intdv", "facet.mincount", "1", "facet.missing", "true"),
        "//lst[@name='facet_fields']/lst[@name='intdv']/int[@name='-1'][.=1]",
        "//lst[@name='facet_fields']/lst[@name='intdv']/int[@name='4'][.=1]",
        "//lst[@name='facet_fields']/lst[@name='intdv']/int[.=2]");
  }
  
  /** double with default lucene sort (treats as 0) */
  public void testDoubleSort() throws Exception {
    assertU(adoc("id", "0")); // missing
    assertU(adoc("id", "1", "doubledv", "-1.3"));
    assertU(adoc("id", "2", "doubledv", "4.2"));
    assertU(commit());
    assertQ(req("q", "*:*", "sort", "doubledv asc"),
        "//result/doc[1]/str[@name='id'][.=1]",
        "//result/doc[2]/str[@name='id'][.=0]",
        "//result/doc[3]/str[@name='id'][.=2]");
    assertQ(req("q", "*:*", "sort", "doubledv desc"),
        "//result/doc[1]/str[@name='id'][.=2]",
        "//result/doc[2]/str[@name='id'][.=0]",
        "//result/doc[3]/str[@name='id'][.=1]");
  }
  
  /** double with sort missing always first */
  public void testDoubleSortMissingFirst() throws Exception {
    assertU(adoc("id", "0")); // missing
    assertU(adoc("id", "1", "doubledv_missingfirst", "-1.3"));
    assertU(adoc("id", "2", "doubledv_missingfirst", "4.2"));
    assertU(commit());
    assertQ(req("q", "*:*", "sort", "doubledv_missingfirst asc"),
        "//result/doc[1]/str[@name='id'][.=0]",
        "//result/doc[2]/str[@name='id'][.=1]",
        "//result/doc[3]/str[@name='id'][.=2]");
    assertQ(req("q", "*:*", "sort", "doubledv_missingfirst desc"),
        "//result/doc[1]/str[@name='id'][.=0]",
        "//result/doc[2]/str[@name='id'][.=2]",
        "//result/doc[3]/str[@name='id'][.=1]");
  }
  
  /** double with sort missing always last */
  public void testDoubleSortMissingLast() throws Exception {
    assertU(adoc("id", "0")); // missing
    assertU(adoc("id", "1", "doubledv_missinglast", "-1.3"));
    assertU(adoc("id", "2", "doubledv_missinglast", "4.2"));
    assertU(commit());
    assertQ(req("q", "*:*", "sort", "doubledv_missinglast asc"),
        "//result/doc[1]/str[@name='id'][.=1]",
        "//result/doc[2]/str[@name='id'][.=2]",
        "//result/doc[3]/str[@name='id'][.=0]");
    assertQ(req("q", "*:*", "sort", "doubledv_missinglast desc"),
        "//result/doc[1]/str[@name='id'][.=2]",
        "//result/doc[2]/str[@name='id'][.=1]",
        "//result/doc[3]/str[@name='id'][.=0]");
  }
  
  /** double function query based on missing */
  public void testDoubleMissingFunction() throws Exception {
    assertU(adoc("id", "0")); // missing
    assertU(adoc("id", "1", "doubledv", "-1.3"));
    assertU(adoc("id", "2", "doubledv", "4.2"));
    assertU(commit());
    assertQ(req("q", "*:*", "fl", "e:exists(doubledv)", "sort", "id asc"),
        "//result/doc[1]/bool[@name='e'][.='false']",
        "//result/doc[2]/bool[@name='e'][.='true']",
        "//result/doc[3]/bool[@name='e'][.='true']");
  }
  
  /** double missing facet count */
  public void testDoubleMissingFacet() throws Exception {
    assertU(adoc("id", "0")); // missing
    assertU(adoc("id", "1")); // missing
    assertU(adoc("id", "2", "doubledv", "-1.3"));
    assertU(adoc("id", "3", "doubledv", "4.2"));
    assertU(commit());
    assertQ(req("q", "*:*", "facet", "true", "facet.field", "doubledv", "facet.mincount", "1", "facet.missing", "true"),
        "//lst[@name='facet_fields']/lst[@name='doubledv']/int[@name='-1.3'][.=1]",
        "//lst[@name='facet_fields']/lst[@name='doubledv']/int[@name='4.2'][.=1]",
        "//lst[@name='facet_fields']/lst[@name='doubledv']/int[.=2]");
  }
  
  /** long with default lucene sort (treats as 0) */
  public void testLongSort() throws Exception {
    assertU(adoc("id", "0")); // missing
    assertU(adoc("id", "1", "longdv", "-1"));
    assertU(adoc("id", "2", "longdv", "4"));
    assertU(commit());
    assertQ(req("q", "*:*", "sort", "longdv asc"),
        "//result/doc[1]/str[@name='id'][.=1]",
        "//result/doc[2]/str[@name='id'][.=0]",
        "//result/doc[3]/str[@name='id'][.=2]");
    assertQ(req("q", "*:*", "sort", "longdv desc"),
        "//result/doc[1]/str[@name='id'][.=2]",
        "//result/doc[2]/str[@name='id'][.=0]",
        "//result/doc[3]/str[@name='id'][.=1]");
  }
  
  /** long with sort missing always first */
  public void testLongSortMissingFirst() throws Exception {
    assertU(adoc("id", "0")); // missing
    assertU(adoc("id", "1", "longdv_missingfirst", "-1"));
    assertU(adoc("id", "2", "longdv_missingfirst", "4"));
    assertU(commit());
    assertQ(req("q", "*:*", "sort", "longdv_missingfirst asc"),
        "//result/doc[1]/str[@name='id'][.=0]",
        "//result/doc[2]/str[@name='id'][.=1]",
        "//result/doc[3]/str[@name='id'][.=2]");
    assertQ(req("q", "*:*", "sort", "longdv_missingfirst desc"),
        "//result/doc[1]/str[@name='id'][.=0]",
        "//result/doc[2]/str[@name='id'][.=2]",
        "//result/doc[3]/str[@name='id'][.=1]");
  }
  
  /** long with sort missing always last */
  public void testLongSortMissingLast() throws Exception {
    assertU(adoc("id", "0")); // missing
    assertU(adoc("id", "1", "longdv_missinglast", "-1"));
    assertU(adoc("id", "2", "longdv_missinglast", "4"));
    assertU(commit());
    assertQ(req("q", "*:*", "sort", "longdv_missinglast asc"),
        "//result/doc[1]/str[@name='id'][.=1]",
        "//result/doc[2]/str[@name='id'][.=2]",
        "//result/doc[3]/str[@name='id'][.=0]");
    assertQ(req("q", "*:*", "sort", "longdv_missinglast desc"),
        "//result/doc[1]/str[@name='id'][.=2]",
        "//result/doc[2]/str[@name='id'][.=1]",
        "//result/doc[3]/str[@name='id'][.=0]");
  }
  
  /** long function query based on missing */
  public void testLongMissingFunction() throws Exception {
    assertU(adoc("id", "0")); // missing
    assertU(adoc("id", "1", "longdv", "-1"));
    assertU(adoc("id", "2", "longdv", "4"));
    assertU(commit());
    assertQ(req("q", "*:*", "fl", "e:exists(longdv)", "sort", "id asc"),
        "//result/doc[1]/bool[@name='e'][.='false']",
        "//result/doc[2]/bool[@name='e'][.='true']",
        "//result/doc[3]/bool[@name='e'][.='true']");
  }
  
  /** long missing facet count */
  public void testLongMissingFacet() throws Exception {
    assertU(adoc("id", "0")); // missing
    assertU(adoc("id", "1")); // missing
    assertU(adoc("id", "2", "longdv", "-1"));
    assertU(adoc("id", "3", "longdv", "4"));
    assertU(commit());
    assertQ(req("q", "*:*", "facet", "true", "facet.field", "longdv", "facet.mincount", "1", "facet.missing", "true"),
        "//lst[@name='facet_fields']/lst[@name='longdv']/int[@name='-1'][.=1]",
        "//lst[@name='facet_fields']/lst[@name='longdv']/int[@name='4'][.=1]",
        "//lst[@name='facet_fields']/lst[@name='longdv']/int[.=2]");
  }
  
  /** date with default lucene sort (treats as 1970) */
  public void testDateSort() throws Exception {
    assertU(adoc("id", "0")); // missing
    assertU(adoc("id", "1", "datedv", "1900-12-31T23:59:59.999Z"));
    assertU(adoc("id", "2", "datedv", "2005-12-31T23:59:59.999Z"));
    assertU(commit());
    assertQ(req("q", "*:*", "sort", "datedv asc"),
        "//result/doc[1]/str[@name='id'][.=1]",
        "//result/doc[2]/str[@name='id'][.=0]",
        "//result/doc[3]/str[@name='id'][.=2]");
    assertQ(req("q", "*:*", "sort", "datedv desc"),
        "//result/doc[1]/str[@name='id'][.=2]",
        "//result/doc[2]/str[@name='id'][.=0]",
        "//result/doc[3]/str[@name='id'][.=1]");
  }
  
  /** date with sort missing always first */
  public void testDateSortMissingFirst() throws Exception {
    assertU(adoc("id", "0")); // missing
    assertU(adoc("id", "1", "datedv_missingfirst", "1900-12-31T23:59:59.999Z"));
    assertU(adoc("id", "2", "datedv_missingfirst", "2005-12-31T23:59:59.999Z"));
    assertU(commit());
    assertQ(req("q", "*:*", "sort", "datedv_missingfirst asc"),
        "//result/doc[1]/str[@name='id'][.=0]",
        "//result/doc[2]/str[@name='id'][.=1]",
        "//result/doc[3]/str[@name='id'][.=2]");
    assertQ(req("q", "*:*", "sort", "datedv_missingfirst desc"),
        "//result/doc[1]/str[@name='id'][.=0]",
        "//result/doc[2]/str[@name='id'][.=2]",
        "//result/doc[3]/str[@name='id'][.=1]");
  }
  
  /** date with sort missing always last */
  public void testDateSortMissingLast() throws Exception {
    assertU(adoc("id", "0")); // missing
    assertU(adoc("id", "1", "datedv_missinglast", "1900-12-31T23:59:59.999Z"));
    assertU(adoc("id", "2", "datedv_missinglast", "2005-12-31T23:59:59.999Z"));
    assertU(commit());
    assertQ(req("q", "*:*", "sort", "datedv_missinglast asc"),
        "//result/doc[1]/str[@name='id'][.=1]",
        "//result/doc[2]/str[@name='id'][.=2]",
        "//result/doc[3]/str[@name='id'][.=0]");
    assertQ(req("q", "*:*", "sort", "datedv_missinglast desc"),
        "//result/doc[1]/str[@name='id'][.=2]",
        "//result/doc[2]/str[@name='id'][.=1]",
        "//result/doc[3]/str[@name='id'][.=0]");
  }
  
  /** date function query based on missing */
  public void testDateMissingFunction() throws Exception {
    assertU(adoc("id", "0")); // missing
    assertU(adoc("id", "1", "datedv", "1900-12-31T23:59:59.999Z"));
    assertU(adoc("id", "2", "datedv", "2005-12-31T23:59:59.999Z"));
    assertU(commit());
    assertQ(req("q", "*:*", "fl", "e:exists(datedv)", "sort", "id asc"),
        "//result/doc[1]/bool[@name='e'][.='false']",
        "//result/doc[2]/bool[@name='e'][.='true']",
        "//result/doc[3]/bool[@name='e'][.='true']");
  }
  
  /** date missing facet count */
  public void testDateMissingFacet() throws Exception {
    assertU(adoc("id", "0")); // missing
    assertU(adoc("id", "1")); // missing
    assertU(adoc("id", "2", "datedv", "1900-12-31T23:59:59.999Z"));
    assertU(adoc("id", "3", "datedv", "2005-12-31T23:59:59.999Z"));
    assertU(commit());
    assertQ(req("q", "*:*", "facet", "true", "facet.field", "datedv", "facet.mincount", "1", "facet.missing", "true"),
        "//lst[@name='facet_fields']/lst[@name='datedv']/int[@name='1900-12-31T23:59:59.999Z'][.=1]",
        "//lst[@name='facet_fields']/lst[@name='datedv']/int[@name='2005-12-31T23:59:59.999Z'][.=1]",
        "//lst[@name='facet_fields']/lst[@name='datedv']/int[.=2]");
  }
  
  /** string with default lucene sort (treats as "") */
  public void testStringSort() throws Exception {
    assertU(adoc("id", "0")); // missing
    assertU(adoc("id", "1", "stringdv", "a"));
    assertU(adoc("id", "2", "stringdv", "z"));
    assertU(commit());
    assertQ(req("q", "*:*", "sort", "stringdv asc"),
        "//result/doc[1]/str[@name='id'][.=0]",
        "//result/doc[2]/str[@name='id'][.=1]",
        "//result/doc[3]/str[@name='id'][.=2]");
    assertQ(req("q", "*:*", "sort", "stringdv desc"),
        "//result/doc[1]/str[@name='id'][.=2]",
        "//result/doc[2]/str[@name='id'][.=1]",
        "//result/doc[3]/str[@name='id'][.=0]");
  }
  
  /** string with sort missing always first */
  public void testStringSortMissingFirst() throws Exception {
    assertU(adoc("id", "0")); // missing
    assertU(adoc("id", "1", "stringdv_missingfirst", "a"));
    assertU(adoc("id", "2", "stringdv_missingfirst", "z"));
    assertU(commit());
    assertQ(req("q", "*:*", "sort", "stringdv_missingfirst asc"),
        "//result/doc[1]/str[@name='id'][.=0]",
        "//result/doc[2]/str[@name='id'][.=1]",
        "//result/doc[3]/str[@name='id'][.=2]");
    assertQ(req("q", "*:*", "sort", "stringdv_missingfirst desc"),
        "//result/doc[1]/str[@name='id'][.=0]",
        "//result/doc[2]/str[@name='id'][.=2]",
        "//result/doc[3]/str[@name='id'][.=1]");
  }
  
  /** string with sort missing always last */
  public void testStringSortMissingLast() throws Exception {
    assertU(adoc("id", "0")); // missing
    assertU(adoc("id", "1", "stringdv_missinglast", "a"));
    assertU(adoc("id", "2", "stringdv_missinglast", "z"));
    assertU(commit());
    assertQ(req("q", "*:*", "sort", "stringdv_missinglast asc"),
        "//result/doc[1]/str[@name='id'][.=1]",
        "//result/doc[2]/str[@name='id'][.=2]",
        "//result/doc[3]/str[@name='id'][.=0]");
    assertQ(req("q", "*:*", "sort", "stringdv_missinglast desc"),
        "//result/doc[1]/str[@name='id'][.=2]",
        "//result/doc[2]/str[@name='id'][.=1]",
        "//result/doc[3]/str[@name='id'][.=0]");
  }
  
  /** string function query based on missing */
  public void testStringMissingFunction() throws Exception {
    assertU(adoc("id", "0")); // missing
    assertU(adoc("id", "1", "stringdv", "a"));
    assertU(adoc("id", "2", "stringdv", "z"));
    assertU(commit());
    assertQ(req("q", "*:*", "fl", "e:exists(stringdv)", "sort", "id asc"),
        "//result/doc[1]/bool[@name='e'][.='false']",
        "//result/doc[2]/bool[@name='e'][.='true']",
        "//result/doc[3]/bool[@name='e'][.='true']");
  }
  
  /** string missing facet count */
  public void testStringMissingFacet() throws Exception {
    assertU(adoc("id", "0")); // missing
    assertU(adoc("id", "1")); // missing
    assertU(adoc("id", "2", "stringdv", "a"));
    assertU(adoc("id", "3", "stringdv", "z"));
    assertU(commit());
    assertQ(req("q", "*:*", "facet", "true", "facet.field", "stringdv", "facet.mincount", "1", "facet.missing", "true"),
        "//lst[@name='facet_fields']/lst[@name='stringdv']/int[@name='a'][.=1]",
        "//lst[@name='facet_fields']/lst[@name='stringdv']/int[@name='z'][.=1]",
        "//lst[@name='facet_fields']/lst[@name='stringdv']/int[.=2]");
  }
}

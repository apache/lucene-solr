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
package org.apache.solr.schema;

import org.apache.solr.SolrTestCaseJ4;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Tests things like sorting on docvalues with missing values
 */
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
  
  /** numeric default lucene sort (relative to presumed default value of 0) */
  private void checkSortMissingDefault(final String field,
                                       final String negative,
                                       final String positive) {
      assertU(adoc("id", "0")); // missing
      assertU(adoc("id", "1", field, negative));
      assertU(adoc("id", "2", field, positive));
      assertU(commit());
      assertQ(req("q", "*:*", "sort", field+" asc"),
              "//result/doc[1]/str[@name='id'][.=1]",
              "//result/doc[2]/str[@name='id'][.=0]",
              "//result/doc[3]/str[@name='id'][.=2]");
      assertQ(req("q", "*:*", "sort", field+" desc"),
              "//result/doc[1]/str[@name='id'][.=2]",
              "//result/doc[2]/str[@name='id'][.=0]",
              "//result/doc[3]/str[@name='id'][.=1]");
  }

  /** sort missing always first */
  private void checkSortMissingFirst(final String field,
                                     final String low,
                                     final String high) {
    assertU(adoc("id", "0")); // missing
    assertU(adoc("id", "1", field, low));
    assertU(adoc("id", "2", field, high));
    assertU(commit());
    assertQ(req("q", "*:*", "sort", field+" asc"),
            "//result/doc[1]/str[@name='id'][.=0]",
            "//result/doc[2]/str[@name='id'][.=1]",
            "//result/doc[3]/str[@name='id'][.=2]");
    assertQ(req("q", "*:*", "sort", field+" desc"),
            "//result/doc[1]/str[@name='id'][.=0]",
            "//result/doc[2]/str[@name='id'][.=2]",
            "//result/doc[3]/str[@name='id'][.=1]");
  }

  /** sort missing always last */
  private void checkSortMissingLast(final String field,
                                    final String low,
                                    final String high) {

    assertU(adoc("id", "0")); // missing
    assertU(adoc("id", "1", field, low));
    assertU(adoc("id", "2", field, high));
    assertU(commit());
    assertQ(req("q", "*:*", "sort", field+" asc"),
            "//result/doc[1]/str[@name='id'][.=1]",
            "//result/doc[2]/str[@name='id'][.=2]",
            "//result/doc[3]/str[@name='id'][.=0]");
    assertQ(req("q", "*:*", "sort", field+" desc"),
            "//result/doc[1]/str[@name='id'][.=2]",
            "//result/doc[2]/str[@name='id'][.=1]",
            "//result/doc[3]/str[@name='id'][.=0]");
    
  }

  /** function query based on missing */
  private void checkSortMissingFunction(final String field,
                                        final String low,
                                        final String high) {
    assertU(adoc("id", "0")); // missing
    assertU(adoc("id", "1", field, low));
    assertU(adoc("id", "2", field, high));
    assertU(commit());
    assertQ(req("q", "*:*", "fl", "e:exists("+field+")", "sort", "id asc"),
            "//result/doc[1]/bool[@name='e'][.='false']",
            "//result/doc[2]/bool[@name='e'][.='true']",
            "//result/doc[3]/bool[@name='e'][.='true']");
  }

  /** missing facet count */
  private void checkSortMissingFacet(final String field,
                                     final String low,
                                     final String high) {
    assertU(adoc("id", "0")); // missing
    assertU(adoc("id", "1")); // missing
    assertU(adoc("id", "2", field, low));
    assertU(adoc("id", "3", field, high));
    assertU(commit());
    assertQ(req("q", "*:*", "facet", "true", "facet.field", field, 
                "facet.mincount", "1", "facet.missing", "true"),
            "//lst[@name='facet_fields']/lst[@name='"+field+"']/int[@name='"+low+"'][.=1]",
            "//lst[@name='facet_fields']/lst[@name='"+field+"']/int[@name='"+high+"'][.=1]",
            "//lst[@name='facet_fields']/lst[@name='"+field+"']/int[.=2]");
  }

  /** float with default lucene sort (treats as 0) */
  @Test
  public void testFloatSort() throws Exception {
    checkSortMissingDefault("floatdv", "-1.3", "4.2");
  }
  /** dynamic float with default lucene sort (treats as 0) */
  @Test
  public void testDynFloatSort() throws Exception {
    checkSortMissingDefault("dyn_floatdv", "-1.3", "4.2");
  }

  /** float with sort missing always first */
  @Test
  public void testFloatSortMissingFirst() throws Exception {
    checkSortMissingFirst("floatdv_missingfirst", "-1.3", "4.2");
  }
  /** dynamic float with sort missing always first */
  @Test
  public void testDynFloatSortMissingFirst() throws Exception {
    checkSortMissingFirst("dyn_floatdv_missingfirst", "-1.3", "4.2");
  }

  /** float with sort missing always last */
  @Test
  public void testFloatSortMissingLast() throws Exception {
    checkSortMissingLast("floatdv_missinglast", "-1.3", "4.2");
  }
  /** dynamic float with sort missing always last */
  @Test
  public void testDynFloatSortMissingLast() throws Exception {
    checkSortMissingLast("dyn_floatdv_missinglast", "-1.3", "4.2");
  }
  
  /** float function query based on missing */
  @Test
  public void testFloatMissingFunction() throws Exception {
    checkSortMissingFunction("floatdv", "-1.3", "4.2");
  }
  /** dyanmic float function query based on missing */
  @Test
  public void testDynFloatMissingFunction() throws Exception {
    checkSortMissingFunction("dyn_floatdv", "-1.3", "4.2");
  }
  
  /** float missing facet count */
  @Test
  public void testFloatMissingFacet() throws Exception {
    checkSortMissingFacet("floatdv", "-1.3", "4.2");
  }
  /** dynamic float missing facet count */
  @Test
  public void testDynFloatMissingFacet() throws Exception {
    checkSortMissingFacet("dyn_floatdv", "-1.3", "4.2");
  }

  /** int with default lucene sort (treats as 0) */
  @Test
  public void testIntSort() throws Exception {
    checkSortMissingDefault("intdv", "-1", "4");
  }
  /** dynamic int with default lucene sort (treats as 0) */
  @Test
  public void testDynIntSort() throws Exception {
    checkSortMissingDefault("dyn_intdv", "-1", "4");
  }
  
  /** int with sort missing always first */
  @Test
  public void testIntSortMissingFirst() throws Exception {
    checkSortMissingFirst("intdv_missingfirst", "-1", "4");
  }
  /** dynamic int with sort missing always first */
  @Test
  public void testDynIntSortMissingFirst() throws Exception {
    checkSortMissingFirst("dyn_intdv_missingfirst", "-1", "4");
  }
  
  /** int with sort missing always last */
  @Test
  public void testIntSortMissingLast() throws Exception {
    checkSortMissingLast("intdv_missinglast", "-1", "4");
  }
  /** dynamic int with sort missing always last */
  @Test
  public void testDynIntSortMissingLast() throws Exception {
    checkSortMissingLast("dyn_intdv_missinglast", "-1", "4");
  }
  
  /** int function query based on missing */
  @Test
  public void testIntMissingFunction() throws Exception {
    checkSortMissingFunction("intdv", "-1", "4");
  }
  /** dynamic int function query based on missing */
  @Test
  public void testDynIntMissingFunction() throws Exception {
    checkSortMissingFunction("dyn_intdv", "-1", "4");
  }
  
  /** int missing facet count */
  @Test
  public void testIntMissingFacet() throws Exception {
    checkSortMissingFacet("intdv", "-1", "4");
  }
  /** dynamic int missing facet count */
  @Test
  public void testDynIntMissingFacet() throws Exception {
    checkSortMissingFacet("dyn_intdv", "-1", "4");
  }
  
  /** double with default lucene sort (treats as 0) */
  @Test
  public void testDoubleSort() throws Exception {
    checkSortMissingDefault("doubledv", "-1.3", "4.2");
  }
  /** dynamic double with default lucene sort (treats as 0) */
  @Test
  public void testDynDoubleSort() throws Exception {
    checkSortMissingDefault("dyn_doubledv", "-1.3", "4.2");
  }
  
  /** double with sort missing always first */
  @Test
  public void testDoubleSortMissingFirst() throws Exception {
    checkSortMissingFirst("doubledv_missingfirst", "-1.3", "4.2");
  }
  /** dynamic double with sort missing always first */
  @Test
  public void testDynDoubleSortMissingFirst() throws Exception {
    checkSortMissingFirst("dyn_doubledv_missingfirst", "-1.3", "4.2");
  }

  /** double with sort missing always last */
  @Test
  public void testDoubleSortMissingLast() throws Exception {
    checkSortMissingLast("doubledv_missinglast", "-1.3", "4.2");
  }
  /** dynamic double with sort missing always last */
  @Test
  public void testDynDoubleSortMissingLast() throws Exception {
    checkSortMissingLast("dyn_doubledv_missinglast", "-1.3", "4.2");
  }
  
  /** double function query based on missing */
  @Test
  public void testDoubleMissingFunction() throws Exception {
    checkSortMissingFunction("doubledv", "-1.3", "4.2");
  }
  /** dyanmic double function query based on missing */
  @Test
  public void testDynDoubleMissingFunction() throws Exception {
    checkSortMissingFunction("dyn_doubledv", "-1.3", "4.2");
  }
  
  /** double missing facet count */
  @Test
  public void testDoubleMissingFacet() throws Exception {
    checkSortMissingFacet("doubledv", "-1.3", "4.2");
  }
  /** dynamic double missing facet count */
  @Test
  public void testDynDoubleMissingFacet() throws Exception {
    checkSortMissingFacet("dyn_doubledv", "-1.3", "4.2");
  }
  
  /** long with default lucene sort (treats as 0) */
  @Test
  public void testLongSort() throws Exception {
    checkSortMissingDefault("longdv", "-1", "4");
  }
  /** dynamic long with default lucene sort (treats as 0) */
  @Test
  public void testDynLongSort() throws Exception {
    checkSortMissingDefault("dyn_longdv", "-1", "4");
  }

  /** long with sort missing always first */
  @Test
  public void testLongSortMissingFirst() throws Exception {
    checkSortMissingFirst("longdv_missingfirst", "-1", "4");
  }
  /** dynamic long with sort missing always first */
  @Test
  public void testDynLongSortMissingFirst() throws Exception {
    checkSortMissingFirst("dyn_longdv_missingfirst", "-1", "4");
  }

  /** long with sort missing always last */
  @Test
  public void testLongSortMissingLast() throws Exception {
    checkSortMissingLast("longdv_missinglast", "-1", "4");
  }
  /** dynamic long with sort missing always last */
  @Test
  public void testDynLongSortMissingLast() throws Exception {
    checkSortMissingLast("dyn_longdv_missinglast", "-1", "4");
  }
  
  /** long function query based on missing */
  @Test
  public void testLongMissingFunction() throws Exception {
    checkSortMissingFunction("longdv", "-1", "4");
  }
  /** dynamic long function query based on missing */
  @Test
  public void testDynLongMissingFunction() throws Exception {
    checkSortMissingFunction("dyn_longdv", "-1", "4");
  }
  
  /** long missing facet count */
  @Test
  public void testLongMissingFacet() throws Exception {
    checkSortMissingFacet("longdv", "-1", "4");
  }
  /** dynamic long missing facet count */
  @Test
  public void testDynLongMissingFacet() throws Exception {
    checkSortMissingFacet("dyn_longdv", "-1", "4");
  }
  
  /** date with default lucene sort (treats as 1970) */
  @Test
  public void testDateSort() throws Exception {
    checkSortMissingDefault("datedv", "1900-12-31T23:59:59.999Z", "2005-12-31T23:59:59.999Z");
  }
  /** dynamic date with default lucene sort (treats as 1970) */
  @Test
  public void testDynDateSort() throws Exception {
    checkSortMissingDefault("dyn_datedv", "1900-12-31T23:59:59.999Z", "2005-12-31T23:59:59.999Z");
  }
  
  /** date with sort missing always first */
  @Test
  public void testDateSortMissingFirst() throws Exception {
    checkSortMissingFirst("datedv_missingfirst", 
                          "1900-12-31T23:59:59.999Z", "2005-12-31T23:59:59.999Z");
  }
  /** dynamic date with sort missing always first */
  @Test
  public void testDynDateSortMissingFirst() throws Exception {
    checkSortMissingFirst("dyn_datedv_missingfirst", 
                          "1900-12-31T23:59:59.999Z", "2005-12-31T23:59:59.999Z");
  }
  
  /** date with sort missing always last */
  @Test
  public void testDateSortMissingLast() throws Exception {
    checkSortMissingLast("datedv_missinglast", 
                          "1900-12-31T23:59:59.999Z", "2005-12-31T23:59:59.999Z");
  }
  /** dynamic date with sort missing always last */
  @Test
  public void testDynDateSortMissingLast() throws Exception {
    checkSortMissingLast("dyn_datedv_missinglast", 
                         "1900-12-31T23:59:59.999Z", "2005-12-31T23:59:59.999Z");
  }
  
  /** date function query based on missing */
  @Test
  public void testDateMissingFunction() throws Exception {
    checkSortMissingFunction("datedv", 
                             "1900-12-31T23:59:59.999Z", "2005-12-31T23:59:59.999Z");
  }
  /** dynamic date function query based on missing */
  @Test
  public void testDynDateMissingFunction() throws Exception {
    checkSortMissingFunction("dyn_datedv", 
                             "1900-12-31T23:59:59.999Z", "2005-12-31T23:59:59.999Z");
  }
  
  /** date missing facet count */
  @Test
  public void testDateMissingFacet() throws Exception {
    checkSortMissingFacet("datedv", 
                          "1900-12-31T23:59:59.999Z", "2005-12-31T23:59:59.999Z");
  }
  /** dynamic date missing facet count */
  @Test
  public void testDynDateMissingFacet() throws Exception {
    checkSortMissingFacet("dyn_datedv", 
                          "1900-12-31T23:59:59.999Z", "2005-12-31T23:59:59.999Z");
  }
  
  /** string (and dynamic string) with default lucene sort (treats as "") */
  @Test
  public void testStringSort() throws Exception {

    // note: cant use checkSortMissingDefault because 
    // nothing sorts lower then the default of ""
    for (String field : new String[] {"stringdv","dyn_stringdv"}) {
      assertU(adoc("id", "0")); // missing
      assertU(adoc("id", "1", field, "a"));
      assertU(adoc("id", "2", field, "z"));
      assertU(commit());
      assertQ(req("q", "*:*", "sort", field+" asc"),
              "//result/doc[1]/str[@name='id'][.=0]",
              "//result/doc[2]/str[@name='id'][.=1]",
              "//result/doc[3]/str[@name='id'][.=2]");
      assertQ(req("q", "*:*", "sort", field+" desc"),
              "//result/doc[1]/str[@name='id'][.=2]",
              "//result/doc[2]/str[@name='id'][.=1]",
              "//result/doc[3]/str[@name='id'][.=0]");
    }
  }
  
  /** string with sort missing always first */
  @Test
  public void testStringSortMissingFirst() throws Exception {
    checkSortMissingFirst("stringdv_missingfirst", "a", "z");
  }
  /** dynamic string with sort missing always first */
  @Test
  public void testDynStringSortMissingFirst() throws Exception {
    checkSortMissingFirst("dyn_stringdv_missingfirst", "a", "z");
  }
  
  /** string with sort missing always last */
  @Test
  public void testStringSortMissingLast() throws Exception {
    checkSortMissingLast("stringdv_missinglast", "a", "z");
  }
  /** dynamic string with sort missing always last */
  @Test
  public void testDynStringSortMissingLast() throws Exception {
    checkSortMissingLast("dyn_stringdv_missinglast", "a", "z");
  }

  /** string function query based on missing */
  @Test
  public void testStringMissingFunction() throws Exception {
    checkSortMissingFunction("stringdv", "a", "z");
  }
  /** dynamic string function query based on missing */
  @Test
  public void testDynStringMissingFunction() throws Exception {
    checkSortMissingFunction("dyn_stringdv", "a", "z");
  }
  
  /** string missing facet count */
  @Test
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

  /** bool (and dynamic bool) with default lucene sort (treats as "") */
  @Test
  public void testBoolSort() throws Exception {
    // note: cant use checkSortMissingDefault because 
    // nothing sorts lower then the default of "" and
    // bool fields are, at root, string fields.
    for (String field : new String[] {"booldv","dyn_booldv"}) {
      assertU(adoc("id", "0")); // missing
      assertU(adoc("id", "1", field, "false"));
      assertU(adoc("id", "2", field, "true"));
      assertU(commit());
      assertQ(req("q", "*:*", "sort", field+" asc"),
          "//result/doc[1]/str[@name='id'][.=0]",
          "//result/doc[2]/str[@name='id'][.=1]",
          "//result/doc[3]/str[@name='id'][.=2]");
      assertQ(req("q", "*:*", "sort", field+" desc"),
          "//result/doc[1]/str[@name='id'][.=2]",
          "//result/doc[2]/str[@name='id'][.=1]",
          "//result/doc[3]/str[@name='id'][.=0]");
    }
  }

  /** bool with sort missing always first */
  @Test
  public void testBoolSortMissingFirst() throws Exception {
    checkSortMissingFirst("booldv_missingfirst", "false", "ture");
  }
  /** dynamic bool with sort missing always first */
  @Test
  public void testDynBoolSortMissingFirst() throws Exception {
    checkSortMissingFirst("dyn_booldv_missingfirst", "false", "true");
  }

  /** bool with sort missing always last */
  @Test
  public void testBoolSortMissingLast() throws Exception {
    checkSortMissingLast("booldv_missinglast", "false", "true");
  }
  /** dynamic bool with sort missing always last */
  @Test
  public void testDynBoolSortMissingLast() throws Exception {
    checkSortMissingLast("dyn_booldv_missinglast", "false", "true");
  }

  /** bool function query based on missing */
  @Test
  public void testBoolMissingFunction() throws Exception {
    checkSortMissingFunction("booldv", "false", "true");
  }
  /** dynamic bool function query based on missing */
  @Test
  public void testDynBoolMissingFunction() throws Exception {
    checkSortMissingFunction("dyn_booldv", "false", "true");
  }

  /** bool missing facet count */
  @Test
  public void testBoolMissingFacet() throws Exception {
    assertU(adoc("id", "0")); // missing
    assertU(adoc("id", "1")); // missing
    assertU(adoc("id", "2", "booldv", "false"));
    assertU(adoc("id", "3", "booldv", "true"));
    assertU(commit());
    assertQ(req("q", "*:*", "facet", "true", "facet.field", "booldv", "facet.mincount", "1", "facet.missing", "true"),
        "//lst[@name='facet_fields']/lst[@name='booldv']/int[@name='false'][.=1]",
        "//lst[@name='facet_fields']/lst[@name='booldv']/int[@name='true'][.=1]",
        "//lst[@name='facet_fields']/lst[@name='booldv']/int[.=2]");
  }

}

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
package org.apache.solr.analysis;

import org.apache.solr.SolrTestCaseJ4;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class PatternTypingFilterFactoryTest extends SolrTestCaseJ4 {

  @BeforeClass
  public static void beforeClass() throws Exception {
    assumeWorkingMockito();
    initCore("solrconfig.xml","schema-pat-typing.xml");
  }

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    clearIndex();
    assertU(commit());
  }

  @Test
  public void test() throws Exception {

    // demonstrate a nifty application of Pattern typing and test it's features

    // add some docs                                0123456789012345678901234567890   << debugging aid
    assertU(adoc("id", "1", "text", "One 401(k) two"));
    assertU(adoc("id", "2", "text", "Three 401k four"));
    assertU(adoc("id", "3", "text", "Five 501(c)(3) six"));
    assertU(adoc("id", "4", "text", "Seven 501c3 eight"));
    assertU(adoc("id", "5", "text", "nine 501k2 ten"));
    assertU(adoc("id", "6", "text", "eleven 501(k)(2) but this one goes to eleven..."));
    assertU(adoc("id", "7", "text", "nineteen twenty"));
    assertU(adoc("id", "8", "text", "twenty twenty-five"));
    assertU(adoc("id", "9", "text", "19 20"));
    assertU(adoc("id", "10", "text", "20 25"));

    assertU(commit());

    // test that docs with matches on the same pattern are synonymous
    // patterns.txt: (\d+)\(?([a-z])\)? ::: legal2_$1_$2
    assertQ("should have matched",
        req(params("q","+text:401k", "debug", "true")),
        "//result[@numFound=2]"); // 1 & 2

    // test that docs pattern capture groups can be used to distinguish the synonyms
    // patterns.txt:
    assertQ("should have matched",
        req(params("q","+text:501c3")),
        "//result[@numFound=2]"); // 3 & 4
    assertQ("should have matched",
        req(params("q","+text:501k2")),
        "//result[@numFound=2]"); // 5 & 6

    // check that flags are set and can be utilized
    assertQ("should have matched",
        req(params("q","+text:20")),
        "//result[@numFound=2]"); // 10
    assertQ("should have matched",
        req(params("q","+text:twenty")),
        "//result[@numFound=2]"); // 7 & 8
    assertQ("should have matched",
        req(params("q","+text:twenty-five")),
        "//result[@numFound=2]"); // 8 & 10 but NOT 3 or 7 because of DropIfFlagFilter
    assertQ("should have matched",
        req(params("q","+text:25")),
        "//result[@numFound=2]"); // 8 & 10

    // tokenization is still working as normal
    assertQ("should have matched",
        req(params("q","+text:k")),
        "//result[@numFound=2]"); // 1 & 6

    // Note this fails without ignoring standard types, also lower-casing is still happening
    assertQ("should have matched",
        req(params("q","+text:one")),
        "//result[@numFound=2]"); // 1 & 6

    // check can still rely on phrase queries
    assertQ("should have matched",
        req(params("q","+text:\"one 401\"")),
        "//result[@numFound=1]"); // 1
    assertQ("should have matched",
        req(params("q","+text:\"three 401k\"")),
        "//result[@numFound=1]"); // 2
  }

}

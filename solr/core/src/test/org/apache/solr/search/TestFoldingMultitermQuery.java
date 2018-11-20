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

import org.apache.solr.SolrTestCaseJ4;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestFoldingMultitermQuery extends SolrTestCaseJ4 {

  public String getCoreName() {
    return "basic";
  }

  @BeforeClass
  public static void beforeTests() throws Exception {
    initCore("solrconfig-basic.xml", "schema-folding.xml");

    String docs[] = {
        "abcdefg1 finger",
        "gangs hijklmn1",
        "opqrstu1 zilly",
    };

    // prepare the index
    for (int i = 0; i < docs.length; i++) {
      String num = Integer.toString(i);
      String boolVal = ((i % 2) == 0) ? "true" : "false";
      assertU(adoc("id", num,
          "int_f", num,
          "float_f", num,
          "long_f", num,
          "double_f", num,
          "bool_f", boolVal,
          "date_f", "200" + Integer.toString(i % 10) + "-01-01T00:00:00Z",
          "content", docs[i],
          "content_ws", docs[i],
          "content_rev", docs[i],
          "content_multi", docs[i],
          "content_lower_token", docs[i],
          "content_oldstyle", docs[i],
          "content_charfilter", docs[i],
          "content_multi_bad", docs[i],
          "content_straight", docs[i],
          "content_lower", docs[i],
          "content_folding", docs[i],
          "content_stemming", docs[i],
          "content_keyword", docs[i]
      ));
    }
    // Mixing and matching amongst various languages is probalby a bad thing, so add some tests for various
    // special filters
    int idx = docs.length;
    // Greek
    assertU(adoc("id", Integer.toString(idx++), "content_greek", "Μάϊος"));
    assertU(adoc("id", Integer.toString(idx++), "content_greek", "ΜΆΪΟΣ"));

    // Turkish

    assertU(adoc("id", Integer.toString(idx++), "content_turkish", "\u0130STANBUL"));
    assertU(adoc("id", Integer.toString(idx++), "content_turkish", "ISPARTA"));
    assertU(adoc("id", Integer.toString(idx++), "content_turkish", "izmir"));


    // Russian normalization
    assertU(adoc("id", Integer.toString(idx++), "content_russian", "электромагнитной"));
    assertU(adoc("id", Integer.toString(idx++), "content_russian", "Вместе"));
    assertU(adoc("id", Integer.toString(idx++), "content_russian", "силе"));

    // persian normalization
    assertU(adoc("id", Integer.toString(idx++), "content_persian", "هاي"));
    
    // arabic normalization
    assertU(adoc("id", Integer.toString(idx++), "content_arabic", "روبرت"));

    // hindi normalization
    assertU(adoc("id", Integer.toString(idx++), "content_hindi", "हिंदी"));
    assertU(adoc("id", Integer.toString(idx++), "content_hindi", "अाअा"));
    
    // german normalization
    assertU(adoc("id", Integer.toString(idx++), "content_german", "weissbier"));
    
    // cjk width normalization
    assertU(adoc("id", Integer.toString(idx++), "content_width", "ｳﾞｨｯﾂ"));
    assertU(commit());
  }

  @Test
  public void testPrefixCaseAccentFolding() throws Exception {
    String matchOneDocPrefixUpper[][] = {
        {"A*", "ÁB*", "ABÇ*"},   // these should find only doc 0
        {"H*", "HÏ*", "HìJ*"},   // these should find only doc 1
        {"O*", "ÖP*", "OPQ*"},   // these should find only doc 2
    };

    String matchRevPrefixUpper[][] = {
        {"*Ğ1", "*DEfG1", "*EfG1"},
        {"*N1", "*LmŊ1", "*MÑ1"},
        {"*Ǖ1", "*sTu1", "*RŠTU1"}
    };

    // test the prefix queries find only one doc where the query is uppercased. Must go through query parser here!
    for (int idx = 0; idx < matchOneDocPrefixUpper.length; idx++) {
      for (int jdx = 0; jdx < matchOneDocPrefixUpper[idx].length; jdx++) {
        String me = matchOneDocPrefixUpper[idx][jdx];
        assertQ(req("q", "content:" + me),
            "//*[@numFound='1']",
            "//*[@name='id'][.='" + Integer.toString(idx) + "']");
        assertQ(req("q", "content_ws:" + me),
            "//*[@numFound='1']",
            "//*[@name='id'][.='" + Integer.toString(idx) + "']");
        assertQ(req("q", "content_multi:" + me),
            "//*[@numFound='1']",
            "//*[@name='id'][.='" + Integer.toString(idx) + "']");
        assertQ(req("q", "content_lower_token:" + me),
            "//result[@numFound='1']",
            "//*[@name='id'][.='" + Integer.toString(idx) + "']");
        assertQ(req("q", "content_oldstyle:" + me),
            "//result[@numFound='0']");
      }
    }
    for (int idx = 0; idx < matchRevPrefixUpper.length; idx++) {
      for (int jdx = 0; jdx < matchRevPrefixUpper[idx].length; jdx++) {
        String me = matchRevPrefixUpper[idx][jdx];
        assertQ(req("q", "content_rev:" + me),
            "//*[@numFound='1']",
            "//*[@name='id'][.='" + Integer.toString(idx) + "']");
      }
    }
  }

  // test the wildcard queries find only one doc  where the query is uppercased and/or accented.
  @Test
  public void testWildcardCaseAccentFolding() throws Exception {
    String matchOneDocWildUpper[][] = {
        {"Á*C*", "ÁB*1", "ABÇ*g1", "Á*FG1"},      // these should find only doc 0
        {"H*k*", "HÏ*l?*", "HìJ*n*", "HìJ*m*"},   // these should find only doc 1
        {"O*ř*", "ÖP*ş???", "OPQ*S?Ů*", "ÖP*1"},  // these should find only doc 2
    };

    for (int idx = 0; idx < matchOneDocWildUpper.length; idx++) {
      for (int jdx = 0; jdx < matchOneDocWildUpper[idx].length; jdx++) {
        String me = matchOneDocWildUpper[idx][jdx];
        assertQ("Error with " + me, req("q", "content:" + me),
            "//result[@numFound='1']",
            "//*[@name='id'][.='" + Integer.toString(idx) + "']");
        assertQ(req("q", "content_ws:" + me),
            "//result[@numFound='1']",
            "//*[@name='id'][.='" + Integer.toString(idx) + "']");
        assertQ(req("q", "content_multi:" + me),
            "//result[@numFound='1']",
            "//*[@name='id'][.='" + Integer.toString(idx) + "']");
        assertQ(req("q", "content_oldstyle:" + me),
            "//result[@numFound='0']");
      }
    }
  }

  @Test
  public void testFuzzy() throws Exception {
    assertQ(req("q", "content:ZiLLx~1"),
            "//result[@numFound='1']");
    assertQ(req("q", "content_straight:ZiLLx~1"),      // case preserving field shouldn't match
           "//result[@numFound='0']");
    assertQ(req("q", "content_folding:ZiLLx~1"),       // case preserving field shouldn't match
           "//result[@numFound='0']");
  }

  @Test
  public void testRegex() throws Exception {
    assertQ(req("q", "content:/Zill[a-z]/"),
        "//result[@numFound='1']");
    assertQ(req("q", "content:/Zill[A-Z]/"),   // everything in the regex gets lowercased?
        "//result[@numFound='1']");
    assertQ(req("q", "content_keyword:/.*Zill[A-Z]/"),
        "//result[@numFound='1']");

    assertQ(req("q", "content_straight:/Zill[a-z]/"),      // case preserving field shouldn't match
        "//result[@numFound='0']");
    assertQ(req("q", "content_folding:/Zill[a-z]/"),       // case preserving field shouldn't match
        "//result[@numFound='0']");

    assertQ(req("q", "content_keyword:/Abcdefg1 Finger/"), // test spaces
        "//result[@numFound='1']");

  }



  @Test
  public void testGeneral() throws Exception {
    assertQ(req("q", "content_stemming:fings*"), "//result[@numFound='0']"); // should not match (but would if fings* was stemmed to fing*
    assertQ(req("q", "content_stemming:fing*"), "//result[@numFound='1']");
  }

  // Phrases should fail. This test is mainly a marker so if phrases ever do start working with wildcards we go
  // and update the documentation
  @Test
  public void testPhrase() {
    assertQ(req("q", "content:\"silly ABCD*\""),
        "//result[@numFound='0']");
  }

  @Test
  public void testWildcardRange() {
    assertQ(req("q", "content:[* TO *]"),
        "//result[@numFound='3']");
    assertQ(req("q", "content:[AB* TO Z*]"),
        "//result[@numFound='3']");
    assertQ(req("q", "content:[AB*E?G* TO TU*W]"),
        "//result[@numFound='3']");
  }


  // Does the char filter get correctly handled?
  @Test
  public void testCharFilter() {
    assertQ(req("q", "content_charfilter:" + "Á*C*"),
        "//result[@numFound='1']",
        "//*[@name='id'][.='0']");
    assertQ(req("q", "content_charfilter:" + "ABÇ*g1"),
        "//result[@numFound='1']",
        "//*[@name='id'][.='0']");
    assertQ(req("q", "content_charfilter:" + "HÏ*l?*"),
        "//result[@numFound='1']",
        "//*[@name='id'][.='1']");
  }

  @Test
  public void testRangeQuery() {
    assertQ(req("q", "content:" + "{Ȫp*1 TO QŮ*}"),
        "//result[@numFound='1']",
        "//*[@name='id'][.='2']");

    assertQ(req("q", "content:" + "[Áb* TO f?Ñg?r]"),
        "//result[@numFound='1']",
        "//*[@name='id'][.='0']");

  }

  @Test
  public void testNonTextTypes() {
    String[] intTypes = {"int_f", "float_f", "long_f", "double_f"};

    for (String str : intTypes) {
      assertQ(req("q", str + ":" + "0"),
          "//result[@numFound='1']",
          "//*[@name='id'][.='0']");

      assertQ(req("q", str + ":" + "[0 TO 2]"),
          "//result[@numFound='3']",
          "//*[@name='id'][.='0']",
          "//*[@name='id'][.='1']",
          "//*[@name='id'][.='2']");
    }
    assertQ(req("q", "bool_f:true"),
        "//result[@numFound='2']",
        "//*[@name='id'][.='0']",
        "//*[@name='id'][.='2']");

    assertQ(req("q", "bool_f:[false TO true]"),
        "//result[@numFound='3']",
        "//*[@name='id'][.='0']",
        "//*[@name='id'][.='1']",
        "//*[@name='id'][.='2']");

    assertQ(req("q", "date_f:2000-01-01T00\\:00\\:00Z"),
        "//result[@numFound='1']",
        "//*[@name='id'][.='0']");

    assertQ(req("q", "date_f:[2000-12-31T23:59:59.999Z TO 2002-01-02T00:00:01Z]"),
        "//result[@numFound='2']",
        "//*[@name='id'][.='1']",
        "//*[@name='id'][.='2']");
  }

  @Test
  public void testMultiBad() {
    try {
      ignoreException("analyzer returned too many terms");
      Exception expected = expectThrows(Exception.class, "Should throw exception when token evaluates to more than one term",
          () -> assertQ(req("q", "content_multi_bad:" + "abCD*"))
      );
      assertTrue(expected.getCause() instanceof org.apache.solr.common.SolrException);
    } finally {
      resetExceptionIgnores();
    }
  }
  @Test
  public void testGreek() {
    assertQ(req("q", "content_greek:μαιο*"), "//result[@numFound='2']");
    assertQ(req("q", "content_greek:ΜΆΪΟ*"), "//result[@numFound='2']");
    assertQ(req("q", "content_greek:Μάϊο*"), "//result[@numFound='2']");
  }
  @Test
  public void testRussian() {
    assertQ(req("q", "content_russian:элЕктРомагн*тной"), "//result[@numFound='1']");
    assertQ(req("q", "content_russian:Вме*те"), "//result[@numFound='1']");
    assertQ(req("q", "content_russian:Си*е"), "//result[@numFound='1']");
    assertQ(req("q", "content_russian:эЛектромагнИт*"), "//result[@numFound='1']");
  }
  
  public void testPersian() {
    assertQ(req("q", "content_persian:های*"), "//result[@numFound='1']");
  }
  
  public void testArabic() {
    assertQ(req("q", "content_arabic:روبرـــــــــــــــــــــــــــــــــت*"), "//result[@numFound='1']");
  }
  
  public void testHindi() {
    assertQ(req("q", "content_hindi:हिन्दी*"), "//result[@numFound='1']");
    assertQ(req("q", "content_hindi:आआ*"), "//result[@numFound='1']");
  }
  
  public void testGerman() {
    assertQ(req("q", "content_german:weiß*"), "//result[@numFound='1']");
  }
  
  public void testCJKWidth() {
    assertQ(req("q", "content_width:ヴィ*"), "//result[@numFound='1']");
  }
}

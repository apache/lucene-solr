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

import java.util.Arrays;

import org.apache.lucene.search.Query;
import org.apache.solr.SolrTestCaseJ4;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestMultiWordSynonyms extends SolrTestCaseJ4 {

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig.xml", "schema-multiword-synonyms.xml");
    index();
  }

  private static void index() throws Exception {
    assertU(adoc("id","1", "text","USA Today"));
    assertU(adoc("id","2", "text","A dynamic US economy"));
    assertU(adoc("id","3", "text","The United States of America's 50 states"));
    assertU(adoc("id","4", "text","Party in the U.S.A."));
    assertU(adoc("id","5", "text","These United States"));

    assertU(adoc("id","6", "text","America United of States"));
    assertU(adoc("id","7", "text","States United"));

    assertU(commit());
  }

  @Test
  public void testNonPhrase() throws Exception {
    // Don't split on whitespace (sow=false)
    for (String q : Arrays.asList("US", "U.S.", "USA", "U.S.A.", "United States", "United States of America")) {
      for (String defType : Arrays.asList("lucene", "edismax")) {
        assertJQ(req("q", q,
            "defType", defType,
            "df", "text",
            "sow", "false")
            , "/response/numFound==7"
        );
      }
    }

    // Split on whitespace (sow=true)
    for (String q : Arrays.asList("US", "U.S.", "USA", "U.S.A.")) {
      for (String defType : Arrays.asList("lucene", "edismax")) {
        assertJQ(req("q", q,
            "defType", defType,
            "df", "text",
            "sow", "true")
            , "/response/numFound==7"
        );
      }
    }
    for (String q : Arrays.asList("United States", "United States of America")) {
      for (String defType : Arrays.asList("lucene", "edismax")) {
        assertJQ(req("q", q,
            "defType", defType,
            "df", "text",
            "sow", "true")
            , "/response/numFound==4"
        );
      }
    }
  }

  @Test
  public void testPhrase() throws Exception {
    for (String q : Arrays.asList
        ("\"US\"", "\"U.S.\"", "\"USA\"", "\"U.S.A.\"", "\"United States\"", "\"United States of America\"")) {
      for (String defType : Arrays.asList("lucene", "edismax")) {
        for (String sow : Arrays.asList("true", "false")) {
          assertJQ(req("q", q,
              "defType", defType,
              "df", "text",
              "sow", sow)
              , "/response/numFound==5"
          );
        }
      }
    }
  }

  @Test
  public void testPf() throws Exception {
    // test phrase fields including pf2 pf3 and phrase slop
    // same as edismax test, but "bar" is synonym for "tropical cyclone" here
    assertU(adoc("id", "10", "text", "foo bar a b c", "boost_d", "1.0"));
    assertU(adoc("id", "11", "text", "foo a bar b c", "boost_d", "2.0"));
    assertU(adoc("id", "12", "text", "foo a b bar c", "boost_d", "3.0"));
    assertU(adoc("id", "13", "text", "foo a b c bar", "boost_d", "4.0"));
    assertU(commit());

    assertQ("default order assumption wrong",
        req("q", "foo bar",
            "qf", "text",
            "bf", "boost_d",
            "fl", "score,*",
            "defType", "edismax"),
        "//doc[1]/str[@name='id'][.='13']",
        "//doc[2]/str[@name='id'][.='12']",
        "//doc[3]/str[@name='id'][.='11']",
        "//doc[4]/str[@name='id'][.='10']");

    assertQ("default order assumption wrong",
        req("q", "foo tropical cyclone",
            "qf", "text",
            "bf", "boost_d",
            "fl", "score,*",
            "defType", "edismax"),
        "//doc[1]/str[@name='id'][.='13']",
        "//doc[2]/str[@name='id'][.='12']",
        "//doc[3]/str[@name='id'][.='11']",
        "//doc[4]/str[@name='id'][.='10']");

    assertQ("pf not working",
        req("q", "foo bar",
            "qf", "text",
            "pf", "text^10",
            "fl", "score,*",
            "bf", "boost_d",
            "defType", "edismax"),
        "//doc[1]/str[@name='id'][.='10']");

    assertQ("pf not working",
        req("q", "foo tropical cyclone",
            "qf", "text",
            "pf", "text^10",
            "fl", "score,*",
            "bf", "boost_d",
            "defType", "edismax"),
        "//doc[1]/str[@name='id'][.='10']");

    assertQ("pf2 not working",
        req("q", "foo bar",
            "qf", "text",
            "pf2", "text^10",
            "fl", "score,*",
            "bf", "boost_d",
            "defType", "edismax"),
        "//doc[1]/str[@name='id'][.='10']");

    assertQ("pf3 not working",
        req("q", "a b bar",
            "qf", "text",
            "pf3", "text^10",
            "fl", "score,*",
            "bf", "boost_d",
            "defType", "edismax"),
        "//doc[1]/str[@name='id'][.='12']");

    assertQ("pf3 not working",
        req("q", "a b tropical cyclone",
            "qf", "text",
            "pf3", "text^10",
            "fl", "score,*",
            "bf", "boost_d",
            "defType", "edismax"),
        "//doc[1]/str[@name='id'][.='12']");

    assertQ("ps not working for pf2",
        req("q", "bar foo",
            "qf", "text",
            "pf2", "text^10",
            "ps", "2",
            "fl", "score,*",
            "defType", "edismax"),
        "//doc[1]/str[@name='id'][.='10']");

    assertQ("ps not working for pf2",
        req("q", "tropical cyclone foo",
            "qf", "text",
            "pf2", "text^10",
            "ps", "2",
            "fl", "score,*",
            "defType", "edismax"),
        "//doc[1]/str[@name='id'][.='10']");
  }

  @Test
  public void testPf3WithReordering() throws Exception {
    // test pf3 and phrase slop
    assertU(adoc("id", "20", "text", "chicken 1 2 3 4 5 pig 1 2 3 4 5  anteater bunny cow", "boost_d", "1.0"));
    assertU(adoc("id", "21", "text", "chicken anteater pig bunny cow", "boost_d", "2.0"));
    assertU(adoc("id", "22", "text", "chicken 1 2 3 4 5 anteater bunny 1 2 3 4 5 pig cow", "boost_d", "3.0"));
    assertU(adoc("id", "23", "text", "chicken 1 2 3 4 5 anteater bunny cow 1 2 3 4 5 pig", "boost_d", "4.0"));
    assertU(commit());

    assertQ("ps not working for pf3",
        req("q", "anteater chicken pig",
            "qf", "text",
            "bf", "boost_d",
            "pf3", "text^10",
            "ps", "6",
            "fl", "score,*",
            "debugQuery", "true",
            "defType", "edismax"),
        "//doc[1]/str[@name='id'][.='21']");
  }

 @Test
  public void testPf3WithoutReordering() throws Exception {
    // test pf3 and phrase slop
    assertU(adoc("id", "20", "text", "anteater 1 2 3 4 5 pig 1 2 3 4 5  chicken bunny pig", "boost_d", "1.0"));
    assertU(adoc("id", "21", "text", "anteater 1 2 chicken 1 2 pig bunny cow", "boost_d", "2.0"));
    assertU(adoc("id", "22", "text", "chicken 1 2 3 4 5 anteater bunny 1 2 3 4 5 pig cow", "boost_d", "3.0"));
    assertU(adoc("id", "23", "text", "chicken 1 2 3 4 5 anteater bunny cow 1 2 3 4 5 pig", "boost_d", "4.0"));
    assertU(commit());

    assertQ("ps not working for pf3",
        req("q", "anteater chicken pig",
            "qf", "text",
            "bf", "boost_d",
            "pf3", "text^10",
            "ps", "6",
            "fl", "score,*",
            "debugQuery", "true",
            "defType", "edismax"),
        "//doc[1]/str[@name='id'][.='21']");
  }

  public void testEdismaxQueryParsing_multiTermWithPf_shouldParseCorrectPhraseQueries() throws Exception {
    Query q = QParser.getParser("foo a b bar","edismax",true,
        req(params("sow", "false","qf", "text^10","pf", "text^10","pf2", "text^5","pf3", "text^8"))).getQuery();
    assertEquals("+(" +
        "((text:foo)^10.0) ((text:a)^10.0) ((text:b)^10.0) (((+text:tropical +text:cyclone) text:bar)^10.0)) " +
        "((spanNear([text:foo, text:a, text:b, spanOr([spanNear([text:tropical, text:cyclone], 0, true), text:bar])], 0, true))^10.0) " +
        "(((text:\"foo a\")^5.0) ((text:\"a b\")^5.0) ((spanNear([text:b, spanOr([spanNear([text:tropical, text:cyclone], 0, true), text:bar])], 0, true))^5.0)) " +
        "(((text:\"foo a b\")^8.0) ((spanNear([text:a, text:b, spanOr([spanNear([text:tropical, text:cyclone], 0, true), text:bar])], 0, true))^8.0))", q.toString());

    q = QParser.getParser("tropical cyclone foo a b ","edismax",true, req(params("qf", "text^10","pf", "text^10","pf2", "text^5","pf3", "text^8"))).getQuery();
    assertEquals("+(" +
        "((text:bar (+text:tropical +text:cyclone))^10.0) ((text:foo)^10.0) ((text:a)^10.0) ((text:b)^10.0)) " +
        "((spanNear([spanOr([text:bar, spanNear([text:tropical, text:cyclone], 0, true)]), text:foo, text:a, text:b], 0, true))^10.0) " +
        "(((spanOr([text:bar, spanNear([text:tropical, text:cyclone], 0, true)]))^5.0) ((text:\"cyclone foo\")^5.0) ((text:\"foo a\")^5.0) ((text:\"a b\")^5.0)) " +
        "(((spanNear([spanOr([text:bar, spanNear([text:tropical, text:cyclone], 0, true)]), text:foo], 0, true))^8.0) ((text:\"cyclone foo a\")^8.0) ((text:\"foo a b\")^8.0))", q.toString());

    q = QParser.getParser("foo a b tropical cyclone","edismax",true, req(params("qf", "text^10","pf", "text^10","pf2", "text^5","pf3", "text^8"))).getQuery();
    assertEquals("+(" +
        "((text:foo)^10.0) ((text:a)^10.0) ((text:b)^10.0) ((text:bar (+text:tropical +text:cyclone))^10.0)) " +
        "((spanNear([text:foo, text:a, text:b, spanOr([text:bar, spanNear([text:tropical, text:cyclone], 0, true)])], 0, true))^10.0) " +
        "(((text:\"foo a\")^5.0) ((text:\"a b\")^5.0) ((text:\"b tropical\")^5.0) ((spanOr([text:bar, spanNear([text:tropical, text:cyclone], 0, true)]))^5.0)) " +
        "(((text:\"foo a b\")^8.0) ((text:\"a b tropical\")^8.0) ((spanNear([text:b, spanOr([text:bar, spanNear([text:tropical, text:cyclone], 0, true)])], 0, true))^8.0))", q.toString());
  }
}

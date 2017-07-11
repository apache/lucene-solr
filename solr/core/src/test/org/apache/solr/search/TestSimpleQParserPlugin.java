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

/** Simple tests for SimpleQParserPlugin. */
public class TestSimpleQParserPlugin extends SolrTestCaseJ4 {
  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig-basic.xml","schema-simpleqpplugin.xml");
    index();
  }

  public static void index() throws Exception {
    assertU(adoc("id", "42", "text0", "t0 t0 t0", "text1", "t0 t1 t2", "text-keyword0", "kw0 kw0 kw0"));
    assertU(adoc("id", "43", "text0", "t0 t1 t2", "text1", "t3 t4 t5", "text-keyword0", "kw0 kw1 kw2"));
    assertU(adoc("id", "44", "text0", "t0 t1 t1", "text1", "t6 t7 t8", "text-keyword0", "kw3 kw4 kw5"));
    assertU(adoc("id", "45", "text0", "t0 t0 t1", "text1", "t9 t10 t11", "text-keyword0", "kw6 kw7 kw8"));
    assertU(adoc("id", "46", "text0", "t1 t1 t1", "text1", "t12 t13 t14", "text-keyword0", "kw9 kw10 kw11"));
    assertU(adoc("id", "47", "text0", "and", "text1", "+", "text-keyword0", "+"));
    assertU(adoc("id", "48", "text0", "not", "text1", "-", "text-keyword0", "-"));
    assertU(adoc("id", "49", "text0", "or", "text1", "|", "text-keyword0", "|"));
    assertU(adoc("id", "50", "text0", "prefix", "text1", "t*", "text-keyword0", "kw*"));
    assertU(adoc("id", "51", "text0", "phrase", "text1", "\"", "text-keyword0", "\""));
    assertU(adoc("id", "52", "text0", "open", "text1", "(", "text-keyword0", "("));
    assertU(adoc("id", "53", "text0", "close", "text1", ")", "text-keyword0", ")"));
    assertU(adoc("id", "54", "text0", "escape", "text1", "\\", "text-keyword0", "\\"));
    assertU(adoc("id", "55", "text0", "whitespace", "text1", "whitespace", "text-keyword0", " "));
    assertU(adoc("id", "56", "text0", "whitespace", "text1", "whitespace", "text-keyword0", "\n"));
    assertU(adoc("id", "57", "text0", "foobar", "text1", "foo bar", "text-keyword0", "fb"));
    assertU(adoc("id", "58", "text-query0", "HELLO"));
    assertU(adoc("id", "59", "text-query0", "hello"));
    assertU(commit());
  }

  @Test
  public void testQueryFields() throws Exception {
    assertJQ(req("defType", "simple", "qf", "text0^2 text1 text-keyword0", "q", "t3"), "/response/numFound==1");
    assertJQ(req("defType", "simple", "qf", "text0^3 text1^4 text-keyword0^0.55", "q", "t0"), "/response/numFound==4");
    assertJQ(req("defType", "simple", "qf", "text-keyword0^9.2", "q", "\"kw9 kw10 kw11\""), "/response/numFound==1");
    assertJQ(req("defType", "simple", "qf", "text-keyword0", "q", "kw9 kw10 kw11"), "/response/numFound==0");
    assertJQ(req("defType", "simple", "qf", "text1 text-keyword0", "q", "kw9"), "/response/numFound==0");
    assertJQ(req("defType", "simple", "qf", "text0", "q", "t2"), "/response/numFound==1");
    assertJQ(req("defType", "simple", "qf", "text0^1.1 text1^0.9", "q", "t2 t9 t12"), "/response/numFound==4");
  }

  @Test
  public void testDefaultField() throws Exception {
    assertJQ(req("defType", "simple", "df", "text0", "q", "t2 t9 t12"), "/response/numFound==1");
    assertJQ(req("defType", "simple", "df", "text0", "q", "t3"), "/response/numFound==0");
    assertJQ(req("defType", "simple", "df", "text1", "q", "t2 t9 t12"), "/response/numFound==3");
    assertJQ(req("defType", "simple", "df", "text1", "q", "t3"), "/response/numFound==1");
    assertJQ(req("defType", "simple", "df", "text-keyword0", "q", "\"kw9 kw10 kw11\""), "/response/numFound==1");
    assertJQ(req("defType", "simple", "df", "text-keyword0", "q", "kw9 kw10 kw11"), "/response/numFound==0");
  }

  @Test
  public void testQueryFieldPriority() throws Exception {
    assertJQ(req("defType", "simple", "qf", "text0^2 text1 text-keyword0", "df", "text0", "q", "t3"), "/response/numFound==1");
  }

  @Test
  public void testOnlyAndOperatorEnabledDisabled() throws Exception {
    assertJQ(req("defType", "simple", "qf", "text0 text1 text-keyword0", "q", "+",
        "q.operators", "NOT, OR, PHRASE, PREFIX, PRECEDENCE, ESCAPE, WHITESPACE"), "/response/numFound==1");
    assertJQ(req("defType", "simple", "qf", "text0 text1 text-keyword0", "q", "-",
        "q.operators", "NOT, OR, PHRASE, PREFIX, PRECEDENCE, ESCAPE, WHITESPACE"), "/response/numFound==0");
    assertJQ(req("defType", "simple", "qf", "text0 text1 text-keyword0", "q", "+",
        "q.operators", "AND"), "/response/numFound==0");
    assertJQ(req("defType", "simple", "qf", "text0 text1 text-keyword0", "q", "-",
        "q.operators", "AND"), "/response/numFound==1");
  }

  @Test
  public void testOnlyNotOperatorEnabledDisabled() throws Exception {
    assertJQ(req("defType", "simple", "qf", "text0 text1 text-keyword0", "q", "-",
        "q.operators", "AND, OR, PHRASE, PREFIX, PRECEDENCE, ESCAPE, WHITESPACE"), "/response/numFound==1");
    assertJQ(req("defType", "simple", "qf", "text0 text1 text-keyword0", "q", "|",
        "q.operators", "AND, OR, PHRASE, PREFIX, PRECEDENCE, ESCAPE, WHITESPACE"), "/response/numFound==0");
    assertJQ(req("defType", "simple", "qf", "text0 text1 text-keyword0", "q", "-",
        "q.operators", "NOT"), "/response/numFound==0");
    assertJQ(req("defType", "simple", "qf", "text0 text1 text-keyword0", "q", "|",
        "q.operators", "NOT"), "/response/numFound==1");
  }

  @Test
  public void testOnlyOrOperatorEnabledDisabled() throws Exception {
    assertJQ(req("defType", "simple", "qf", "text0 text1 text-keyword0", "q", "|",
        "q.operators", "AND, NOT, PHRASE, PREFIX, PRECEDENCE, ESCAPE, WHITESPACE"), "/response/numFound==1");
    assertJQ(req("defType", "simple", "qf", "text0 text1 text-keyword0", "q", "\"",
        "q.operators", "AND, NOT, PHRASE, PREFIX, PRECEDENCE, ESCAPE, WHITESPACE"), "/response/numFound==0");
    assertJQ(req("defType", "simple", "qf", "text0 text1 text-keyword0", "q", "|",
        "q.operators", "OR"), "/response/numFound==0");
    assertJQ(req("defType", "simple", "qf", "text0 text1 text-keyword0", "q", "\"",
        "q.operators", "OR"), "/response/numFound==1");
  }

  @Test
  public void testOnlyPhraseOperatorEnabledDisabled() throws Exception {
    assertJQ(req("defType", "simple", "qf", "text0 text1 text-keyword0", "q", "\"",
        "q.operators", "AND, NOT, OR, PREFIX, PRECEDENCE, ESCAPE, WHITESPACE"), "/response/numFound==1");
    assertJQ(req("defType", "simple", "qf", "text0 text1 text-keyword0", "q", "|",
        "q.operators", "AND, NOT, OR, PREFIX, PRECEDENCE, ESCAPE, WHITESPACE"), "/response/numFound==0");
    assertJQ(req("defType", "simple", "qf", "text0 text1 text-keyword0", "q", "\"",
        "q.operators", "PHRASE"), "/response/numFound==0");
    assertJQ(req("defType", "simple", "qf", "text0 text1 text-keyword0", "q", "|",
        "q.operators", "PHRASE"), "/response/numFound==1");
  }

  @Test
  public void testOnlyPrefixOperatorEnabledDisabled() throws Exception {
    assertJQ(req("defType", "simple", "qf", "text0 text1 text-keyword0", "q", "t*",
        "q.operators", "AND, NOT, OR, PHRASE, PRECEDENCE, ESCAPE, WHITESPACE"), "/response/numFound==1");
    assertJQ(req("defType", "simple", "qf", "text0 text1 text-keyword0", "q", "(",
        "q.operators", "AND, NOT, OR, PHRASE, PRECEDENCE, ESCAPE, WHITESPACE"), "/response/numFound==0");
    assertJQ(req("defType", "simple", "qf", "text0 text1 text-keyword0", "q", "t*",
        "q.operators", "PREFIX"), "/response/numFound==6");
    assertJQ(req("defType", "simple", "qf", "text0 text1 text-keyword0", "q", "(",
        "q.operators", "PREFIX"), "/response/numFound==1");
  }

  @Test
  public void testOnlyPrecedenceOperatorEnabledDisabled() throws Exception {
    assertJQ(req("defType", "simple", "qf", "text0 text1 text-keyword0", "q", "(",
        "q.operators", "AND, NOT, OR, PHRASE, PREFIX, ESCAPE, WHITESPACE"), "/response/numFound==1");
    assertJQ(req("defType", "simple", "qf", "text0 text1 text-keyword0", "q", "\\",
        "q.operators", "AND, NOT, OR, PHRASE, PREFIX, ESCAPE, WHITESPACE"), "/response/numFound==0");
    assertJQ(req("defType", "simple", "qf", "text0 text1 text-keyword0", "q", "(",
        "q.operators", "PRECEDENCE"), "/response/numFound==0");
    assertJQ(req("defType", "simple", "qf", "text0 text1 text-keyword0", "q", "\\",
        "q.operators", "PRECEDENCE"), "/response/numFound==1");

    assertJQ(req("defType", "simple", "qf", "text0 text1 text-keyword0", "q", ")",
        "q.operators", "AND, NOT, OR, PHRASE, PREFIX, ESCAPE, WHITESPACE"), "/response/numFound==1");
    assertJQ(req("defType", "simple", "qf", "text0 text1 text-keyword0", "q", "\\",
        "q.operators", "AND, NOT, OR, PHRASE, PREFIX, ESCAPE, WHITESPACE"), "/response/numFound==0");
    assertJQ(req("defType", "simple", "qf", "text0 text1 text-keyword0", "q", ")",
        "q.operators", "PRECEDENCE"), "/response/numFound==0");
    assertJQ(req("defType", "simple", "qf", "text0 text1 text-keyword0", "q", "\\",
        "q.operators", "PRECEDENCE"), "/response/numFound==1");
  }

  @Test
  public void testOnlyEscapeOperatorEnabledDisabled() throws Exception {
    assertJQ(req("defType", "simple", "qf", "text0 text1 text-keyword0", "q", "\\",
        "q.operators", "AND, NOT, OR, PHRASE, PREFIX, PRECEDENCE, WHITESPACE"), "/response/numFound==1");
    assertJQ(req("defType", "simple", "qf", "text0 text1 text-keyword0", "q", "\n",
        "q.operators", "AND, NOT, OR, PHRASE, PREFIX, PRECEDENCE, WHITESPACE"), "/response/numFound==0");
    assertJQ(req("defType", "simple", "qf", "text0 text1 text-keyword0", "q", "\\",
        "q.operators", "ESCAPE"), "/response/numFound==0");
    assertJQ(req("defType", "simple", "qf", "text0 text1 text-keyword0", "q", "\n",
        "q.operators", "ESCAPE"), "/response/numFound==1");
  }

  @Test
  public void testOnlyWhitespaceOperatorEnabledDisabled() throws Exception {
    assertJQ(req("defType", "simple", "qf", "text0 text1 text-keyword0", "q", "\n",
        "q.operators", "AND, NOT, OR, PHRASE, PREFIX, PRECEDENCE, ESCAPE"), "/response/numFound==1");
    assertJQ(req("defType", "simple", "qf", "text0 text1 text-keyword0", "q", "\\",
        "q.operators", "AND, NOT, OR, PHRASE, PREFIX, PRECEDENCE, ESCAPE"), "/response/numFound==0");
    assertJQ(req("defType", "simple", "qf", "text0 text1 text-keyword0", "q", "\n",
        "q.operators", "WHITESPACE"), "/response/numFound==0");
    assertJQ(req("defType", "simple", "qf", "text0 text1 text-keyword0", "q", "\\",
        "q.operators", "WHITESPACE"), "/response/numFound==1");
  }

  @Test
  public void testArbitraryOperatorsEnabledDisabled() throws Exception {
    assertJQ(req("defType", "simple", "qf", "text0 text1 text-keyword0", "q", "kw0+kw1+kw2| \\ ",
        "q.operators", "AND, NOT, OR, PHRASE"), "/response/numFound==1");
    assertJQ(req("defType", "simple", "qf", "text0 text1 text-keyword0", "q", "t1 + t2 \\",
        "q.operators", "AND, WHITESPACE"), "/response/numFound==3");
    assertJQ(req("defType", "simple", "qf", "text0 text-keyword0", "q", "t0 + (-t1 -t2) |",
        "q.operators", "AND, NOT, PRECEDENCE, WHITESPACE"), "/response/numFound==4");
  }

  @Test
  public void testNoOperators() throws Exception {
    assertJQ(req("defType", "simple", "qf", "text1 text-keyword0", "q", "kw0 kw1 kw2",
        "q.operators", ""), "/response/numFound==1");
    assertJQ(req("defType", "simple", "qf", "text1", "q", "t1 t2 t3",
        "q.operators", ""), "/response/numFound==2");
  }

  @Test
  public void testDefaultOperator() throws Exception {
    assertJQ(req("defType", "simple", "qf", "text1 text-keyword0", "q", "t2 t3",
        "q.op", "AND"), "/response/numFound==0");
    assertJQ(req("defType", "simple", "qf", "text0 text-keyword0", "q", "t0 t2",
        "q.op", "AND"), "/response/numFound==1");
    assertJQ(req("defType", "simple", "qf", "text1", "q", "t2 t3"), "/response/numFound==2");
  }

  /** Test that multiterm analysis chain is used for prefix. */
  public void testPrefixChain() throws Exception {
    assertJQ(req("defType", "simple", "qf", "text0", "q", "FOOBAR*"), "/response/numFound==1");
    assertJQ(req("defType", "simple", "qf", "text0", "q", "Fóóbar*"), "/response/numFound==1");
    assertJQ(req("defType", "simple", "qf", "text0", "q", "FOO*"), "/response/numFound==1");
    assertJQ(req("defType", "simple", "qf", "text0", "q", "BAR*"), "/response/numFound==0");
  }

  /** Test that multiterm analysis chain is used for fuzzy. */
  public void testFuzzyChain() throws Exception {
    assertJQ(req("defType", "simple", "qf", "text0", "q", "FOOBAT~1"), "/response/numFound==1");
    assertJQ(req("defType", "simple", "qf", "text0", "q", "Fóóba~1"), "/response/numFound==1");
    assertJQ(req("defType", "simple", "qf", "text0", "q", "FOOB~2"), "/response/numFound==1");
    assertJQ(req("defType", "simple", "qf", "text0", "q", "BAR~1"), "/response/numFound==0");
  }

  public void testQueryAnalyzerIsUsed() throws Exception {
    // this should only match one doc, which was lower cased before being added
    assertJQ(req("defType", "simple", "qf", "text-query0", "q", "HELLO"),
             "/response/numFound==1");
  }
}

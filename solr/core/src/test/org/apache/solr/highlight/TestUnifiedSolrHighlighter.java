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
package org.apache.solr.highlight;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.schema.IndexSchema;
import org.junit.AfterClass;
import org.junit.BeforeClass;

/** Tests for the UnifiedHighlighter Solr plugin **/
public class TestUnifiedSolrHighlighter extends SolrTestCaseJ4 {
  
  @BeforeClass
  public static void beforeClass() throws Exception {
    System.setProperty("filterCache.enabled", "false");
    System.setProperty("queryResultCache.enabled", "false");
    System.setProperty("documentCache.enabled", "true"); // this is why we use this particular solrconfig
    initCore("solrconfig-cache-enable-disable.xml", "schema-unifiedhighlight.xml");
    
    // test our config is sane, just to be sure:

    // 'text' and 'text3' should have offsets, 'text2' should not
    IndexSchema schema = h.getCore().getLatestSchema();
    assertTrue(schema.getField("text").storeOffsetsWithPositions());
    assertTrue(schema.getField("text3").storeOffsetsWithPositions());
    assertFalse(schema.getField("text2").storeOffsetsWithPositions());
  }
  @AfterClass
  public static void afterClass() {
    System.clearProperty("filterCache.enabled");
    System.clearProperty("queryResultCache.enabled");
    System.clearProperty("documentCache.enabled");
    System.clearProperty("solr.tests.id.stored");
    System.clearProperty("solr.tests.id.docValues");
  }
  
  @Override
  public void setUp() throws Exception {
    super.setUp();
    clearIndex();
    assertU(adoc("text", "document one", "text2", "document one", "text3", "crappy document", "id", "101"));
    assertU(adoc("text", "second document", "text2", "second document", "text3", "crappier document", "id", "102"));
    assertU(commit());
  }

  public static SolrQueryRequest req(String... params) {
    return SolrTestCaseJ4.req(params, "hl.method", "unified");
  }

  public void testSimple() {
    assertQ("simplest test", 
        req("q", "text:document", "sort", "id asc", "hl", "true"),
        "count(//lst[@name='highlighting']/*)=2",
        "//lst[@name='highlighting']/lst[@name='101']/arr[@name='text']/str='<em>document</em> one'",
        "//lst[@name='highlighting']/lst[@name='102']/arr[@name='text']/str='second <em>document</em>'");
  }

  public void testImpossibleOffsetSource() {
    IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> {
      h.query(req("q", "text2:document", "hl.offsetSource", "postings",
          "hl.fl", "text2", "sort", "id asc", "hl", "true"));
    });
    assertTrue("Should warn no offsets", e.getMessage().contains("indexed without offsets"));

  }

  public void testMultipleSnippetsReturned() {
    clearIndex();
    assertU(adoc("text", "Document snippet one. Intermediate sentence. Document snippet two.",
        "text2", "document one", "text3", "crappy document", "id", "101"));
    assertU(commit());
    assertQ("multiple snippets test",
        req("q", "text:document", "sort", "id asc", "hl", "true", "hl.snippets", "2", "hl.bs.type", "SENTENCE",
            "hl.fragsize", "-1"),
        "count(//lst[@name='highlighting']/lst[@name='101']/arr[@name='text']/*)=2",
        "//lst[@name='highlighting']/lst[@name='101']/arr/str[1]='<em>Document</em> snippet one. '",
        "//lst[@name='highlighting']/lst[@name='101']/arr/str[2]='<em>Document</em> snippet two.'");
  }

  public void testStrictPhrasesEnabledByDefault() {
    clearIndex();
    assertU(adoc("text", "Strict phrases should be enabled for phrases",
        "text2", "document one", "text3", "crappy document", "id", "101"));
    assertU(commit());
    assertQ("strict phrase handling",
        req("q", "text:\"strict phrases\"", "sort", "id asc", "hl", "true"),
        "count(//lst[@name='highlighting']/lst[@name='101']/arr[@name='text']/*)=1",
        "//lst[@name='highlighting']/lst[@name='101']/arr/str[1]='<em>Strict phrases</em> should be enabled for phrases'");
  }

  public void testStrictPhrasesCanBeDisabled() {
    clearIndex();
    assertU(adoc("text", "Strict phrases should be disabled for phrases",
        "text2", "document one", "text3", "crappy document", "id", "101"));
    assertU(commit());
    assertQ("strict phrase handling",
        req("q", "text:\"strict phrases\"", "sort", "id asc", "hl", "true", "hl.usePhraseHighlighter", "false"),
        "count(//lst[@name='highlighting']/lst[@name='101']/arr[@name='text']/*)=1",
        "//lst[@name='highlighting']/lst[@name='101']/arr/str[1]='<em>Strict</em> <em>phrases</em> should be disabled for <em>phrases</em>'");
  }

  public void testMultiTermQueryEnabledByDefault() {
    clearIndex();
    assertU(adoc("text", "Aviary Avenue document",
        "text2", "document one", "text3", "crappy document", "id", "101"));
    assertU(commit());
    assertQ("multi term query handling",
        req("q", "text:av*", "sort", "id asc", "hl", "true"),
        "count(//lst[@name='highlighting']/lst[@name='101']/arr[@name='text']/*)=1",
        "//lst[@name='highlighting']/lst[@name='101']/arr/str[1]='<em>Aviary</em> <em>Avenue</em> document'");
  }

  public void testMultiTermQueryCanBeDisabled() {
    clearIndex();
    assertU(adoc("text", "Aviary Avenue document",
        "text2", "document one", "text3", "crappy document", "id", "101"));
    assertU(commit());
    assertQ("multi term query handling",
        req("q", "text:av*", "sort", "id asc", "hl", "true", "hl.highlightMultiTerm", "false"),
        "count(//lst[@name='highlighting']/lst[@name='101']/arr[@name='text']/*)=0");
  }

  public void testPagination() {
    assertQ("pagination test", 
        req("q", "text:document", "sort", "id asc", "hl", "true", "rows", "1", "start", "1"),
        "count(//lst[@name='highlighting']/*)=1",
        "//lst[@name='highlighting']/lst[@name='102']/arr[@name='text']/str='second <em>document</em>'");
  }
  
  public void testEmptySnippet() {
    assertQ("null snippet test", 
      req("q", "text:one OR *:*", "sort", "id asc", "hl", "true"),
        "count(//lst[@name='highlighting']/*)=2",
        "//lst[@name='highlighting']/lst[@name='101']/arr[@name='text']/str='document <em>one</em>'",
        "count(//lst[@name='highlighting']/lst[@name='102']/arr[@name='text']/*)=0");
  }
  
  public void testDefaultSummary() {
    assertQ("null snippet test", 
      req("q", "text:one OR *:*", "sort", "id asc", "hl", "true", "hl.defaultSummary", "true"),
        "count(//lst[@name='highlighting']/*)=2",
        "//lst[@name='highlighting']/lst[@name='101']/arr[@name='text']/str='document <em>one</em>'",
        "//lst[@name='highlighting']/lst[@name='102']/arr[@name='text']/str='second document'");
  }
  
  public void testDifferentField() {
    assertQ("highlighting text3", 
        req("q", "text3:document", "sort", "id asc", "hl", "true", "hl.fl", "text3"),
        "count(//lst[@name='highlighting']/*)=2",
        "//lst[@name='highlighting']/lst[@name='101']/arr[@name='text3']/str='crappy <em>document</em>'",
        "//lst[@name='highlighting']/lst[@name='102']/arr[@name='text3']/str='crappier <em>document</em>'");
  }
  
  public void testTwoFields() {
    assertQ("highlighting text and text3", 
        req("q", "text:document text3:document", "sort", "id asc", "hl", "true", "hl.fl", "text,text3"),
        "count(//lst[@name='highlighting']/*)=2",
        "//lst[@name='highlighting']/lst[@name='101']/arr[@name='text']/str='<em>document</em> one'",
        "//lst[@name='highlighting']/lst[@name='101']/arr[@name='text3']/str='crappy <em>document</em>'",
        "//lst[@name='highlighting']/lst[@name='102']/arr[@name='text']/str='second <em>document</em>'",
        "//lst[@name='highlighting']/lst[@name='102']/arr[@name='text3']/str='crappier <em>document</em>'");
  }

  // SOLR-5127
  public void testMultipleFieldsViaWildcard() {
    assertQ("highlighting text and text3*",
        req("q", (random().nextBoolean() ? "text:document text3:document" : "text3:document text:document"),
            "sort", "id asc", "hl", "true",
            "hl.fl", (random().nextBoolean() ? "text,text3*" : "text3*,text")),
        "count(//lst[@name='highlighting']/*)=2",
        "//lst[@name='highlighting']/lst[@name='101']/arr[@name='text']/str='<em>document</em> one'",
        "//lst[@name='highlighting']/lst[@name='101']/arr[@name='text3']/str='crappy <em>document</em>'",
        "//lst[@name='highlighting']/lst[@name='102']/arr[@name='text']/str='second <em>document</em>'",
        "//lst[@name='highlighting']/lst[@name='102']/arr[@name='text3']/str='crappier <em>document</em>'");
  }

  public void testTags() {
    assertQ("different pre/post tags", 
        req("q", "text:document", "sort", "id asc", "hl", "true", "hl.tag.pre", "[", "hl.tag.post", "]"),
        "count(//lst[@name='highlighting']/*)=2",
        "//lst[@name='highlighting']/lst[@name='101']/arr[@name='text']/str='[document] one'",
        "//lst[@name='highlighting']/lst[@name='102']/arr[@name='text']/str='second [document]'");
  }

  public void testUsingSimplePrePostTags() {
    assertQ("different pre/post tags",
        req("q", "text:document", "sort", "id asc", "hl", "true", "hl.simple.pre", "[", "hl.simple.post", "]"),
        "count(//lst[@name='highlighting']/*)=2",
        "//lst[@name='highlighting']/lst[@name='101']/arr[@name='text']/str='[document] one'",
        "//lst[@name='highlighting']/lst[@name='102']/arr[@name='text']/str='second [document]'");
  }

  public void testUsingSimplePrePostTagsPerField() {
    assertQ("different pre/post tags",
        req("q", "text:document", "sort", "id asc", "hl", "true", "f.text.hl.simple.pre", "[", "f.text.hl.simple.post", "]"),
        "count(//lst[@name='highlighting']/*)=2",
        "//lst[@name='highlighting']/lst[@name='101']/arr[@name='text']/str='[document] one'",
        "//lst[@name='highlighting']/lst[@name='102']/arr[@name='text']/str='second [document]'");
  }

  public void testTagsPerField() {
    assertQ("highlighting text and text3", 
        req("q", "text:document text3:document", "sort", "id asc", "hl", "true", "hl.fl", "text,text3", "f.text3.hl.tag.pre", "[", "f.text3.hl.tag.post", "]"),
        "count(//lst[@name='highlighting']/*)=2",
        "//lst[@name='highlighting']/lst[@name='101']/arr[@name='text']/str='<em>document</em> one'",
        "//lst[@name='highlighting']/lst[@name='101']/arr[@name='text3']/str='crappy [document]'",
        "//lst[@name='highlighting']/lst[@name='102']/arr[@name='text']/str='second <em>document</em>'",
        "//lst[@name='highlighting']/lst[@name='102']/arr[@name='text3']/str='crappier [document]'");
  }
  
  public void testBreakIteratorWord() {
    assertQ("different breakiterator", 
        req("q", "text:document", "sort", "id asc", "hl", "true", "hl.bs.type", "WORD", "hl.fragsize", "-1"),
        "count(//lst[@name='highlighting']/*)=2",
        "//lst[@name='highlighting']/lst[@name='101']/arr[@name='text']/str='<em>document</em>'",
        "//lst[@name='highlighting']/lst[@name='102']/arr[@name='text']/str='<em>document</em>'");
  }
  
  public void testBreakIteratorWhole() {
    assertU(adoc("text", "Document one has a first sentence. Document two has a second sentence.", "id", "103"));
    assertU(commit());
    assertQ("WHOLE breakiterator",
        req("q", "text:document", "sort", "id asc", "hl", "true", "hl.bs.type", "WHOLE", "hl.fragsize", "-1"),
        "//lst[@name='highlighting']/lst[@name='103']/arr[@name='text']/str='<em>Document</em> one has a first sentence. <em>Document</em> two has a second sentence.'");
    assertQ("hl.fragsize 0 is equivalent to WHOLE",
        req("q", "text:document", "sort", "id asc", "hl", "true", "hl.fragsize", "0"),
        "//lst[@name='highlighting']/lst[@name='103']/arr[@name='text']/str='<em>Document</em> one has a first sentence. <em>Document</em> two has a second sentence.'");
  }
  
  public void testBreakIteratorCustom() {
    assertU(adoc("text", "This document contains # special characters, while the other document contains the same # special character.", "id", "103"));
    assertU(adoc("text", "While the other document contains the same # special character.", "id", "104"));
    assertU(commit());
    assertQ("CUSTOM breakiterator",
        req("q", "text:document", "sort", "id asc", "hl", "true", "hl.bs.type", "SEPARATOR","hl.bs.separator","#","hl.fragsize", "-1"),
        "//lst[@name='highlighting']/lst[@name='103']/arr[@name='text']/str='This <em>document</em> contains #'");
    assertQ("different breakiterator",
        req("q", "text:document", "sort", "id asc", "hl", "true", "hl.bs.type", "SEPARATOR","hl.bs.separator","#","hl.fragsize", "-1"),
        "//lst[@name='highlighting']/lst[@name='104']/arr[@name='text']/str='While the other <em>document</em> contains the same #'");

    assertQ("CUSTOM breakiterator with fragsize 70 minimum",
        req("q", "text:document", "sort", "id asc", "hl", "true", "hl.bs.type", "SEPARATOR","hl.bs.separator","#","hl.fragsize", "70", "hl.fragsizeIsMinimum", "true"),
        "//lst[@name='highlighting']/lst[@name='103']/arr[@name='text']/str='This <em>document</em> contains # special characters, while the other <em>document</em> contains the same #'");
    assertQ("CUSTOM breakiterator with fragsize 70 avg",
        req("q", "text:document", "sort", "id asc", "hl", "true", "hl.bs.type", "SEPARATOR","hl.bs.separator","#","hl.fragsize", "70", "hl.fragsizeIsMinimum", "false"),
        "//lst[@name='highlighting']/lst[@name='103']/arr[@name='text']/str='This <em>document</em> contains #'");
    assertQ("CUSTOM breakiterator with fragsize 90 avg",
        req("q", "text:document", "sort", "id asc", "hl", "true", "hl.bs.type", "SEPARATOR","hl.bs.separator","#","hl.fragsize", "90", "hl.fragsizeIsMinimum", "false"),
        "//lst[@name='highlighting']/lst[@name='103']/arr[@name='text']/str='This <em>document</em> contains #'");
    assertQ("CUSTOM breakiterator with fragsize 100 avg",
        req("q", "text:document", "sort", "id asc", "hl", "true", "hl.bs.type", "SEPARATOR","hl.bs.separator","#","hl.fragsize", "100", "hl.fragsizeIsMinimum", "false"),
        "//lst[@name='highlighting']/lst[@name='103']/arr[@name='text']/str='This <em>document</em> contains # special characters, while the other <em>document</em> contains the same #'");
  }

  public void testFragsize() {
    // test default is 70... so make a sentence that is a little less (closer to 70 than end of text)
    clearIndex();
    assertU(adoc("id", "10", "text", "This is a sentence just under seventy chars in length blah blah. Next sentence is here."));
    assertU(commit());
    assertQ("default fragsize",
        req("q", "text:seventy", "hl", "true", "hl.fragsizeIsMinimum", "true"),
        "//lst[@name='highlighting']/lst[@name='10']/arr[@name='text']/str='This is a sentence just under <em>seventy</em> chars in length blah blah. Next sentence is here.'");
    assertQ("default fragsize",
        req("q", "text:seventy", "hl", "true", "hl.fragsizeIsMinimum", "true", "hl.fragsize", "60"),
        "//lst[@name='highlighting']/lst[@name='10']/arr[@name='text']/str='This is a sentence just under <em>seventy</em> chars in length blah blah. '");
    assertQ("smaller fragsize",
        req("q", "text:seventy", "hl", "true", "hl.fragsizeIsMinimum", "false"),
        "//lst[@name='highlighting']/lst[@name='10']/arr[@name='text']/str='This is a sentence just under <em>seventy</em> chars in length blah blah. '");
    assertQ("default fragsize",
        req("q", "text:seventy", "hl", "true", "hl.fragsize", "90", "hl.fragsizeIsMinimum", "false"),
        "//lst[@name='highlighting']/lst[@name='10']/arr[@name='text']/str='This is a sentence just under <em>seventy</em> chars in length blah blah. Next sentence is here.'");
  }
  
  public void testEncoder() {
    assertU(adoc("text", "Document one has a first <i>sentence</i>.", "id", "103"));
    assertU(commit());
    assertQ("html escaped", 
        req("q", "text:document", "sort", "id asc", "hl", "true", "hl.encoder", "html"),
        "//lst[@name='highlighting']/lst[@name='103']/arr[@name='text']/str='<em>Document</em> one has a first &lt;i&gt;sentence&lt;&#x2F;i&gt;.'");
  }

  public void testRangeQuery() {
    assertQ(req("q", "id:101", "hl", "true", "hl.q", "text:[dob TO doe]"),
        "count(//lst[@name='highlighting']/lst[@name='101']/arr[@name='text']/*)=1");
  }

  public void testRequireFieldMatch() {
    // We highlight on field text3 (hl.fl), but our query only references the "text" field. Nonetheless, the query word
    //  "document" is found in all fields here.

    assertQ(req("q", "id:101", "hl", "true", "hl.q", "text:document", "hl.fl", "text3"), //hl.requireFieldMatch is false by default
        "count(//lst[@name='highlighting']/lst[@name='101']/arr[@name='text3']/*)=1");
    assertQ(req("q", "id:101", "hl", "true", "hl.q", "text:document", "hl.fl", "text3", "hl.requireFieldMatch", "true"),
        "count(//lst[@name='highlighting']/lst[@name='101']/arr[@name='text3']/*)=0");
  }

  public void testWeightMatchesDisabled() {
    clearIndex();
    assertU(adoc("text", "alpha bravo charlie", "id", "101"));
    assertU(commit());
    assertQ("weight matches disabled, phrase highlights separately",
        req("q", "text:\"alpha bravo\"", "hl", "true", "hl.weightMatches", "false"),
        "count(//lst[@name='highlighting']/lst[@name='101']/arr[@name='text']/*)=1",
        "//lst[@name='highlighting']/lst[@name='101']/arr/str[1]='<em>alpha</em> <em>bravo</em> charlie'");
  }

  // LUCENE-8492
  public void testSurroundQParser() {
    assertQ(req("q", "{!surround df=text}2w(second, document)", "hl", "true", "hl.fl", "text"),
        "count(//lst[@name='highlighting']/lst[@name='102']/arr[@name='text']/*)=1");
  }

  // LUCENE-7757
  public void testComplexPhraseQParser() {
    assertQ(req("q", "{!complexphrase df=text}(\"sec* doc*\")", "hl", "true", "hl.fl", "text"),
        "count(//lst[@name='highlighting']/lst[@name='102']/arr[@name='text']/*)=1");
  }

}

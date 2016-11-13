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
import org.apache.solr.handler.component.HighlightComponent;
import org.apache.solr.schema.IndexSchema;
import org.junit.BeforeClass;
import org.junit.Ignore;

/** simple tests for PostingsSolrHighlighter */
public class TestUnifiedSolrHighlighter extends SolrTestCaseJ4 {
  
  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig-unifiedhighlight.xml", "schema-unifiedhighlight.xml");
    
    // test our config is sane, just to be sure:
    
    // postingshighlighter should be used
    SolrHighlighter highlighter = HighlightComponent.getHighlighter(h.getCore());
    assertTrue("wrong highlighter: " + highlighter.getClass(), highlighter instanceof UnifiedSolrHighlighter);
    
    // 'text' and 'text3' should have offsets, 'text2' should not
    IndexSchema schema = h.getCore().getLatestSchema();
    assertTrue(schema.getField("text").storeOffsetsWithPositions());
    assertTrue(schema.getField("text3").storeOffsetsWithPositions());
    assertFalse(schema.getField("text2").storeOffsetsWithPositions());
  }
  
  @Override
  public void setUp() throws Exception {
    super.setUp();
    clearIndex();
    assertU(adoc("text", "document one", "text2", "document one", "text3", "crappy document", "id", "101"));
    assertU(adoc("text", "second document", "text2", "second document", "text3", "crappier document", "id", "102"));
    assertU(commit());
  }
  
  public void testSimple() {
    assertQ("simplest test", 
        req("q", "text:document", "sort", "id asc", "hl", "true"),
        "count(//lst[@name='highlighting']/*)=2",
        "//lst[@name='highlighting']/lst[@name='101']/arr[@name='text']/str='<em>document</em> one'",
        "//lst[@name='highlighting']/lst[@name='102']/arr[@name='text']/str='second <em>document</em>'");
  }

  public void testMultipleSnippetsReturned() {
    clearIndex();
    assertU(adoc("text", "Document snippet one. Intermediate sentence. Document snippet two.",
        "text2", "document one", "text3", "crappy document", "id", "101"));
    assertU(commit());
    assertQ("multiple snippets test",
        req("q", "text:document", "sort", "id asc", "hl", "true", "hl.snippets", "2", "hl.bs.type", "SENTENCE"),
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
        "//lst[@name='highlighting']/lst[@name='101']/arr/str[1]='<em>Strict</em> <em>phrases</em> should be enabled for phrases'");
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

  //todo: need to configure field that is not at least stored, hence no analysis
  //otherwise, this highlighter is resilient
  @Ignore
  public void testMisconfiguredField() {
    ignoreException("was indexed without offsets");
    try {
      assertQ("should fail, has no offsets",
        req("q", "text2:document", "sort", "id asc", "hl", "true", "hl.fl", "text2"));
      fail();
    } catch (Exception expected) {
      // expected
    }
    resetExceptionIgnores();
  }
  
  public void testTags() {
    assertQ("different pre/post tags", 
        req("q", "text:document", "sort", "id asc", "hl", "true", "hl.tag.pre", "[", "hl.tag.post", "]"),
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
  
  public void testBreakIterator() {
    assertQ("different breakiterator", 
        req("q", "text:document", "sort", "id asc", "hl", "true", "hl.bs.type", "WORD"),
        "count(//lst[@name='highlighting']/*)=2",
        "//lst[@name='highlighting']/lst[@name='101']/arr[@name='text']/str='<em>document</em>'",
        "//lst[@name='highlighting']/lst[@name='102']/arr[@name='text']/str='<em>document</em>'");
  }
  
  public void testBreakIterator2() {
    assertU(adoc("text", "Document one has a first sentence. Document two has a second sentence.", "id", "103"));
    assertU(commit());
    assertQ("different breakiterator", 
        req("q", "text:document", "sort", "id asc", "hl", "true", "hl.bs.type", "WHOLE"),
        "//lst[@name='highlighting']/lst[@name='103']/arr[@name='text']/str='<em>Document</em> one has a first sentence. <em>Document</em> two has a second sentence.'");
  }
  
  public void testEncoder() {
    assertU(adoc("text", "Document one has a first <i>sentence</i>.", "id", "103"));
    assertU(commit());
    assertQ("html escaped", 
        req("q", "text:document", "sort", "id asc", "hl", "true", "hl.encoder", "html"),
        "//lst[@name='highlighting']/lst[@name='103']/arr[@name='text']/str='<em>Document</em>&#32;one&#32;has&#32;a&#32;first&#32;&lt;i&gt;sentence&lt;&#x2F;i&gt;&#46;'");
  }
  
  public void testWildcard() {
    assertQ("simplest test", 
        req("q", "text:doc*ment", "sort", "id asc", "hl", "true", "hl.highlightMultiTerm", "true"),
        "count(//lst[@name='highlighting']/*)=2",
        "//lst[@name='highlighting']/lst[@name='101']/arr[@name='text']/str='<em>document</em> one'",
        "//lst[@name='highlighting']/lst[@name='102']/arr[@name='text']/str='second <em>document</em>'");
  }
}

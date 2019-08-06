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

/** simple tests for PostingsSolrHighlighter */
public class TestPostingsSolrHighlighter extends SolrTestCaseJ4 {
  
  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig-postingshighlight.xml", "schema-postingshighlight.xml");
    
    // test our config is sane, just to be sure:
    
    // postingshighlighter should be used
    SolrHighlighter highlighter = HighlightComponent.getHighlighter(h.getCore());
    assertTrue("wrong highlighter: " + highlighter.getClass(), highlighter instanceof PostingsSolrHighlighter);
    
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
        req("q", "text:document", "sort", "id asc", "hl", "true", "hl.method", "postings"), // test hl.method is happy too
        "count(//lst[@name='highlighting']/*)=2",
        "//lst[@name='highlighting']/lst[@name='101']/arr[@name='text']/str='<em>document</em> one'",
        "//lst[@name='highlighting']/lst[@name='102']/arr[@name='text']/str='second <em>document</em>'");
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

  public void testMisconfiguredField() {
    ignoreException("was indexed without offsets");
    expectThrows(Exception.class, () ->
        h.query(req("q", "text2:document", "sort", "id asc", "hl", "true", "hl.fl", "text2")));
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
  
  public void testBreakIterator3() {
    assertU(adoc("text", "This document contains # special characters, while the other document contains the same # special character.", "id", "103"));
    assertU(adoc("text", "While the other document contains the same # special character.", "id", "104"));
    assertU(commit());
    assertQ("different breakiterator", 
        req("q", "text:document", "sort", "id asc", "hl", "true", "hl.bs.type", "SEPARATOR","hl.bs.separator","#"),
        "//lst[@name='highlighting']/lst[@name='103']/arr[@name='text']/str='This <em>document</em> contains #'");
    assertQ("different breakiterator", 
        req("q", "text:document", "sort", "id asc", "hl", "true", "hl.bs.type", "SEPARATOR","hl.bs.separator","#"),
        "//lst[@name='highlighting']/lst[@name='104']/arr[@name='text']/str='While the other <em>document</em> contains the same #'");

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

/*
 * This software was produced for the U. S. Government
 * under Contract No. W15P7T-11-C-F600, and is
 * subject to the Rights in Noncommercial Computer Software
 * and Noncommercial Computer Software Documentation
 * Clause 252.227-7014 (JUN 1995)
 *
 * Copyright 2013 The MITRE Corporation. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.solr.handler.tagger;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;

import org.apache.commons.io.IOUtils;
import org.apache.lucene.analysis.charfilter.HTMLStripCharFilter;
import org.apache.lucene.analysis.core.WhitespaceTokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.solr.common.SolrException;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.xml.sax.InputSource;

public class XmlInterpolationTest extends TaggerTestCase {

  private static DocumentBuilder xmlDocBuilder;


  @BeforeClass
  public static void beforeClass() throws Exception {
    DocumentBuilderFactory xmlDocBuilderFactory = DocumentBuilderFactory.newInstance();
    xmlDocBuilderFactory.setValidating(true);
    xmlDocBuilderFactory.setNamespaceAware(true);
    xmlDocBuilder = xmlDocBuilderFactory.newDocumentBuilder();

    initCore("solrconfig-tagger.xml", "schema-tagger.xml");
  }

  @AfterClass
  public static void cleanUpAfterClass() throws Exception {
    xmlDocBuilder = null;
  }

  @Override
  public void setUp() throws Exception {
    super.setUp();
    baseParams.set("field", "name_tagXml");
    baseParams.set("overlaps", "LONGEST_DOMINANT_RIGHT");
    baseParams.set("xmlOffsetAdjust", "true");
  }

  @Test
  public void test() throws Exception {
    buildNames("start end");

    assertXmlTag("<doc>before start <!-- c --> end after</doc>", true);
    assertXmlTag("<doc>before start <br/> end after</doc>", true);
    assertXmlTag("<doc>before <em>start</em> <b>end</b> after</doc>", true);
    assertXmlTag("<doc>before <em>start</em> end after</doc>", true);
    assertXmlTag("<doc>before start end<em> after</em></doc>", true);
    assertXmlTag("<doc><em>before </em>start end after</doc>", true);//adjacent tags
    assertXmlTag("<doc>before <b> <em>start</em> </b> end after</doc>", true);
    assertXmlTag("<doc>before <b> <em>start</em> </b> <em>  end  </em> after</doc>", true);

    assertXmlTag("<doc><p>before start</p> end after</doc>", false);
    assertXmlTag("<doc>before start <p>end after</p> </doc>", false);

    assertXmlTag("<doc>before <em a='A' b='B'>start</em> <b a='A' b='B'>end</b> after</doc>", true);
  }

  @Test(expected = SolrException.class)
  public void testInvalidXml() throws Exception {
    assertXmlTag("notXml", false);
  }

  @Test(expected = Exception.class)
  public void testValidatingXml() throws Exception {
    validateXml("foo");
  }

  protected void assertXmlTag(String docText, boolean expected) throws Exception {
    final SolrQueryRequest req = reqDoc(docText);
    try { // 5.4 and beyond we can use try-with-resources
      final SolrQueryResponse rsp = h.queryAndResponse(req.getParams().get("qt"), req);
      final TestTag[] testTags = pullTagsFromResponse(req, rsp);
      if (!expected) {
        assertEquals(0, testTags.length);
      } else {
        assertEquals(1, testTags.length);
        final TestTag tag = testTags[0];
        validateXml(insertAnchorAtOffsets(docText, tag.startOffset, tag.endOffset, tag.docName));
      }
    } finally {
      req.close();
    }
  }

  protected void validateXml(String xml) throws Exception {
    // the "parse" method also validates XML, will throw an exception if mis-formatted
    xmlDocBuilder.parse(new InputSource(new StringReader(xml)));
  }


  @Test
  public void testLuceneHtmlFilterBehavior() {
    String docText;

    //Close tag adjacent to start & end results in end offset including the close tag. LUCENE-5734
    docText = "<doc><a><b>start</b> end</a></doc>";
    assertArrayEquals(tagExpect(docText, "start", "end</a>"), analyzeTagOne(docText, "start", "end"));

    //Space after "end" means offset doesn't include </a>
    docText = "<doc><a><b>start</b> end </a></doc>";
    assertArrayEquals(tagExpect(docText, "start", "end"), analyzeTagOne(docText, "start", "end"));

    //Matches entity at end
    final String endStr = String.format(Locale.ROOT, "en&#x%02x;", (int) 'd');
    docText = "<doc>start " + endStr + "</doc>";
    assertArrayEquals(tagExpect(docText, "start", endStr), analyzeTagOne(docText, "start", "end"));
    //... and at start
    final String startStr = String.format(Locale.ROOT, "&#x%02x;tart", (int) 's');
    docText = "<doc>" + startStr + " end</doc>";
    assertArrayEquals(tagExpect(docText, startStr, "end"), analyzeTagOne(docText, "start", "end"));

    //Test ignoring proc instructions & comments. Note: doesn't expand the entity to "start".
    docText = "<!DOCTYPE start [ "
            + "<!ENTITY start \"start\">"
            + "]><start><?start start ?><!-- start --><start/>&start;</start>";
    assertArrayEquals(new int[]{-1, -1}, analyzeTagOne(docText, "start", "start"));

    //Test entity behavior
    docText =                " &mdash; &ndash; &amp; &foo; &#xA0; a&nbsp;b";
    assertArrayEquals(new String[]{"—", "–", "&", "&foo;", "\u00A0", "a", "b"},
            analyzeReturnTokens(docText));

    //Observe offset adjustment of trailing entity to end tag
    docText = "foo&nbsp;bar";
    assertArrayEquals(tagExpect(docText, "foo", "foo"), analyzeTagOne(docText, "foo", "foo"));
  }

  private String insertAnchorAtOffsets(String docText, int startOffset, int endOffset, String id) {
    String insertStart = "<A id='"+ id +"'>";// (normally we'd escape id)
    String insertEnd = "</A>";
    return docText.substring(0, startOffset)
            + insertStart
            + docText.substring(startOffset, endOffset)
            + insertEnd
            + docText.substring(endOffset);
  }

  private int[] tagExpect(String docText, String start, String end) {
    return new int[]{docText.indexOf(start), docText.indexOf(end) + end.length()};
  }

  private int[] analyzeTagOne(String docText, String start, String end) {
    int[] result = {-1, -1};

    Reader filter = new HTMLStripCharFilter(new StringReader(docText));

    WhitespaceTokenizer ts = new WhitespaceTokenizer();
    final CharTermAttribute termAttribute = ts.addAttribute(CharTermAttribute.class);
    final OffsetAttribute offsetAttribute = ts.addAttribute(OffsetAttribute.class);
    try {
      ts.setReader(filter);
      ts.reset();
      while (ts.incrementToken()) {
        final String termString = termAttribute.toString();
        if (termString.equals(start))
          result[0] = offsetAttribute.startOffset();
        if (termString.equals(end)) {
          result[1] = offsetAttribute.endOffset();
          return result;
        }
      }
      ts.end();
    } catch (IOException e) {
      throw new RuntimeException(e);
    } finally {
      IOUtils.closeQuietly(ts);
    }
    return result;
  }

  private String[] analyzeReturnTokens(String docText) {
    List<String> result = new ArrayList<>();

    Reader filter = new HTMLStripCharFilter(new StringReader(docText),
            Collections.singleton("unescaped"));
    WhitespaceTokenizer ts = new WhitespaceTokenizer();
    final CharTermAttribute termAttribute = ts.addAttribute(CharTermAttribute.class);
    try {
      ts.setReader(filter);
      ts.reset();
      while (ts.incrementToken()) {
        result.add(termAttribute.toString());
      }
      ts.end();
    } catch (IOException e) {
      throw new RuntimeException(e);
    } finally {
      IOUtils.closeQuietly(ts);
    }
    return result.toArray(new String[result.size()]);
  }

}

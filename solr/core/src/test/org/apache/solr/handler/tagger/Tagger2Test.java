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

import java.nio.charset.StandardCharsets;

import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Test the {@link TaggerRequestHandler}.
 */
public class Tagger2Test extends TaggerTestCase {

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig-tagger.xml", "schema-tagger.xml");
  }

  @Override
  public void setUp() throws Exception {
    super.setUp();
    baseParams.set("overlaps", "LONGEST_DOMINANT_RIGHT");
  }

  /** whole matching, no sub-tags */
  @Test
  public void testLongestDominantRight() throws Exception {
    buildNames("in", "San", "in San", "Francisco", "San Francisco",
        "San Francisco State College", "College of California",
        "Clayton", "Clayton North", "North Carolina");

    assertTags("He lived in San Francisco.",
        "in", "San Francisco");

    assertTags("He enrolled in San Francisco State College of California",
        "in", "San Francisco State College");

    assertTags("He lived in Clayton North Carolina",
        "in", "Clayton", "North Carolina");

  }

  // As of Lucene/Solr 4.9, StandardTokenizer never does this anymore (reported to Lucene dev-list,
  // Jan 26th 2015.  Honestly it's not particularly important to us but it renders this test
  // pointless.
  /** Orig issue https://github.com/OpenSextant/SolrTextTagger/issues/2  related: #13 */
  @Test
  @Ignore
  public void testVeryLongWord() throws Exception {
    String SANFRAN = "San Francisco";
    buildNames(SANFRAN);

    // exceeds default 255 max token length which means it in-effect becomes a stop-word
    StringBuilder STOP = new StringBuilder(260);//>255
    for (int i = 0; i < STOP.capacity(); i++) {
      STOP.append((char) ('0' + (i % 10)));
    }

    String doc = "San " + STOP + " Francisco";
    assertTags(doc);//no match due to default stop word handling
    //and we find it when we ignore stop words
    assertTags(reqDoc(doc, "ignoreStopwords", "true"), new TestTag(0, doc.length(), doc, lookupByName(SANFRAN)));
  }

  /** Support for stopwords (posInc &gt; 1);
   * discussion: https://github.com/OpenSextant/SolrTextTagger/issues/13 */
  @AwaitsFix(bugUrl = "https://issues.apache.org/jira/browse/LUCENE-8344")
  @Test
  public void testStopWords() throws Exception {
    baseParams.set("field", "name_tagStop");//stop filter (pos inc enabled) index & query

    String SOUTHOFWALES = "South of Wales";//'of' is stop word index time & query
    String ACITYA = "A City A";

    buildNames(SOUTHOFWALES, ACITYA);

    //round-trip works
    assertTags(reqDoc(SOUTHOFWALES), new TestTag(0, SOUTHOFWALES.length(), SOUTHOFWALES,
            lookupByName(SOUTHOFWALES)));
    //  but offsets doesn't include stopword when leading or trailing...
    assertTags(reqDoc(ACITYA), new TestTag(2, 6, "City",
            lookupByName(ACITYA)));
    //break on stop words
    assertTags(reqDoc(SOUTHOFWALES, "ignoreStopwords", "false"));//match nothing
  }

  /** Tests WordDelimiterGraphFilter, stacked/synonymous tokens at index time (catenate options) */
  @Test
  public void testWDF() throws Exception {
    baseParams.set("field", "name_tagWDF");

    final String WINSTONSALEM = "City of Winston-Salem";//hyphen
    final String BOSTONHARBOR = "Boston Harbor";//space
    buildNames(WINSTONSALEM, BOSTONHARBOR);

    //round-trip works
    assertTags(reqDoc(WINSTONSALEM), new TestTag(0, WINSTONSALEM.length(), WINSTONSALEM,
        lookupByName(WINSTONSALEM)));

    // space separated works
    final String WS_SPACE = WINSTONSALEM.replace('-', ' ');
    assertTags(reqDoc(WS_SPACE),
        new TestTag(0, WS_SPACE.length(), WS_SPACE,
        lookupByName(WINSTONSALEM)));

    //must be full match
    assertTags(reqDoc("Winston"));//match nothing
    assertTags(reqDoc("Salem"));//match nothing

    // round-trip works
    assertTags(reqDoc(BOSTONHARBOR), new TestTag(0, BOSTONHARBOR.length(), BOSTONHARBOR,
        lookupByName(BOSTONHARBOR)));

    // hyphen separated works
    final String BH_HYPHEN = BOSTONHARBOR.replace(' ', '-');
    assertTags(reqDoc(BH_HYPHEN),
        new TestTag(0, BH_HYPHEN.length(), BH_HYPHEN,
            lookupByName(BOSTONHARBOR)));
    //must be full match
    assertTags(reqDoc("Boston"));//match nothing
    assertTags(reqDoc("Harbor"));//match nothing
  }

  /** Ensure character offsets work for multi-byte characters */
  @Test
  public void testMultibyteChar() throws Exception {
    //  https://unicode-table.com/en/2019/
    //             0         1         2         3         4
    //             01234567890123456789012345678901234567890
    String TEXT = "He mentionned ’Obama’ in the White House";
    assertEquals(40, TEXT.length()); // char length (in Java, UTF16)

    String QUOTE = TEXT.substring(14, 15);
    assertEquals(8217, QUOTE.codePointAt(0));

    //UTF8
    assertEquals(3, QUOTE.getBytes(StandardCharsets.UTF_8).length);
    assertEquals(1, "a".getBytes(StandardCharsets.UTF_8).length);
    assertEquals(40 + 2*2, TEXT.getBytes(StandardCharsets.UTF_8).length);

    //UTF16 big endian    (by specifying big/little endian, there is no "byte order mark")
    assertEquals(2, QUOTE.getBytes(StandardCharsets.UTF_16BE).length);
    assertEquals(2, "a".getBytes(StandardCharsets.UTF_16BE).length);
    assertEquals(40 * 2, TEXT.getBytes(StandardCharsets.UTF_16BE).length);


    buildNames("Obama");

    assertTags(TEXT, "Obama");

    // TODO test surrogate pairs (i.e. code points not in the BMP)
  }

}

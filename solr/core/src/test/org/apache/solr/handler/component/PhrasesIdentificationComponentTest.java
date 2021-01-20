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
package org.apache.solr.handler.component;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.handler.component.PhrasesIdentificationComponent.Phrase;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.params.ShardParams;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaField;

import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Before;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.BaseMatcher;

public class PhrasesIdentificationComponentTest extends SolrTestCaseJ4 {

  private static final String HANDLER = "/phrases";

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig-phrases-identification.xml","schema-phrases-identification.xml");
  }
  
  @Before
  public void addSomeDocs() throws Exception {
    assertU(adoc("id", "42",
                 "title","Tale of the Brown Fox: was he lazy?",
                 "body", "No. The quick brown fox was a very brown fox who liked to get into trouble."));
    assertU(adoc("id", "43",
                 "title","A fable in two acts",
                 "body", "The brOwn fOx jumped. The lazy dog did not"));
    assertU(adoc("id", "44",
                 "title","Why the LazY dog was lazy",
                 "body", "News flash: Lazy Dog was not actually lazy, it just seemd so compared to Fox"));
    assertU(adoc("id", "45",
                 "title","Why Are We Lazy?",
                 "body", "Because we are. that's why"));
    assertU((commit()));
  }
  
  @After
  public void deleteAllDocs() throws Exception {
    assertU(delQ("*:*"));
    assertU((commit()));
  }

  public void testWhiteBoxPhraseParsingLongInput() throws Exception {
    final SchemaField field = h.getCore().getLatestSchema().getField("multigrams_body");
    assertNotNull(field);
    final List<Phrase> phrases = Phrase.extractPhrases
      (" did  a Quick    brown FOX perniciously jump over the lAZy dog", field, 3, 7);

    assertEquals(IntStream.rangeClosed((11-7+1), 11).sum(), // 11 words, max query phrase size is 7
                 phrases.size());
    
    // spot check a few explicitly choosen phrases of various lengths...
    
    { // single term, close to edge so not as many super phrases as other terms might have
      final Phrase lazy = phrases.get(phrases.size() - 1 - 2);
      final String debug = lazy.toString();

      assertEquals(debug, "lAZy", lazy.getSubSequence());
      assertEquals(debug, 10, lazy.getPositionStart());
      assertEquals(debug, 11, lazy.getPositionEnd());
      assertEquals(debug, 1, lazy.getPositionLength());
      
      assertEquals(debug, 54, lazy.getOffsetStart());
      assertEquals(debug, 58, lazy.getOffsetEnd());

      assertEquals(debug, 1, lazy.getIndividualIndexedTerms().size());
      assertEquals(debug, 1, lazy.getLargestIndexedSubPhrases().size());
      assertEquals(debug, lazy, lazy.getIndividualIndexedTerms().get(0));
      assertEquals(debug, lazy, lazy.getLargestIndexedSubPhrases().get(0));
      assertEquals(debug, 4, lazy.getIndexedSuperPhrases().size()); // (2 each: len=2, len=3)
    }
    { // length 2, middle of the pack
      final Phrase brown_fox = phrases.get((7 * 3) + 1);
      final String debug = brown_fox.toString();

      assertEquals(debug, "brown FOX", brown_fox.getSubSequence());
      assertEquals(debug, 4, brown_fox.getPositionStart());
      assertEquals(debug, 6, brown_fox.getPositionEnd());
      assertEquals(debug, 2, brown_fox.getPositionLength());
      
      assertEquals(debug, 17, brown_fox.getOffsetStart());
      assertEquals(debug, 26, brown_fox.getOffsetEnd());

      assertEquals(debug, 2, brown_fox.getIndividualIndexedTerms().size());
      assertEquals(debug, 1, brown_fox.getLargestIndexedSubPhrases().size());
      assertEquals(debug, brown_fox, brown_fox.getLargestIndexedSubPhrases().get(0));
      assertEquals(debug, 2, brown_fox.getIndexedSuperPhrases().size()); // (2 @ len=3)
      
    }
    { // length 3 (which is the max indexed size) @ start of the string
      final Phrase daq = phrases.get(2);
      final String debug = daq.toString();

      assertEquals(debug, "did  a Quick", daq.getSubSequence());
      assertEquals(debug, 1, daq.getPositionStart());
      assertEquals(debug, 4, daq.getPositionEnd());
      assertEquals(debug, 3, daq.getPositionLength());
      
      assertEquals(debug, 1, daq.getOffsetStart());
      assertEquals(debug, 13, daq.getOffsetEnd());

      assertEquals(debug, 3, daq.getIndividualIndexedTerms().size());
      assertEquals(debug, 1, daq.getLargestIndexedSubPhrases().size());
      assertEquals(debug, daq, daq.getLargestIndexedSubPhrases().get(0));
      assertEquals(debug, 0, daq.getIndexedSuperPhrases().size());
    }
    { // length 4 phrase (larger then the max indexed size)
      final Phrase qbfp = phrases.get((7 * 2) + 3);
      final String debug = qbfp.toString();

      assertEquals(debug, "Quick    brown FOX perniciously", qbfp.getSubSequence());
      assertEquals(debug, 3, qbfp.getPositionStart());
      assertEquals(debug, 7, qbfp.getPositionEnd());
      assertEquals(debug, 4, qbfp.getPositionLength());
      
      assertEquals(debug, 8, qbfp.getOffsetStart());
      assertEquals(debug, 39, qbfp.getOffsetEnd());

      assertEquals(debug, 4, qbfp.getIndividualIndexedTerms().size());
      assertEquals(debug, 2, qbfp.getLargestIndexedSubPhrases().size());
      assertEquals(debug, 0, qbfp.getIndexedSuperPhrases().size());
    }
    
    // some blanket assumptions about the results...
    assertBasicSanityChecks(phrases, 11, 3, 7);
  }

  public void testWhiteBoxPhraseParsingShortInput() throws Exception {
    // for input this short, either of these fields should be (mostly) equivalent
    final Map<String,Integer> fields = new TreeMap<>();
    fields.put("multigrams_body", 7); 
    fields.put("multigrams_body_short", 3);
    for (Map.Entry<String,Integer> entry : fields.entrySet()) {
      try {
        final int maxQ = entry.getValue();
        final SchemaField field = h.getCore().getLatestSchema().getField(entry.getKey());
        assertNotNull(field);
        
        // empty input shouldn't break anything
        assertEquals(0, Phrase.extractPhrases(random().nextBoolean() ? "" : "  ", field, 3, maxQ).size());
        
        // input shorter them our index/query phrase sizes shouldn't break anything either....
        final List<Phrase> phrases = Phrase.extractPhrases("brown FOX", field, 3, maxQ);
        
        assertEquals(3, phrases.size());
        
        { // length 2
          final Phrase brown_fox = phrases.get(1);
          final String debug = brown_fox.toString();
          
          assertEquals(debug, "brown FOX", brown_fox.getSubSequence());
          assertEquals(debug, 1, brown_fox.getPositionStart());
          assertEquals(debug, 3, brown_fox.getPositionEnd());
          assertEquals(debug, 2, brown_fox.getPositionLength());
          
          assertEquals(debug, 0, brown_fox.getOffsetStart());
          assertEquals(debug, 9, brown_fox.getOffsetEnd());
          
          assertEquals(debug, 2, brown_fox.getIndividualIndexedTerms().size());
          assertEquals(debug, 1, brown_fox.getLargestIndexedSubPhrases().size());
          assertEquals(debug, brown_fox, brown_fox.getLargestIndexedSubPhrases().get(0));
          assertEquals(debug, 0, brown_fox.getIndexedSuperPhrases().size());
        }
        { // length 1
          final Phrase fox = phrases.get(2);
          final String debug = fox.toString();
          
          assertEquals(debug, "FOX", fox.getSubSequence());
          assertEquals(debug, 2, fox.getPositionStart());
          assertEquals(debug, 3, fox.getPositionEnd());
          assertEquals(debug, 1, fox.getPositionLength());
          
          assertEquals(debug, 6, fox.getOffsetStart());
          assertEquals(debug, 9, fox.getOffsetEnd());
          
          assertEquals(debug, 1, fox.getIndividualIndexedTerms().size());
          assertEquals(debug, 1, fox.getLargestIndexedSubPhrases().size());
          assertEquals(debug, fox, fox.getLargestIndexedSubPhrases().get(0));
          assertEquals(debug, 1, fox.getIndexedSuperPhrases().size());
        }
        
        assertBasicSanityChecks(phrases, 2, 3, maxQ);
      } catch (AssertionError e) {
        throw new AssertionError(entry.getKey() + " => " + e.getMessage(), e);
      }
    }
  }

  /** 
   * Asserts some basic rules that should be enforced about all Phrases 
   * &amp; their linkages to oher phrases 
   */
  private void assertBasicSanityChecks(final List<Phrase> phrases,
                                       final int inputPositionLength,
                                       final int maxIndexedPositionLength,
                                       final int maxQueryPositionLength) throws Exception {
    assert 0 < phrases.size() : "Don't use this method if phrases might be empty";
    
    assertEmptyStream("no phrase should be longer then "+maxQueryPositionLength+" positions",
                      phrases.stream().filter(p -> p.getPositionLength() > maxQueryPositionLength));

    assertEmptyStream("no phrase should have a start offset < 0",
                      phrases.stream().filter(p -> p.getOffsetStart() < 0));
    assertEmptyStream("no phrase should have a start position < 1",
                      phrases.stream().filter(p -> p.getPositionStart() < 1));

    assertEmptyStream("If a phrase has a start offset of 0, then it must have position 1",
                      phrases.stream().filter(p -> (p.getOffsetStart() == 0)
                                              && (p.getPositionStart() != 1)));
    
    final Phrase first = phrases.get(0);
    final Phrase last = phrases.get(phrases.size()-1);
    
    assertEmptyStream("no phrase should have a start offset < first phrase",
                      phrases.stream().filter(p -> p.getOffsetStart() < first.getOffsetStart()));
    assertEmptyStream("no phrase should have an end offset > last phrase",
                      phrases.stream().filter(p -> last.getOffsetEnd() < p.getOffsetEnd()));
    
    assertEmptyStream("no phrase should have a start position < first phrase",
                      phrases.stream().filter(p -> p.getPositionStart() < first.getPositionStart()));
    assertEmptyStream("no phrase should have an end position > last phrase",
                      phrases.stream().filter(p -> last.getPositionEnd() < p.getPositionEnd()));
                 

    // NOTE: stuff below this point may not be true for all analyzers (ie: stopwords)
    // but should be valid for the analyzers used in this test...
    // (if we expand test to cover analyzers w/stopwords, refactor this into a new method)
        
    for (int n = 1; n <= maxQueryPositionLength; n++) {
      final int len = n;
      final int expected = Math.max(0, 1 + inputPositionLength - n);
      final List<Phrase> sizeN = phrases.stream().filter(p -> p.getPositionLength() == len
                                                         ).collect(Collectors.toList());
      assertEquals("Expected # phrases of size " + n + ": " + sizeN, expected, sizeN.size());
    }

    // check the quantities of sub-terms/phrases...
    assertEmptyStream("no phrase should have num indexed terms != pos_len",
                      phrases.stream().filter
                      (p -> last.getPositionLength() != last.getIndividualIndexedTerms().size()));
    assertEmptyStream("no phrase should have num sub-phrases != max(1, 1 + pos_len - "+maxIndexedPositionLength+")",
                      phrases.stream().filter
                      (p -> (Math.max(1, 1 + last.getPositionLength() - maxIndexedPositionLength)
                             != last.getLargestIndexedSubPhrases().size())));
    // NOTE: indexed super phrases can be of various lengths, and differing quantities near
    // begining/end of input so don't worry about an exact count, just check their properties (below)

    // check the properties of our sub/super phrases
    for (Phrase phrase : phrases) {
      final String debug = phrase.toString();
      
      assertEmptyStream(debug + " should not have any indexed terms where pos_len != 1",
                        phrase.getIndividualIndexedTerms().stream().filter
                        (term -> 1 != term.getPositionLength()));
      
      assertEmptyStream(debug + " should not have any sub-phrases where pos_len > min(pos_len, "
                        + maxIndexedPositionLength+")",
                        phrase.getLargestIndexedSubPhrases().stream().filter
                        (inner -> (Math.min(phrase.getPositionLength(), maxIndexedPositionLength)
                                   < inner.getPositionLength())));
      
      assertEmptyStream(debug + " should not have any super-phrases where super.len <= phrase.len or " 
                        + maxIndexedPositionLength + " < super.len",
                        phrase.getIndexedSuperPhrases().stream().filter
                        (outer -> (outer.getPositionLength() <= phrase.getPositionLength() ||
                                   maxIndexedPositionLength < outer.getPositionLength())));
    }
  }

  public void testWhiteboxStats() throws Exception {
    final SchemaField analysisField = h.getCore().getLatestSchema().getField("multigrams_body");
    assertNotNull(analysisField);
    final String input = "BROWN fox lAzY  dog xxxyyyzzz";

    // a function we'll re-use on phrases generated from the above input
    // the multiplier let's us simulate multiple shards returning the same values
    BiConsumer<Integer,List<Phrase>> assertions = (mult, phrases) -> {
      final Phrase brown_fox = phrases.get(1);
      assertEquals("BROWN fox", brown_fox.getSubSequence());
      
      assertEquals(mult * 1, brown_fox.getTTF("multigrams_title"));
      assertEquals(mult * 1, brown_fox.getDocFreq("multigrams_title"));
      assertEquals(mult * 1, brown_fox.getConjunctionDocCount("multigrams_title"));
      
      assertEquals(mult * 3, brown_fox.getTTF("multigrams_body"));
      assertEquals(mult * 2, brown_fox.getDocFreq("multigrams_body"));
      assertEquals(mult * 2, brown_fox.getConjunctionDocCount("multigrams_body"));
      
      final Phrase fox_lazy = phrases.get(6);
      assertEquals("fox lAzY", fox_lazy.getSubSequence());
      
      assertEquals(mult * 0, fox_lazy.getTTF("multigrams_title"));
      assertEquals(mult * 0, fox_lazy.getDocFreq("multigrams_title"));
      assertEquals(mult * 1, fox_lazy.getConjunctionDocCount("multigrams_title"));
      
      assertEquals(mult * 0, fox_lazy.getTTF("multigrams_body"));
      assertEquals(mult * 0, fox_lazy.getDocFreq("multigrams_body"));
      assertEquals(mult * 2, fox_lazy.getConjunctionDocCount("multigrams_body"));
      
      final Phrase bfld = phrases.get(3);
      assertEquals("BROWN fox lAzY  dog", bfld.getSubSequence());
      
      expectThrows(SolrException.class, () -> { bfld.getTTF("multigrams_title"); });
      expectThrows(SolrException.class, () -> { bfld.getDocFreq("multigrams_title"); });
      assertEquals(mult * 0, bfld.getConjunctionDocCount("multigrams_title"));
      
      expectThrows(SolrException.class, () -> { bfld.getTTF("multigrams_body"); });
      expectThrows(SolrException.class, () -> { bfld.getDocFreq("multigrams_body"); });
      assertEquals(mult * 1, bfld.getConjunctionDocCount("multigrams_body"));
      
      final Phrase xyz = phrases.get(phrases.size()-1);
      
      assertEquals("xxxyyyzzz", xyz.getSubSequence());
      assertEquals(mult * 0, xyz.getTTF("multigrams_title"));
      assertEquals(mult * 0, xyz.getDocFreq("multigrams_title"));
      assertEquals(mult * 0, xyz.getConjunctionDocCount("multigrams_title"));
      
      assertEquals(mult * 0, xyz.getTTF("multigrams_body"));
      assertEquals(mult * 0, xyz.getDocFreq("multigrams_body"));
      assertEquals(mult * 0, xyz.getConjunctionDocCount("multigrams_body"));
      return;
    };


    final List<Phrase> phrasesLocal = Phrase.extractPhrases(input, analysisField, 3, 7);
    
    // freshly parsed phrases, w/o any stats populated, all the stats should be 0
    assertions.accept(0, phrasesLocal);

    // If we populate with our index stats, we should get the basic values in our BiConsumer
    try (SolrQueryRequest req = req()) {
      Phrase.populateStats(phrasesLocal, Arrays.asList("multigrams_body","multigrams_title"),
                           req.getSearcher());
    }
    assertions.accept(1, phrasesLocal);

    // likewise, if we create a new freshly parsed set of phrases, and "merge" in the previous index stats
    // (ie: merge results from one shard) we should get the same results
    final List<Phrase> phrasesMerged = Phrase.extractPhrases(input, analysisField, 3, 7);
    Phrase.populateStats(phrasesMerged, Phrase.formatShardResponse(phrasesLocal));
    assertions.accept(1, phrasesMerged);

    // if we merge in a second copy of the same results (ie: two identical shards)
    // our results should be double what we had before
    Phrase.populateStats(phrasesMerged, Phrase.formatShardResponse(phrasesLocal));
    assertions.accept(2, phrasesMerged);
    
  }
  
  @SuppressWarnings({"unchecked"})
  public void testWhiteboxScores() throws Exception {
    final SchemaField analysisField = h.getCore().getLatestSchema().getField("multigrams_body");
    assertNotNull(analysisField);
    final Map<String,Double> fieldWeights = new TreeMap<>();
    fieldWeights.put("multigrams_title", 1.0D);
    fieldWeights.put("multigrams_body", 0.0D); // NOTE: 0 weighting should only affect total score
    
    final String input = "xxxyyyzzz BROWN fox why are we lAzY";
    final List<Phrase> phrases = Phrase.extractPhrases(input, analysisField, 3, 7);
    try (SolrQueryRequest req = req()) {
      Phrase.populateStats(phrases, fieldWeights.keySet(), req.getSearcher());
    }
    Phrase.populateScores(phrases, fieldWeights, 3, 7);

    // do some basic sanity checks of the field & total scores...

    for (Phrase xyz : phrases.subList(0, 7)) {
      // first 7 all start with xyz which isn't in index (in either field) so all scores should be -1
      assertEquals(xyz.toString(), -1.0D, xyz.getTotalScore(), 0.0D);
      assertEquals(xyz.toString(), -1.0D, xyz.getFieldScore("multigrams_title"), 0.0D);
      assertEquals(xyz.toString(), -1.0D, xyz.getFieldScore("multigrams_body"), 0.0D);
    }
    
    // any individual terms (past xyz) should score 0.0 because they are all actually in the index
    // (in both fields)
    for (Phrase term : phrases.subList(7, phrases.size()).stream().filter
           ((p -> 1 == p.getPositionLength())).collect(Collectors.toList())) {
      
      assertEquals(term.toString(), 0.0D, term.getFieldScore("multigrams_title"), 0.0D);
      assertEquals(term.toString(), 0.0D, term.getFieldScore("multigrams_body"), 0.0D);
      assertEquals(term.toString(), 0.0D, term.getTotalScore(), 0.0D);
    }

    // "brown fox" should score positively in both fields, and overall...
    final Phrase brown_fox = phrases.get(8);
    assertEquals("BROWN fox", brown_fox.getSubSequence());
    assertThat(brown_fox.toString(), brown_fox.getFieldScore("multigrams_title"), greaterThan(0.0D));
    assertThat(brown_fox.toString(), brown_fox.getFieldScore("multigrams_body"), greaterThan(0.0D) );
    assertThat(brown_fox.toString(), brown_fox.getTotalScore(), greaterThan(0.0D));
    
    // "we lazy" does appear in a title value, but should score poorly given how often the terms
    // are used in other contexts, and should score -1 against body -- but because of our weights,
    // that shouldn't bring down the total
    final Phrase we_lazy = phrases.get(phrases.size()-2);
    assertEquals("we lAzY", we_lazy.getSubSequence());
    assertEquals(we_lazy.toString(), -1.0D, we_lazy.getFieldScore("multigrams_body"), 0.0D);
    assertThat(we_lazy.toString(), we_lazy.getFieldScore("multigrams_title"), lessThan(0.0D));
    assertThat(we_lazy.toString(), we_lazy.getTotalScore(), lessThan(0.0D));
    assertEquals(we_lazy.toString(), we_lazy.getFieldScore("multigrams_title"), we_lazy.getTotalScore(),
                 0.0D);

    // "why are we lazy" is longer then the max indexed phrase size & appears verbatim in a title value
    // it should score -1 against body -- but because of our weights, that shouldn't bring down the total
    final Phrase wawl = phrases.get(phrases.size()-7);
    assertEquals("why are we lAzY", wawl.getSubSequence());
    assertEquals(wawl.toString(), -1.0D, wawl.getFieldScore("multigrams_body"), 0.0D);
    assertThat(wawl.toString(), wawl.getFieldScore("multigrams_title"), greaterThan(0.0D));
    assertThat(wawl.toString(), wawl.getTotalScore(), greaterThan(0.0D));
    assertEquals(wawl.toString(), wawl.getFieldScore("multigrams_title"), wawl.getTotalScore(),
                 0.0D);

    // "brown fox why are we" is longer then the max indexed phrase, and none of it's
    // (longest) sub phrases exists in either field -- so all of it's scores should be -1
    final Phrase bfwaw = phrases.get(11);
    assertEquals("BROWN fox why are we", bfwaw.getSubSequence());
    assertEquals(bfwaw.toString(), -1.0D, bfwaw.getFieldScore("multigrams_title"), 0.0D);
    assertEquals(bfwaw.toString(), -1.0D, bfwaw.getFieldScore("multigrams_body"), 0.0D);
    assertEquals(bfwaw.toString(), -1.0D, bfwaw.getTotalScore(), 0.0D);
    
  }
  
  @SuppressWarnings({"unchecked"})
  public void testWhiteboxScorcesStopwords() throws Exception {
    final String input = "why the lazy dog brown fox";
    final Map<String,Double> fieldWeights = new TreeMap<>();
    fieldWeights.put("multigrams_title", 1.0D); 
    fieldWeights.put("multigrams_title_stop", 1.0D);
    
    { // If our analysisField uses all terms,
      // be we also generate scores from a field that filters stopwords...
      final SchemaField analysisField = h.getCore().getLatestSchema().getField("multigrams_title");
      assertNotNull(analysisField);
      
      final List<Phrase> phrases = Phrase.extractPhrases(input, analysisField, 3, 7);
      try (SolrQueryRequest req = req()) {
        Phrase.populateStats(phrases, fieldWeights.keySet(), req.getSearcher());
      }
      Phrase.populateScores(phrases, fieldWeights, 3, 7);

      // phrases that span the stop word should have valid scores from the field that doesn't care
      // about stop words, but the stopword field should reject them
      final Phrase why_the_lazy = phrases.get(2);
      assertEquals("why the lazy", why_the_lazy.getSubSequence());
      assertThat(why_the_lazy.toString(), why_the_lazy.getFieldScore("multigrams_title"), greaterThan(0.0D) );
      assertEquals(why_the_lazy.toString(), -1.0D, why_the_lazy.getFieldScore("multigrams_title_stop"), 0.0D);
      
      final Phrase the_lazy_dog = phrases.get(8);
      assertEquals("the lazy dog", the_lazy_dog.getSubSequence());
      assertThat(the_lazy_dog.toString(), the_lazy_dog.getFieldScore("multigrams_title"), greaterThan(0.0D) );
      assertEquals(the_lazy_dog.toString(), -1.0D, the_lazy_dog.getFieldScore("multigrams_title_stop"), 0.0D);
      
      // sanity check that good scores are still possible with stopwords
      // "brown fox" should score positively in both fields, and overall...
      final Phrase brown_fox = phrases.get(phrases.size()-2);
      assertEquals("brown fox", brown_fox.getSubSequence());
      assertThat(brown_fox.toString(), brown_fox.getFieldScore("multigrams_title"), greaterThan(0.0D));
      assertThat(brown_fox.toString(), brown_fox.getFieldScore("multigrams_title_stop"), greaterThan(0.0D) );
      assertThat(brown_fox.toString(), brown_fox.getTotalScore(), greaterThan(0.0D));
    }
    
    { // now flip things: our analysisField filters stopwords, 
      // but we also generates scores from a field that doesn't know about them...
      //
      // (NOTE: the parser will still generate _some_ candidate phrases spaning the stop word position,
      // but not ones that start with the stopword)
      final SchemaField analysisField = h.getCore().getLatestSchema().getField("multigrams_title_stop");
      assertNotNull(analysisField);
      
      final List<Phrase> phrases = Phrase.extractPhrases(input, analysisField, 3, 7);
      try (SolrQueryRequest req = req()) {
        Phrase.populateStats(phrases, fieldWeights.keySet(), req.getSearcher());
      }
      Phrase.populateScores(phrases, fieldWeights, 3, 7);
      assertTrue(phrases.toString(), 0 < phrases.size());

      for (Phrase p : phrases) {
        if (p.getPositionStart() <= 2 && 2 < p.getPositionEnd()) {
          // phrases that span the stop word should have valid scores from the field that doesn't care
          // about stop words, but the stopword field should reject them
          assertEquals(p.toString(), -1.0D, p.getFieldScore("multigrams_title"), 0.0D);
          assertEquals(p.toString(), -1.0D, p.getFieldScore("multigrams_title_stop"), 0.0D);
        }
      }
      
      // sanity check that good scores are still possible with stopwords
      // "brown fox" should score positively in both fields, and overall...
      final Phrase brown_fox = phrases.get(phrases.size()-2);
      assertEquals("brown fox", brown_fox.getSubSequence());
      assertThat(brown_fox.toString(), brown_fox.getFieldScore("multigrams_title"), greaterThan(0.0D));
      assertThat(brown_fox.toString(), brown_fox.getFieldScore("multigrams_title_stop"), greaterThan(0.0D) );
      assertThat(brown_fox.toString(), brown_fox.getTotalScore(), greaterThan(0.0D));
    }
    
  }
  
  public void testExpectedUserErrors() throws Exception {
    assertQEx("empty field list should error",
              "must specify a (weighted) list of fields", 
              req("q","foo", "phrases","true",
                  "phrases.fields", " "),
              ErrorCode.BAD_REQUEST);
    
    assertQEx("bogus field name should error",
              "does not exist",
              req("q","foo", "phrases","true",
                  "phrases.fields", "bogus1 bogus2"),
              ErrorCode.BAD_REQUEST);
    
    assertQEx("lack of shingles should cause error",
              "Unable to determine max position length",
              req("q","foo", "phrases","true",
                  "phrases.fields", "title"),
              ErrorCode.BAD_REQUEST);
    
    assertQEx("analyzer missmatch should cause error",
              "must have the same fieldType",
              req("q","foo", "phrases","true",
                  "phrases.fields", "multigrams_title multigrams_title_short"),
              ErrorCode.BAD_REQUEST);
    
    assertQEx("analysis field must exist",
              "does not exist",
              req("q","foo", "phrases","true",
                  "phrases.analysis.field", "bogus",
                  "phrases.fields", "multigrams_title multigrams_title_short"),
              ErrorCode.BAD_REQUEST);

    assertQEx("no query param should error",
              "requires a query string", 
              req("qt", "/phrases",
                  "phrases.fields", "multigrams_title"),
              ErrorCode.BAD_REQUEST);
  }
  
  public void testMaxShingleSizeHelper() throws Exception {
    IndexSchema schema = h.getCore().getLatestSchema();
    
    assertEquals(3, PhrasesIdentificationComponent.getMaxShingleSize
                 (schema.getFieldTypeByName("multigrams_3_7").getIndexAnalyzer()));
    assertEquals(7, PhrasesIdentificationComponent.getMaxShingleSize
                 (schema.getFieldTypeByName("multigrams_3_7").getQueryAnalyzer()));
    
    assertEquals(3, PhrasesIdentificationComponent.getMaxShingleSize
                 (schema.getFieldTypeByName("multigrams_3").getIndexAnalyzer()));
    assertEquals(3, PhrasesIdentificationComponent.getMaxShingleSize
                 (schema.getFieldTypeByName("multigrams_3").getQueryAnalyzer()));
    
    assertEquals(-1, PhrasesIdentificationComponent.getMaxShingleSize
                 (schema.getFieldTypeByName("text").getIndexAnalyzer()));
    assertEquals(-1, PhrasesIdentificationComponent.getMaxShingleSize
                 (schema.getFieldTypeByName("text").getQueryAnalyzer()));
    
  }
  
  public void testSimplePhraseRequest() throws Exception {
    final String input = " did  a Quick    brown FOX perniciously jump over the lazy dog";
    final String expected = " did  a Quick    {brown FOX} perniciously jump over {the lazy dog}";

    // should get same behavior regardless of wether we use "q" or "phrases.q"
    for (String p : Arrays.asList("q", "phrases.q")) {
      // basic request...
      assertQ(req("qt", HANDLER, p, input)
              // expect no search results...
              , "count(//result)=0"
              
              // just phrase info...
              , "//lst[@name='phrases']/str[@name='input'][.='"+input+"']"
              , "//lst[@name='phrases']/str[@name='summary'][.='"+expected+"']"
              , "count(//lst[@name='phrases']/arr[@name='details']/lst) = 2"
              //
              , "//lst[@name='phrases']/arr[@name='details']/lst[1]/str[@name='text'][.='the lazy dog']"
              , "//lst[@name='phrases']/arr[@name='details']/lst[1]/int[@name='offset_start'][.='50']"
              , "//lst[@name='phrases']/arr[@name='details']/lst[1]/int[@name='offset_end'][.='62']"
              , "//lst[@name='phrases']/arr[@name='details']/lst[1]/double[@name='score'][number(.) > 0]"
              //
              , "//lst[@name='phrases']/arr[@name='details']/lst[2]/str[@name='text'][.='brown FOX']"
              , "//lst[@name='phrases']/arr[@name='details']/lst[2]/int[@name='offset_start'][.='17']"
              , "//lst[@name='phrases']/arr[@name='details']/lst[2]/int[@name='offset_end'][.='26']"
              , "//lst[@name='phrases']/arr[@name='details']/lst[2]/double[@name='score'][number(.) > 0]"
              );

      // empty input, empty phrases (and no error)...
      assertQ(req("qt", HANDLER, p, "")
              // expect no search results...
              , "count(//result)=0"
              // just empty phrase info for our empty input...
              , "//lst[@name='phrases']/str[@name='input'][.='']"
              , "//lst[@name='phrases']/str[@name='summary'][.='']"
              , "count(//lst[@name='phrases']/arr[@name='details']) = 1"
              , "count(//lst[@name='phrases']/arr[@name='details']/lst) = 0"
              );
    }
  }
  
  public void testSimpleSearchRequests() throws Exception {
    final String input = "\"brown fox\"";
    
    assertQ(req("q", input)
            // basic search should have worked...
            , "//result[@numFound='2']"
            , "//result/doc/str[@name='id'][.='42']"
            , "//result/doc/str[@name='id'][.='43']"
            // and phrases should not be returned since they weren't requested...
            , "0=count(//lst[@name='phrases'])"
            );
    
    assertQ(req("phrases", "false", "q", input)
            // basic search should have worked...
            , "//result[@numFound='2']"
            , "//result/doc/str[@name='id'][.='42']"
            , "//result/doc/str[@name='id'][.='43']"
            // and phrases should not be returned since they were explicitly disabled...
            , "0=count(//lst[@name='phrases'])"
            );

    // with input this short, all of these permutations of requests should produce the same output...
    for (SolrQueryRequest req : Arrays.asList
           ( // simple, using 3/7 defaults
             req("phrases","true", "q", input),
             
             // simple, using just the 3/3 'short' fields
             req("phrases","true", "q", input,
                 "phrases.fields", "multigrams_body_short multigrams_title_short^2"),
             
             // diff analysers, but explicit override using 3/3 "short" field...
             req("phrases","true", "q", input,
                 "phrases.fields", "multigrams_body multigrams_title_short^2",
                 "phrases.analysis.field", "multigrams_title_short"))) {
      assertQ(req
              // basic search should have worked...
              , "//result[@numFound='2']"
              , "//result/doc/str[@name='id'][.='42']"
              , "//result/doc/str[@name='id'][.='43']"
              
              // and we should have gotten phrase info...
              , "//lst[@name='phrases']/str[@name='input'][.='"+input+"']"
              , "//lst[@name='phrases']/str[@name='summary'][.='\"{brown fox}\"']"
              , "count(//lst[@name='phrases']/arr[@name='details']/lst)=1"
              , "//lst[@name='phrases']/arr[@name='details']/lst/str[@name='text'][.='brown fox']"
              , "//lst[@name='phrases']/arr[@name='details']/lst/int[@name='offset_start'][.='1']"
              , "//lst[@name='phrases']/arr[@name='details']/lst/int[@name='offset_end'][.='10']"
              , "//lst[@name='phrases']/arr[@name='details']/lst/double[@name='score'][number(.) > 0]"
              );
    }

    // override the query string to get different phrases
    assertQ(req("phrases","true", "q", "*:*", "phrases.q",  input)
            // basic search should have found all docs...
            , "//result[@numFound='4']"
            // and we should have gotten phrase info for our alternative q string...
            , "//lst[@name='phrases']/str[@name='input'][.='"+input+"']"
            , "//lst[@name='phrases']/str[@name='summary'][.='\"{brown fox}\"']"
            , "count(//lst[@name='phrases']/arr[@name='details']/lst)=1"
            , "//lst[@name='phrases']/arr[@name='details']/lst/str[@name='text'][.='brown fox']"
            , "//lst[@name='phrases']/arr[@name='details']/lst/int[@name='offset_start'][.='1']"
            , "//lst[@name='phrases']/arr[@name='details']/lst/int[@name='offset_end'][.='10']"
            , "//lst[@name='phrases']/arr[@name='details']/lst/double[@name='score'][number(.) > 0]"
            );
    
    // empty input, empty phrases (but no error)
    assertQ(req("phrases","true", "q", "*:*", "phrases.q", "")
            // basic search should have found all docs...
            , "//result[@numFound='4']"
            // and we should have gotten (empty) phrase info for our alternative q string...
            , "//lst[@name='phrases']/str[@name='input'][.='']"
            , "//lst[@name='phrases']/str[@name='summary'][.='']"
            , "count(//lst[@name='phrases']/arr[@name='details'])     = 1"
            , "count(//lst[@name='phrases']/arr[@name='details']/lst) = 0"
            );
  }
  
  public void testGreyboxShardSearchRequests() throws Exception {
    final String input = "quick brown fox ran";

    final String phrase_xpath = "//lst[@name='phrases']";
    final String all_phrase_xpath = phrase_xpath + "/arr[@name='_all']";

    // phrases requested, and correct request stage / shard purpose ...
    assertQ(req("q", input,
                "phrases","true",
                ShardParams.IS_SHARD, "true",
                ShardParams.SHARDS_PURPOSE, ""+PhrasesIdentificationComponent.SHARD_PURPOSE)
            
            // this shard request should have caused stats to be returned about all phrases...
            , "10=count("+ all_phrase_xpath +"/lst)"
            // "quick" ...
            , all_phrase_xpath + "/lst[1]/lst[@name='ttf']/long[@name='multigrams_body'][.='1']"
            , all_phrase_xpath + "/lst[1]/lst[@name='ttf']/long[@name='multigrams_title'][.='0']"
            // ...
            // "brown fox"
            , all_phrase_xpath + "/lst[6]/lst[@name='ttf']/long[@name='multigrams_body'][.='3']"
            , all_phrase_xpath + "/lst[6]/lst[@name='ttf']/long[@name='multigrams_title'][.='1']"
            , all_phrase_xpath + "/lst[6]/lst[@name='df']/long[@name='multigrams_body'][.='2']"
            , all_phrase_xpath + "/lst[6]/lst[@name='df']/long[@name='multigrams_title'][.='1']"
            , all_phrase_xpath + "/lst[6]/lst[@name='conj_dc']/long[@name='multigrams_body'][.='2']"
            , all_phrase_xpath + "/lst[6]/lst[@name='conj_dc']/long[@name='multigrams_title'][.='1']"
            
            // but no computed "scores"...
            , "0=count("+phrase_xpath+"//*[@name='score'])"
            );

    // phrases requested, but incorrect request stage / shard purpose ...
    assertQ(req("q", input,
                "phrases","true",
                ShardParams.IS_SHARD, "true",
                ShardParams.SHARDS_PURPOSE, ""+ShardRequest.PURPOSE_GET_FIELDS)
            , "0=count("+ phrase_xpath +"/lst)");
    
    // phrases disabled, regardless of request stage / shard purpose ...
    assertTrue("sanity check failed, stage was modified in code w/o updating test",
               PhrasesIdentificationComponent.SHARD_PURPOSE != ShardRequest.PURPOSE_GET_FIELDS);
    assertQ(req("q", input,
                "phrases","false",
                ShardParams.IS_SHARD, "true",
                ShardParams.SHARDS_PURPOSE, ""+ShardRequest.PURPOSE_GET_FIELDS)
            , "0=count("+ phrase_xpath +"/lst)");
    assertQ(req("q", input,
                "phrases","false",
                ShardParams.IS_SHARD, "true",
                ShardParams.SHARDS_PURPOSE, ""+PhrasesIdentificationComponent.SHARD_PURPOSE)
            , "0=count("+ phrase_xpath +"/lst)");
  }


  
  // ////////////////////////////////////////////////////////////////



  
  /** 
   * Trivial Helper method that collects &amp; compares to an empty List so
   * the assertion shows the unexpected stream elements 
   */
  public <T> void assertEmptyStream(final String msg, final Stream<? extends T> stream) {
    assertEquals(msg,
                 Collections.emptyList(),
                 stream.collect(Collectors.toList()));
  }

  /** helper, docs for future junit/hamcrest seems to have something similar */
  @SuppressWarnings({"rawtypes"})
  public static Matcher lessThan(double expected) {
    return new BaseMatcher() {
      @Override public boolean matches(Object actual) {
        return ((Double)actual).compareTo(expected) < 0;
      }
      @Override public void describeTo(Description d) {
        d.appendText("should be less than " + expected);
      }
    };
  }
  /** helper, docs for future junit/hamcrest seems to have something similar */
  @SuppressWarnings({"rawtypes"})
  public static Matcher greaterThan(double expected) {
    return new BaseMatcher() {
      @Override public boolean matches(Object actual) {
        return 0 < ((Double)actual).compareTo(expected);
      }
      @Override public void describeTo(Description d) {
        d.appendText("should be greater than " + expected);
      }
    };
  }
}

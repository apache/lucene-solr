package org.apache.lucene.server;

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

import java.io.File;
import java.util.Locale;

import org.apache.lucene.util._TestUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import net.minidev.json.JSONArray;
import net.minidev.json.JSONObject;

public class TestAnalysis extends ServerBaseTestCase {

  @BeforeClass
  public static void initClass() throws Exception {
    useDefaultIndex = true;
    curIndexName = "index";
    startServer();
    createAndStartIndex();
  }

  @AfterClass
  public static void fini() throws Exception {
    shutdownServer();
  }

  public void testCustomAnalysisChain() throws Exception {
    send("analyze", "{text: 'Here is some text', analyzer: {tokenizer: Standard, tokenFilters: [LowerCase]}}");
    assertEquals("here is some text", justTokens());

    send("analyze", "{text: 'Here is some text', analyzer: {tokenizer: Standard}}");
    assertEquals("Here is some text", justTokens());

    send("analyze", "{text: 'Here is some text', analyzer: {tokenizer: {class: Standard, maxTokenLength: 2}, tokenFilters: [LowerCase]}}");
    assertEquals("is", justTokens());
  }

  public void testPatternTokenizer() throws Exception {
    send("analyze", "{text: 'Here is \\'some\\' text', analyzer: {tokenizer: {class: Pattern, pattern: \"\\'([^\\']+)\\'\", group: 1}}}");
    assertEquals("some", justTokens());
  }

  public void testExtraArgs() throws Exception {
    assertFailsWith("analyze", "{text: 'Here is \\'some\\' text', analyzer: {tokenizer: {class: Pattern, pattern: \"\\'([^\\']+)\\'\", group: 1, bad: 14}}}", "analyze > analyzer > tokenizer: failed to create TokenizerFactory for class \"Pattern\": java.lang.IllegalArgumentException: Unknown parameters: {bad=14}");
  }

  public void testKeywordMarkerFilter() throws Exception {
    // No KWMarkerFilter, dogs is stemmed:
    send("analyze", "{text: 'Here is some dogs', analyzer: {tokenizer: Standard, tokenFilters: [EnglishPossessive, LowerCase, Stop, EnglishMinimalStem]}}");
    assertEquals("here some dog", justTokens());

    // Use KWMarkerFilter with protectedFileContents to protect dogs:
    send("analyze", "{text: 'Here is some dogs', analyzer: {tokenizer: Standard, tokenFilters: [EnglishPossessive, LowerCase, Stop, {class: KeywordMarker, protectedFileContents:[dogs]}, EnglishMinimalStem]}}");
    assertEquals("here some dogs", justTokens());

    // Use KWMarkerFilter with pattern to protect dogs:
    send("analyze", "{text: 'Here is some dogs', analyzer: {tokenizer: Standard, tokenFilters: [EnglishPossessive, LowerCase, Stop, {class: KeywordMarker, pattern: dogs}, EnglishMinimalStem]}}");
    assertEquals("here some dogs", justTokens());
  }

  public void testEnglishAnalyzer() throws Exception {
    send("analyze", "{text: 'dogs go running', analyzer: {class: EnglishAnalyzer}}");
    assertEquals("dog go run", justTokens());

    // This time protecting dogs from stemming:
    send("analyze", "{text: 'dogs go running', analyzer: {class: EnglishAnalyzer, stemExclusionSet: [dogs]}}");
    assertEquals("dogs go run", justTokens());
  }

  public void testStopFilter() throws Exception {
    // Uses default (english) stop words:
    send("analyze", "{text: 'the dogs go running', analyzer: {tokenizer: Whitespace, tokenFilters: [Stop]}}");
    assertEquals("dogs go running", justTokens());

    // This time making only running a stop word:
    send("analyze", "{text: 'the dogs go running', analyzer: {tokenizer: Whitespace, tokenFilters: [{class: Stop, wordsFileContents: [running]}]}}");
    assertEquals("the dogs go", justTokens());
  }

  public void testPositionIncrementGap() throws Exception {
    curIndexName = "posinc";
    _TestUtil.rmDir(new File("posinc"));
    send("createIndex", "{rootDir: posinc}");
    send("settings", "{directory: RAMDirectory}");
    send("registerFields", "{fields: {author1: {type: text, analyzer: {tokenizer: Whitespace}, multiValued: true}, author2: {type: text, analyzer: {tokenizer: Whitespace, positionIncrementGap: 1}, multiValued: true}}}");
    send("startIndex");
    send("addDocument", "{fields: {author1: [bob, smith], author2: [bob, smith]}}");
    long gen = getLong("indexGen");

    // This one matches because the two values act like they
    // were just concatenated:
    send("search", String.format(Locale.ROOT, "{queryText: 'author1: \"bob smith\"', searcher: {indexGen: %d}}", gen));
    assertEquals(1, getInt("hits.length"));

    // This one doesn't match because a hole is inserted
    // between the two values:
    send("search", String.format(Locale.ROOT, "{queryText: 'author2: \"bob smith\"', searcher: {indexGen: %d}}", gen));
    assertEquals(0, getInt("hits.length"));
    send("stopIndex");
    send("deleteIndex");
  }

  // nocommit test loading syns from "file" too
  public void testSynonymFilter() throws Exception {
    send("analyze", "{text: 'domain name service is complex', analyzer: {tokenizer: Whitespace, tokenFilters: [LowerCase, {class: Synonym, ignoreCase: true, analyzer: WhitespaceAnalyzer, synonyms: [{input: 'domain name service', output: 'dns'}]}]}}");
    assertEquals("dns/0 is/1 complex/2", tokensAndPositions());

    send("analyze", "{text: 'domain name service is complex', analyzer: {tokenizer: Whitespace, tokenFilters: [LowerCase, {class: Synonym, ignoreCase: true, analyzer: WhitespaceAnalyzer, synonyms: [{input: 'domain name service', output: 'dns', replace: false}]}]}}");
    assertEquals("domain/0 dns/0:3 name/1 service/2 is/3 complex/4", tokensAndPositions());

    send("analyze", "{text: 'mother knows best', analyzer: {tokenizer: Whitespace, tokenFilters: [LowerCase, {class: Synonym, ignoreCase: true, analyzer: WhitespaceAnalyzer, synonyms: [{input: ['mother', 'mommy'], output: 'mom'}]}]}}");
    assertEquals("mom/0 knows/1 best/2", tokensAndPositions());
  }

  String ONLY_WHITESPACE_RULES = "\\n!!forward;\\n" + 
    "\\n" +
    "$Whitespace = [\\\\p{Whitespace}];\\n" +
    "$NonWhitespace = [\\\\P{Whitespace}];\\n" +
    "$Letter = [\\\\p{Letter}];\\n" +
    "$Number = [\\\\p{Number}];\\n" +
    "# Default rule status is {0}=RBBI.WORD_NONE => not tokenized by ICUTokenizer\\n" +
    "$Whitespace;\\n" +
    "# Assign rule status {200}=RBBI.WORD_LETTER when the token contains a letter char\\n" +
    "# Mapped to <ALPHANUM> token type by DefaultICUTokenizerConfig\\n" +
    "$NonWhitespace* $Letter $NonWhitespace*   {200};\\n" +
    "# Assign rule status {100}=RBBI.WORD_NUM when the token contains a numeric char\\n" +
    "# Mapped to <NUM> token type by DefaultICUTokenizerConfig\\n" +
    "$NonWhitespace* $Number $NonWhitespace*   {100};\\n" +
    "# Assign rule status {1} (no RBBI equivalent) when the token contains neither a letter nor a numeric char\\n" +
    "# Mapped to <OTHER> token type by DefaultICUTokenizerConfig\\n" +
    "$NonWhitespace+   {1};";

  public void testICUTokenizer() throws Exception {
    send("analyze", "{text: 'domain-name service is complex', analyzer: {tokenizer: {class: ICU, rules: [{script: Latn, rules: \"" + ONLY_WHITESPACE_RULES + "\"}]}}}");
    assertEquals("domain-name/0 service/1 is/2 complex/3", tokensAndPositions());
  }

  public void testSpanishLightStem() throws Exception {
    send("analyze", "{text: 'las lomitas', analyzer: {tokenizer: Standard, tokenFilters: [SpanishLightStem]}}");
    assertEquals("las/0 lomit/1", tokensAndPositions());
  }

  private String justTokens() {
    StringBuilder sb = new StringBuilder();
    for(Object _o : (JSONArray) lastResult.get("tokens")) {
      JSONObject token = (JSONObject) _o;
      if (sb.length() > 0) {
        sb.append(' ');
      }
      sb.append(token.get("token"));
    }
    return sb.toString();
  }

  private String tokensAndPositions() {
    StringBuilder sb = new StringBuilder();
    for(Object _o : (JSONArray) lastResult.get("tokens")) {
      JSONObject token = (JSONObject) _o;
      if (sb.length() > 0) {
        sb.append(' ');
      }
      sb.append(token.get("token"));
      sb.append('/');
      sb.append(token.get("position"));
      int posLen = ((Integer) token.get("positionLength")).intValue();
      if (posLen != 1) {
        sb.append(':');
        sb.append(posLen);
      }
    }

    return sb.toString();
  }

  // nocommit need testOffsetGap ... how...
}

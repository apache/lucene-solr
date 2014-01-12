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
    //send("{body: {type: text, analyzer: {tokenizer: StandardTokenizer, tokenFilters: [LowerCaseFilter]}}}", "registerFields");
    //send("{queryParser: {class: classic, defaultField: body}}", "settings");
    JSONObject o = send("analyze", "{text: 'Here is some text', analyzer: {tokenizer: StandardTokenizer, tokenFilters: [LowerCaseFilter]}}");
    assertEquals("here is some text", justTokens(o));

    o = send("analyze", "{text: 'Here is some text', analyzer: {tokenizer: StandardTokenizer}}");
    assertEquals("Here is some text", justTokens(o));

    o = send("analyze", "{text: 'Here is some text', analyzer: {tokenizer: {class: StandardTokenizer, maxTokenLength: 2}, tokenFilters: [LowerCaseFilter]}}");
    assertEquals("is", justTokens(o));

    // test maxTokenLength
  }

  public void testPatternTokenizer() throws Exception {
    JSONObject o = send("analyze", "{text: 'Here is \\'some\\' text', analyzer: {tokenizer: {class: PatternTokenizer, pattern: \"\\'([^\\']+)\\'\", group: 1}}}");
    assertEquals("some", justTokens(o));
  }

  public void testSetKeywordMarkerFilter() throws Exception {
    // No KWMarkerFilter, dogs is stemmed:
    JSONObject o = send("analyze", "{text: 'Here is some dogs', analyzer: {tokenizer: StandardTokenizer, tokenFilters: [EnglishPossessiveFilter, LowerCaseFilter, StopFilter, EnglishMinimalStemFilter]}}");
    assertEquals("here some dog", justTokens(o));

    // KWMarkerFilter protects dogs:
    o = send("analyze", "{text: 'Here is some dogs', analyzer: {tokenizer: StandardTokenizer, tokenFilters: [EnglishPossessiveFilter, LowerCaseFilter, StopFilter, {class: SetKeywordMarkerFilter, keyWords:[dogs]}, EnglishMinimalStemFilter]}}");
    assertEquals("here some dogs", justTokens(o));
  }

  public void testEnglishAnalyzer() throws Exception {
    JSONObject o = send("analyze", "{text: 'dogs go running', analyzer: {class: EnglishAnalyzer}}");
    assertEquals("dog go run", justTokens(o));

    // This time protecting dogs from stemming:
    o = send("analyze", "{text: 'dogs go running', analyzer: {class: EnglishAnalyzer, stemExclusionSet: [dogs]}}");
    assertEquals("dogs go run", justTokens(o));
  }

  public void testPositionIncrementGap() throws Exception {
    curIndexName = "posinc";
    _TestUtil.rmDir(new File("posinc"));
    send("createIndex", "{rootDir: posinc}");
    send("settings", "{directory: RAMDirectory}");
    send("registerFields", "{fields: {author1: {type: text, analyzer: {tokenizer: WhitespaceTokenizer}, multiValued: true}, author2: {type: text, analyzer: {tokenizer: WhitespaceTokenizer, positionIncrementGap: 1}, multiValued: true}}}");
    send("startIndex");
    long gen = getLong(send("addDocument", "{fields: {author1: [bob, smith], author2: [bob, smith]}}"), "indexGen");

    // This one matches because the two values act like they
    // were just concatenated:
    JSONObject result = send("search", String.format(Locale.ROOT, "{queryText: 'author1: \"bob smith\"', searcher: {indexGen: %d}}", gen));
    assertEquals(1, getInt(result, "hits.length"));

    // This one doesn't match because a hole is inserted
    // between the two values:
    result = send("search", String.format(Locale.ROOT, "{queryText: 'author2: \"bob smith\"', searcher: {indexGen: %d}}", gen));
    assertEquals(0, getInt(result, "hits.length"));
    send("stopIndex");
    send("deleteIndex");
  }

  public void testSynonymFilter() throws Exception {
    JSONObject o = send("analyze", "{text: 'domain name service is complex', analyzer: {tokenizer: WhitespaceTokenizer, tokenFilters: [LowerCaseFilter, {class: SynonymFilter, ignoreCase: true, analyzer: WhitespaceAnalyzer, synonyms: [{input: 'domain name service', output: 'dns'}]}]}}");
    assertEquals("dns/0 is/1 complex/2", tokensAndPositions(o));

    o = send("analyze", "{text: 'domain name service is complex', analyzer: {tokenizer: WhitespaceTokenizer, tokenFilters: [LowerCaseFilter, {class: SynonymFilter, ignoreCase: true, analyzer: WhitespaceAnalyzer, synonyms: [{input: 'domain name service', output: 'dns', replace: false}]}]}}");
    assertEquals("domain/0 dns/0:3 name/1 service/2 is/3 complex/4", tokensAndPositions(o));

    o = send("analyze", "{text: 'mother knows best', analyzer: {tokenizer: WhitespaceTokenizer, tokenFilters: [LowerCaseFilter, {class: SynonymFilter, ignoreCase: true, analyzer: WhitespaceAnalyzer, synonyms: [{input: ['mother', 'mommy'], output: 'mom'}]}]}}");
    assertEquals("mom/0 knows/1 best/2", tokensAndPositions(o));
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
    JSONObject o = send("analyze", "{text: 'domain-name service is complex', analyzer: {tokenizer: {class: ICUTokenizer, rules: [{script: Latn, rules: \"" + ONLY_WHITESPACE_RULES + "\"}]}}}");
    assertEquals("domain-name/0 service/1 is/2 complex/3", tokensAndPositions(o));
  }

  private String justTokens(JSONObject o) {
    StringBuilder sb = new StringBuilder();
    for(Object _o : (JSONArray) o.get("tokens")) {
      JSONObject token = (JSONObject) _o;
      if (sb.length() > 0) {
        sb.append(' ');
      }
      sb.append(token.get("token"));
    }
    return sb.toString();
  }

  private String tokensAndPositions(JSONObject o) {
    StringBuilder sb = new StringBuilder();
    for(Object _o : (JSONArray) o.get("tokens")) {
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

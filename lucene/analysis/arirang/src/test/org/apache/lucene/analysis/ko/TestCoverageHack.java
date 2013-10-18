package org.apache.lucene.analysis.ko;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.util.LuceneTestCase;

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

// nocommit: remove this file before merging, its totally bogus
/** A test coverage hack, so we can refactor some things without fear
 * parsed ko_wiki, sentence tokenized, discarded sentences < 25 chars in length,
 * bucked into sentences with hanja and sentences without
 * random-sorted each and took a 100k sample.
 * otherwise we are working with 37.3% test coverage
 */
public class TestCoverageHack extends LuceneTestCase {
  
  public void test() throws Exception {
    KoreanAnalyzer a = new KoreanAnalyzer();
    BufferedReader inputLines = new BufferedReader(new InputStreamReader(getClass().getResourceAsStream("wiki_sentence.txt"), "UTF-8"));
    BufferedReader expectedLines = new BufferedReader(new InputStreamReader(getClass().getResourceAsStream("wiki_results.txt"), "UTF-8"));
    String input = null;
    String expected = null;
    StringBuilder actual = new StringBuilder();
    int line = 0;
    while ((input = inputLines.readLine()) != null) {
      line++;
      expected = expectedLines.readLine();
      assert expected != null;
      actual.setLength(0);
      TokenStream ts = a.tokenStream("bogus", input);
      CharTermAttribute termAtt = ts.addAttribute(CharTermAttribute.class);
      ts.reset();
      while (ts.incrementToken()) {
        actual.append(termAtt.toString());
        actual.append(' ');
      }
      assertEquals("for line " + line + " input: " + input, expected, actual.toString());
      ts.end();
      ts.close();
    }
    assert expectedLines.readLine() == null;
    inputLines.close();
    expectedLines.close();
  }
  
  // run from eclipse to regen
  public void atestGenerateResults() throws Exception {
    KoreanAnalyzer a = new KoreanAnalyzer();
    BufferedReader br = new BufferedReader(new InputStreamReader(getClass().getResourceAsStream("wiki_sentence.txt"), "UTF-8"));
    BufferedWriter w = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("/home/rmuir/workspace/lucene-clean-trunk/lucene/analysis/arirang/src/test/org/apache/lucene/analysis/ko/wiki_results.txt"), "UTF-8"));
    String line = null;
    while ((line = br.readLine()) != null) {
      TokenStream ts = a.tokenStream("bogus", line);
      CharTermAttribute termAtt = ts.addAttribute(CharTermAttribute.class);
      ts.reset();
      while (ts.incrementToken()) {
        w.write(termAtt.toString());
        w.write(' ');
      }
      w.write('\n');
      ts.end();
      ts.close();
    }
    br.close();
    w.close();
  }
}

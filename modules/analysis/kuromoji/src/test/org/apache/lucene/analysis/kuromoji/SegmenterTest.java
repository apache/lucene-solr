package org.apache.lucene.analysis.kuromoji;

/**
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

import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.util.List;

import org.apache.lucene.util.LuceneTestCase;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class SegmenterTest extends LuceneTestCase {
  
  private static Segmenter segmenter;
  
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    segmenter = new Segmenter();
  }
  
  @AfterClass
  public static void afterClass() throws Exception {
    segmenter = null;
  }
  
  @Test
  public void testSegmentation() {
    // Skip tests for Michelle Kwan -- UniDic segments Kwan as ク ワン
    //		String input = "ミシェル・クワンが優勝しました。スペースステーションに行きます。うたがわしい。";
    //		String[] surfaceForms = {
    //				"ミシェル", "・", "クワン", "が", "優勝", "し", "まし", "た", "。",
    //				"スペース", "ステーション", "に", "行き", "ます", "。",
    //				"うたがわしい", "。"
    //		};
    String input = "スペースステーションに行きます。うたがわしい。";
    String[] surfaceForms = {
        "スペース", "ステーション", "に", "行き", "ます", "。",
        "うたがわしい", "。"
    };
    List<Token> tokens = segmenter.tokenize(input);
    assertTrue(tokens.size() == surfaceForms.length);
    for (int i = 0; i < tokens.size(); i++) {
      assertEquals(surfaceForms[i], tokens.get(i).getSurfaceFormString());
    }
  }
  
  @Test
  public void testReadings() {
    List<Token> tokens = segmenter.tokenize("寿司が食べたいです。");
    assertEquals(6, tokens.size());
    assertEquals("スシ", tokens.get(0).getReading());
    assertEquals("ガ",    tokens.get(1).getReading());
    assertEquals("タベ", tokens.get(2).getReading());
    assertEquals("タイ",  tokens.get(3).getReading());
    assertEquals("デス", tokens.get(4).getReading());
    assertEquals("。", tokens.get(5).getReading());
  }
  
  @Test
  public void testReadings2() {
    List<Token> tokens = segmenter.tokenize("多くの学生が試験に落ちた。");
    assertEquals(9, tokens.size());
    assertEquals("オオク", tokens.get(0).getReading());
    assertEquals("ノ", tokens.get(1).getReading());
    assertEquals("ガクセイ", tokens.get(2).getReading());
    assertEquals("ガ", tokens.get(3).getReading());
    assertEquals("シケン", tokens.get(4).getReading());
    assertEquals("ニ", tokens.get(5).getReading());
    assertEquals("オチ", tokens.get(6).getReading());
    assertEquals("タ", tokens.get(7).getReading());
    assertEquals("。", tokens.get(8).getReading());
  }
  
  @Test
  public void testPronunciations() {
    List<Token> tokens = segmenter.tokenize("寿司が食べたいです。");
    assertEquals(6, tokens.size());
    assertEquals("スシ", tokens.get(0).getPronunciation());
    assertEquals("ガ",    tokens.get(1).getPronunciation());
    assertEquals("タベ", tokens.get(2).getPronunciation());
    assertEquals("タイ",  tokens.get(3).getPronunciation());
    assertEquals("デス", tokens.get(4).getPronunciation());
    assertEquals("。", tokens.get(5).getPronunciation());
  }
  
  @Test
  public void testPronunciations2() {
    List<Token> tokens = segmenter.tokenize("多くの学生が試験に落ちた。");
    assertEquals(9, tokens.size());
    // pronunciation differs from reading here
    assertEquals("オーク", tokens.get(0).getPronunciation());
    assertEquals("ノ", tokens.get(1).getPronunciation());
    assertEquals("ガクセイ", tokens.get(2).getPronunciation());
    assertEquals("ガ", tokens.get(3).getPronunciation());
    assertEquals("シケン", tokens.get(4).getPronunciation());
    assertEquals("ニ", tokens.get(5).getPronunciation());
    assertEquals("オチ", tokens.get(6).getPronunciation());
    assertEquals("タ", tokens.get(7).getPronunciation());
    assertEquals("。", tokens.get(8).getPronunciation());
  }
  
  @Test
  public void testBasicForms() {
    List<Token> tokens = segmenter.tokenize("それはまだ実験段階にあります。");
    assertEquals(9, tokens.size());
    assertNull(tokens.get(0).getBaseForm());
    assertNull(tokens.get(1).getBaseForm());
    assertNull(tokens.get(2).getBaseForm());
    assertNull(tokens.get(3).getBaseForm());
    assertNull(tokens.get(4).getBaseForm());
    assertNull(tokens.get(5).getBaseForm());
    assertEquals(tokens.get(6).getBaseForm(), "ある");
    assertNull(tokens.get(7).getBaseForm());
    assertNull(tokens.get(8).getBaseForm());
  }
  
  @Test
  public void testInflectionTypes() {
    List<Token> tokens = segmenter.tokenize("それはまだ実験段階にあります。");
    assertEquals(9, tokens.size());
    assertNull(tokens.get(0).getInflectionType());
    assertNull(tokens.get(1).getInflectionType());
    assertNull(tokens.get(2).getInflectionType());
    assertNull(tokens.get(3).getInflectionType());
    assertNull(tokens.get(4).getInflectionType());
    assertNull(tokens.get(5).getInflectionType());
    assertEquals("五段・ラ行", tokens.get(6).getInflectionType());
    assertEquals("特殊・マス", tokens.get(7).getInflectionType());
    assertNull(tokens.get(8).getInflectionType());
  }
  
  @Test
  public void testInflectionForms() {
    List<Token> tokens = segmenter.tokenize("それはまだ実験段階にあります。");
    assertEquals(9, tokens.size());
    assertNull(tokens.get(0).getInflectionForm());
    assertNull(tokens.get(1).getInflectionForm());
    assertNull(tokens.get(2).getInflectionForm());
    assertNull(tokens.get(3).getInflectionForm());
    assertNull(tokens.get(4).getInflectionForm());
    assertNull(tokens.get(5).getInflectionForm());
    assertEquals("連用形", tokens.get(6).getInflectionForm());
    assertEquals("基本形", tokens.get(7).getInflectionForm());
    assertNull(tokens.get(8).getInflectionForm());
  }
  
  @Test
  public void testPartOfSpeech() {
    List<Token> tokens = segmenter.tokenize("それはまだ実験段階にあります。");
    assertEquals(9, tokens.size());
    assertEquals("名詞-代名詞-一般",  tokens.get(0).getPartOfSpeech());
    assertEquals("助詞-係助詞",    tokens.get(1).getPartOfSpeech());
    assertEquals("副詞-助詞類接続", tokens.get(2).getPartOfSpeech());
    assertEquals("名詞-サ変接続",   tokens.get(3).getPartOfSpeech());
    assertEquals("名詞-一般",      tokens.get(4).getPartOfSpeech());
    assertEquals("助詞-格助詞-一般",  tokens.get(5).getPartOfSpeech());
    assertEquals("動詞-自立",      tokens.get(6).getPartOfSpeech());
    assertEquals("助動詞",       tokens.get(7).getPartOfSpeech());
    assertEquals("記号-句点",      tokens.get(8).getPartOfSpeech());
  }

  // TODO: the next 2 tests are no longer using the first/last word ids, maybe lookup the words and fix?
  // do we have a possibility to actually lookup the first and last word from dictionary?
  public void testYabottai() {
    List<Token> tokens = segmenter.tokenize("やぼったい");
    assertEquals(1, tokens.size());
    assertEquals("やぼったい", tokens.get(0).getSurfaceFormString());
  }

  public void testTsukitosha() {
    List<Token> tokens = segmenter.tokenize("突き通しゃ");
    assertEquals(1, tokens.size());
    assertEquals("突き通しゃ", tokens.get(0).getSurfaceFormString());
  }

  public void testBocchan() throws Exception {
    doTestBocchan(1);
  }
  
  @Test @Nightly
  public void testBocchanBig() throws Exception {
    doTestBocchan(100);
  }
  
  private void doTestBocchan(int numIterations) throws Exception {
    LineNumberReader reader = new LineNumberReader(new InputStreamReader(
        this.getClass().getResourceAsStream("bocchan.utf-8")));
    
    String line = reader.readLine();
    reader.close();
    
    if (VERBOSE) {
      System.out.println("Test for Bocchan without pre-splitting sentences");
    }
    long totalStart = System.currentTimeMillis();
    for (int i = 0; i < numIterations; i++){
      segmenter.tokenize(line);
    }
    if (VERBOSE) {
      System.out.println("Total time : " + (System.currentTimeMillis() - totalStart));
      System.out.println("Test for Bocchan with pre-splitting sentences");
    }
    String[] sentences = line.split("、|。");
    totalStart = System.currentTimeMillis();
    for (int i = 0; i < numIterations; i++) {
      for (String sentence: sentences) {
        segmenter.tokenize(sentence);       
      }
    }
    if (VERBOSE) {
      System.out.println("Total time : " + (System.currentTimeMillis() - totalStart));
    }
  }
}

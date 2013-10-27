package org.apache.lucene.analysis.ko;

import java.util.ArrayList;
import java.util.List;

import junit.framework.TestCase;

import org.apache.lucene.analysis.ko.dic.CompoundEntry;
import org.apache.lucene.analysis.ko.morph.AnalysisOutput;
import org.apache.lucene.analysis.ko.morph.MorphAnalyzer;

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

public class TestMorphologicalAnalyzer extends TestCase {
  
  /**
   * An example sentence, test removal of particles
   * MorphAnalyzer has a input string after being tokenized by KoreanTokenizer
   * @throws Exception  throw exception
   */
  public void testMorphAnalyze() throws Exception {
  
    MorphAnalyzer morphAnalyzer = new MorphAnalyzer(false);
    
    assertEquals(extractStem(morphAnalyzer, "냉방을"), "냉방");
    
    assertEquals(extractStem(morphAnalyzer, "하고"), "하");
    
    assertEquals(extractStem(morphAnalyzer, "전기를"), "전기");
    
    assertEquals(extractStem(morphAnalyzer, "만들고"), "만들");
    
    assertEquals(extractStem(morphAnalyzer, "공장을"), "공장");
    
    assertEquals(extractStem(morphAnalyzer, "가동하는"), "가동");
    
    assertEquals(extractStem(morphAnalyzer, "가스"), "가스");
    
    assertEquals(extractStem(morphAnalyzer, "서민에게는"), "서민");
    
    assertEquals(extractStem(morphAnalyzer, "필수"), "필수");
    
    assertEquals(extractStem(morphAnalyzer, "생활"), "생활");
    
    assertEquals(extractStem(morphAnalyzer, "에너지이다"), "에너지");
  }
  
  /**
   * 
   * @param morphAnalyzer the Korean Morphlogical Analyzer
   * @param input the input phrase
   * @return  the stem extracted from the input
   */
  private String extractStem(MorphAnalyzer morphAnalyzer, String input) {
    
    List<AnalysisOutput> results = morphAnalyzer.analyze(input);
    
    // if fail to analyze, return the input
    if(results.size()==0) return input;
    
    // It is likely to be more than one result because of the ambiguity of the input phrase.
    // The result is ordered by the analyzing score
    // the first result has the most high score of the results.
    return results.get(0).getStem();
  }
  
  /**
   * An compound noun being segmented by a unit word
   * @throws Exception  throw exception
   */
  public void testCompoundNoun() throws Exception {
    
    MorphAnalyzer morphAnalyzer = new MorphAnalyzer(false);
    
    assertArrayEquals(splitByUnitWord(morphAnalyzer, "과학기술연구과제가"), 
        new String[]{"과학","기술","연구","과제"});
    
    assertArrayEquals(splitByUnitWord(morphAnalyzer, "대형화되고"), 
        new String[]{"대형화"});
    
    assertArrayEquals(splitByUnitWord(morphAnalyzer, "연구개발주기가"), 
        new String[]{"연구","개발","주기"});
    
    assertArrayEquals(splitByUnitWord(morphAnalyzer, "단축되면서"), 
        new String[]{"단축"});

    assertArrayEquals(splitByUnitWord(morphAnalyzer, "기술을"), 
        new String[]{"기술"});

    assertArrayEquals(splitByUnitWord(morphAnalyzer, "평가하는"), 
        new String[]{"평가"});
    
    assertArrayEquals(splitByUnitWord(morphAnalyzer, "기준이"), 
        new String[]{"기준"});
    
    assertArrayEquals(splitByUnitWord(morphAnalyzer, "달라지고"), 
        new String[]{"달라지"});
    
    assertArrayEquals(splitByUnitWord(morphAnalyzer, "있다"), 
        new String[]{"있"});    
    
  }
  
  /**
   * 
   * @param morphAnalyzer the Korean Morphlogical Analyzer 
   * @param input the input text to be segmented by unit word
   * @return  the segmented texts
   * @throws Exception  throw exception
   */
  private String[] splitByUnitWord(MorphAnalyzer morphAnalyzer, String input) throws Exception {
    
    List<AnalysisOutput> results = morphAnalyzer.analyze(input);
    
    // if fail to analyze, return the input
    if(results.size()==0) return new String[]{input};
    
    AnalysisOutput output = results.get(0);
    
    if(output.getCNounList().size()<2)
      return new String[] {output.getStem()};
    
    List<String> nounList = new ArrayList<String>();
    for(CompoundEntry entry : output.getCNounList()) {
      nounList.add(entry.getWord());
    }
    
    return nounList.toArray(new String[0]);
  }
  
  /**
   * 
   * @param sources source text
   * @param targets target text
   * @throws Exception  throw exception
   */
  private void assertArrayEquals(String[] sources, String[] targets) throws Exception {
    
    assertEquals(sources.length, targets.length);
    
    for(int i=0; i< sources.length;i++) {
      assertEquals(sources[i], targets[i]);
    }
    
  }
  
  
}

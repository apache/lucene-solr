package org.apache.lucene.analysis.ko;

import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.analysis.ko.dic.CompoundEntry;
import org.apache.lucene.analysis.ko.morph.CompoundNounAnalyzer;

import junit.framework.TestCase;

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

public class TestCompoundSegment extends TestCase {
  

  /**
   * segment a compound noun into unit nouns
   * @throws Exception  throw exception
   */
  public void testSegmentCompound() throws Exception {
    
    CompoundNounAnalyzer analyzer = new CompoundNounAnalyzer(false);
    
    assertArrayEquals(splitByUnitWord(analyzer, "연구개발과제")
        ,new String[]{"연구","개발","과제"});
    
    assertArrayEquals(splitByUnitWord(analyzer, "연구개발주기")
        ,new String[]{"연구","개발","주기"});
  }
  
  /**
   * 
   * @param analyzer the Compound noun segment Analyzer 
   * @param input the input text to be segmented by unit word
   * @return  the segmented texts
   * @throws Exception  throw exception
   */
  private String[] splitByUnitWord(CompoundNounAnalyzer analyzer, String input) throws Exception {
    
    CompoundEntry results[] = analyzer.analyze(input);
        
    List<String> nounList = new ArrayList<String>();
    for(CompoundEntry entry : results) {
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

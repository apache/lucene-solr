package org.apache.lucene.analysis.ko.morph;

import java.util.Iterator;
import java.util.List;

import org.apache.lucene.analysis.ko.dic.DictionaryUtil;

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

public class AbbrevAnalyzer {
  
  MorphAnalyzer morphAnalyzer;
  
  public AbbrevAnalyzer(MorphAnalyzer analyzer) {
    this.morphAnalyzer = analyzer;
  }
  
  public void analyze(String input, List<AnalysisOutput> candidates) {
    
    if(candidates==null ||
        candidates.size()==0 ||
        candidates.get(0).getScore()>AnalysisOutput.SCORE_ANALYSIS) return;
    
    String eMorph[] = findAbbrv(input);
    if(eMorph[0]==null) return;
    
    String normInput = input.substring(0, input.length()-eMorph[0].length());
//    morphAnalyzer.analyze(normInput, pos);
  }
  
  private String lookupDictionary(String input) {
  
    String word = null;
    
    for(int i=1; i<input.length();i++) {
      String snippet = input.substring(0,i);
      
      Iterator<String[]> entries = DictionaryUtil.findWithPrefix(snippet);
      if(!entries.hasNext()) break;
      
      word = snippet;
    }
    
    return word;
  }
  
  private String[] findAbbrv(String end) {
    String[] abbrev = new String[2];
    
    for(int i=end.length()-1;i>0;i--) {
      String text = end.substring(i);
      String morph = DictionaryUtil.getAbbrevMorph(text);
      if(morph==null) continue;
      abbrev[0] = text;
      abbrev[1] = morph;
    }
    return abbrev;
  }
}

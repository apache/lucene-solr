package org.apache.lucene.analysis.ko.morph;

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

import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.analysis.ko.dic.CompoundEntry;

class WSOutput  implements Cloneable {

  private int lastStart = 0; // nocommit, seems unused?
  private int lastEnd = 0;  
  private List<AnalysisOutput> phrases = new ArrayList<AnalysisOutput>();
  
  private void setLastStart(int start) {
    this.lastStart = start;
  }

  int getLastEnd() {
    return lastEnd;
  }

  private void setLastEnd(int end) {
    this.lastStart = end;
  }
  
  List<AnalysisOutput> getPhrases() {
    return phrases;
  }

  void removeLast() {
    if (phrases.isEmpty()) {
      return;
    }
        
    AnalysisOutput o = phrases.remove(phrases.size()-1);
    
    if (phrases.isEmpty()) {
      lastStart = 0;
      lastEnd = 0;
    } else {
      lastEnd -= o.getSource().length();
      if (phrases.size() > 1) {
        AnalysisOutput o1 = phrases.get(phrases.size()-1);
        this.lastStart = lastEnd - o1.getSource().length();
      } else {
        this.lastStart = 0;
      }
    }
  }
  
  void addPhrase(AnalysisOutput o) {
    lastStart = lastEnd;
    lastEnd += o.getSource().length();
    
    if (o.getCNounList().size() == 0)
      phrases.add(o);
    else
      addCompounds(o);
  }
  
  private void addCompounds(AnalysisOutput o) {
    
    List<CompoundEntry> cnouns = o.getCNounList();
    String source = o.getSource();    

    for (int i = 0; i < cnouns.size() - 1; i++) {
      
      String noun = cnouns.get(i).getWord();      
      boolean isOnechar = false;
    
      // 접두어는 처음 음절에만 온다. 복합명사 분해규칙
      // 처음이 아닌 경우 1글자는 앞 문자와 결합한다.
      if (cnouns.get(i).getWord().length() == 1 ||
          cnouns.get(i+1).getWord().length() == 1) { // 접두어는 처음 음절에만 온다. 복합명사 분해규칙
        noun += cnouns.get(i+1).getWord();      
        isOnechar = true;
      }
      
      if (isOnechar && i >= cnouns.size()-2) {
        break;
      }
            
      int score = AnalysisOutput.SCORE_CORRECT;
      if (!cnouns.get(i).isExist()) {
        score = AnalysisOutput.SCORE_CANDIDATE;
      }
      
      AnalysisOutput o1 = new AnalysisOutput(noun, null, null, PatternConstants.POS_NOUN, PatternConstants.PTN_N, score);
      
      o1.setSource(noun);
      
      if (isOnechar) {
        o1.addCNoun(cnouns.get(i));
        o1.addCNoun(cnouns.get(i+1));
      }
    
      if (source.length()>noun.length()) {
        source = source.substring(noun.length());
      }
      
      phrases.add(o1);
      cnouns.remove(0);
      i--;
      
      if (isOnechar) {
        cnouns.remove(0);
      }
    }
    
    o.setStem(o.getStem().substring(o.getSource().length() - source.length()));
    o.setSource(source);
    if (cnouns.size() == 1) { 
      cnouns.clear();
    }
  
    phrases.add(o);
  }
  
  private void setPhrases(List<AnalysisOutput> phrases) {
    this.phrases = phrases;
  }
  
  public WSOutput clone() throws CloneNotSupportedException {
        
    WSOutput candidate = (WSOutput)super.clone(); // FIXME: What's this? -Christian
    
    candidate.setLastStart(lastStart);
    candidate.setLastEnd(lastEnd);
    
    List<AnalysisOutput> list = new ArrayList<AnalysisOutput>();
    list.addAll(phrases);
    candidate.setPhrases(list);
    
    return candidate;
  }
}

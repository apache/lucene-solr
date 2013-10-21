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

import java.util.List;

import org.apache.lucene.analysis.ko.dic.SyllableFeatures;

class MorphUtil {
  private MorphUtil() {}

  private static final char[] CHOSEONG = {
      'ㄱ','ㄲ','ㄴ','ㄷ','ㄸ','ㄹ','ㅁ','ㅂ','ㅃ','ㅅ',
      'ㅆ','ㅇ','ㅈ','ㅉ','ㅊ','ㅋ','ㅌ','ㅍ','ㅎ'
  };

  private static final char[] JUNGSEONG = {
      'ㅏ','ㅐ','ㅑ','ㅒ','ㅓ','ㅔ','ㅕ','ㅖ','ㅗ','ㅘ',
      'ㅙ','ㅚ','ㅛ','ㅜ','ㅝ','ㅞ','ㅟ','ㅠ','ㅡ','ㅢ',
      'ㅣ'
  };
  
  private static final char[] JONGSEONG = {
      '\0','ㄱ','ㄲ','ㄳ','ㄴ','ㄵ','ㄶ','ㄷ','ㄹ','ㄺ',
      'ㄻ','ㄼ','ㄽ','ㄾ','ㄿ','ㅀ','ㅁ','ㅂ','ㅄ','ㅅ',
      'ㅆ','ㅇ','ㅈ','ㅊ','ㅋ','ㅌ','ㅍ','ㅎ'
  };
  
  private static final int JUNG_JONG = JUNGSEONG.length * JONGSEONG.length;

  
  /**
   * 한글 한글자를 초성/중성/종성의 배열로 만들어 반환한다.
   * @param c the character to be decomposed
   */
  static char[] decompose(char c) {
    char[] result = null;

    if(c>0xD7A3||c<0xAC00) return new char[]{c};
    
    c -= 0xAC00;

    char choseong = CHOSEONG[c/JUNG_JONG];
    c = (char)(c % JUNG_JONG);
    
    char jungseong = JUNGSEONG[c/JONGSEONG.length];
    
    char jongseong = JONGSEONG[c%JONGSEONG.length];
    
    if(jongseong != 0) {
      result = new char[] {choseong, jungseong, jongseong};
    }else {
      result = new char[] {choseong, jungseong};      
    }
    return result;
  }  
  
  static char compound(int first, int middle, int last) {    
    return (char)(0xAC00 + first* JUNG_JONG + middle * JONGSEONG.length + last);
  }
  
  static char makeChar(char ch, int mdl, int last) {    
    ch -= 0xAC00;    
    int first = ch/JUNG_JONG;     
    return compound(first,mdl,last);
  }
  
  static char makeChar(char ch, int last) {
    ch -= 0xAC00;    
    int first = ch/JUNG_JONG;  
    ch = (char)(ch % JUNG_JONG);
    int middle = ch/JONGSEONG.length;
    
    return compound(first,middle,last);    
  }
  
  static char replaceJongsung(char dest, char source) {
    source -= 0xAC00;    
    int last = source % JONGSEONG.length;
      
    return makeChar(dest,last);  
  }
  
  static void buildPtnVM(AnalysisOutput output, List<AnalysisOutput> candidates) {
    
    String end = output.getEomi();
    if(output.getPomi()!=null) end = output.getPomi();
    
    output.setPatn(PatternConstants.PTN_VM);
    output.setPos(PatternConstants.POS_VERB);
    
    if(output.getScore()==AnalysisOutput.SCORE_CORRECT) {
      candidates.add(output);
    }else {
      String[] irrs = IrregularUtil.restoreIrregularVerb(output.getStem(),end);
      if(irrs!=null) {
        output.setScore(AnalysisOutput.SCORE_CORRECT);
        output.setStem(irrs[0]);
        candidates.add(output);  
      }
    }
    
  }

  static boolean hasVerbOnly(String input) {
    for (int i = input.length()-1; i >=0; i--) {
      if (SyllableFeatures.hasFeature(input.charAt(i), SyllableFeatures.WDSURF)) {
        assert input.length() > i;
        return true;
      }
    }
    return false;
  }
}

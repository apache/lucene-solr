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

import org.apache.lucene.analysis.ko.dic.CompoundEntry;
import org.apache.lucene.analysis.ko.dic.DictionaryUtil;
import org.apache.lucene.analysis.ko.dic.WordEntry;

/**
 * 복합명사를 분해한다.
 */
public class CompoundNounAnalyzer {
  private final boolean exactMatch;
  
  public CompoundNounAnalyzer(boolean exactMatch) {
    this.exactMatch = exactMatch;
  }

  /** Returns decompounded list for word, or null */
  public CompoundEntry[] analyze(String input) {
    if (input.length() < 3 || input.length() > 20) {
      // ignore less than 3 letters or more than 20 letters.
      return null;
    }
    WordEntry entry = DictionaryUtil.getCompoundNoun(input);
    if (entry != null) {
      return entry.getCompounds();
    } else {
      return analyze(input, true);
    }
  }
    
  private CompoundEntry[] analyze(String input, boolean isFirst) {    
    switch (input.length()) {
      case 3: return analyze3Word(input, isFirst);
      case 4: return analyze4Word(input, isFirst);
      case 5: return analyze5Word(input, isFirst);
      default: return analyzeLongText(input, isFirst);
    }
  }
  
  private static final int[] UNITS_1_2   = {1, 2};
  private static final int[] UNITS_2_1   = {2, 1};

  private CompoundEntry[] analyze3Word(String input, boolean isFirst) {
    CompoundEntry[] entries = analysisBySplited(UNITS_2_1, input, isFirst);
    if (entries != null && entries[0].isExist() && entries[1].isExist()) {
      return entries;    
    }

    entries = analysisBySplited(UNITS_1_2, input, isFirst);
    if (entries !=null && entries[0].isExist() && entries[1].isExist()) {
      return entries;
    }
          
    return null;
  } 
  
  private static final int[] UNITS_1_3   = {1, 3};
  private static final int[] UNITS_2_2   = {2, 2};
  private static final int[] UNITS_3_1   = {3, 1};
  private static final int[] UNITS_1_2_1 = {1, 2, 1};
  
  private CompoundEntry[] analyze4Word(String input, boolean isFirst) {   
    if (!isFirst) {
      CompoundEntry[] entries = analysisBySplited(UNITS_1_3, input, false);
      if (entries != null && entries[0].isExist() && entries[1].isExist()) {
        return entries;
      }
    }

    CompoundEntry[] entries3 = analysisBySplited(UNITS_3_1, input, isFirst);
    if (entries3 != null && entries3[0].isExist() && entries3[1].isExist()) {
      return entries3;    
    }
    
    CompoundEntry[] entries2 = analysisBySplited(UNITS_2_2, input, isFirst);
    if (entries2 != null && entries2[0].isExist() && entries2[1].isExist()) {
      return entries2;
    }
    
    CompoundEntry[] entries1 = analysisBySplited(UNITS_1_2_1, input, isFirst); 
    if (entries1 != null && entries1[0].isExist() && entries1[1].isExist() && entries1[2].isExist()) {
      return entries1;
    }

    if (!exactMatch && entries2 != null && (entries2[0].isExist() || entries2[1].isExist())) {
      return entries2;
    }
    
    return null;
  }

  private static final int[] UNITS_2_3     = {2, 3};
  private static final int[] UNITS_3_2     = {3, 2};
  private static final int[] UNITS_4_1     = {4, 1};
  private static final int[] UNITS_2_1_2   = {2, 1, 2};
  private static final int[] UNITS_2_2_1   = {2, 2, 1};
  
  private CompoundEntry[] analyze5Word(String input, boolean isFirst) {
      
    CompoundEntry[] entries1 = analysisBySplited(UNITS_2_3, input, isFirst);
    if (entries1 != null && entries1[0].isExist() && entries1[1].isExist()) {
      return entries1;    
    }
    
    CompoundEntry[] entries2 = analysisBySplited(UNITS_3_2, input, isFirst);
    if (entries2 != null && entries2[0].isExist() && entries2[1].isExist()) {
      return entries2;    
    }
    
    CompoundEntry[] entries_1 = analysisBySplited(UNITS_4_1, input, isFirst);
    if (entries_1 != null && entries_1[0].isExist() && entries_1[1].isExist()) {     
      return entries_1;
    }
    
    CompoundEntry[] entries3 = analysisBySplited(UNITS_2_2_1, input, isFirst);
    if (entries3 != null && entries3[0].isExist() && entries3[1].isExist() && entries3[2].isExist()) {     
      return entries3;
    }
    
    CompoundEntry[] entries4 = analysisBySplited(UNITS_2_1_2, input, isFirst);
    if (entries4 != null && entries4[0].isExist() && entries4[1].isExist() && entries4[2].isExist()) {     
      return entries4;
    }
    
    if (!exactMatch && entries1 != null && (entries1[0].isExist() || entries1[1].isExist())) {
      return entries1;
    }
    
    if (!exactMatch && entries2 != null && (entries2[0].isExist() || entries2[1].isExist())) {  
      return entries2;
    }
    
    CompoundEntry[] res = null;
    if (!exactMatch && entries3 != null && (entries3[0].isExist() || entries3[1].isExist())) {
      res = entries3;
    } 
    
    if (!exactMatch && entries4 != null && (entries4[0].isExist() || entries4[2].isExist())) {
      if (res == null) {
        res = entries4;
      } else {
        CompoundEntry[] both = new CompoundEntry[res.length + entries4.length];
        System.arraycopy(res, 0, both, 0, res.length);
        System.arraycopy(entries4, 0, both, res.length, entries4.length);
        res = both;
      }
    }   
    
    return res;
  }
  
  /** 
   * analyzes one part of the input recursively: called by analyzeLongText
   */
  private CompoundEntry[] analyzeLongPart(String input) {
    WordEntry e = DictionaryUtil.getAllNoun(input);
    if (e == null) {
      return analyze(input, false);
    } else {
      if (e.isCompoundNoun()) {
        return e.getCompounds();
      } else {
        return new CompoundEntry[] { new CompoundEntry(input, true) };
      }
    }
  }
   
  private CompoundEntry[] analyzeLongText(String input, boolean isFirst) {
    int len = input.length();

    boolean hasSuffix = isFirst && existSuffix(input.charAt(len-1));        
    int pos = calculatePos(input, hasSuffix);
    if (pos < 1) {
      return null; // fail to search a valid word segment
    }
    
    // whole word (or word+suffix)
    if (pos == input.length()) {     
      if (hasSuffix) {
        return new CompoundEntry[] { 
            new CompoundEntry(input.substring(0, len-1), true),
            new CompoundEntry(input.substring(len-1), true)
        };
      } else {
        return new CompoundEntry[] { new CompoundEntry(input, true) };
      } 
    }
    
    String prev = input.substring(0, pos);
    String rear = input.substring(pos);
    
    CompoundEntry[] pRes = analyzeLongPart(prev);
    CompoundEntry[] rRes = analyzeLongPart(rear);
    
    if (pRes == null && rRes == null) {
      return null; // no good split
    } else if (pRes == null) {
      pRes = new CompoundEntry[] { new CompoundEntry(prev, false) };
    } else if (rRes == null) {
      rRes = new CompoundEntry[] { new CompoundEntry(rear, false) };
    }
    
    CompoundEntry result[] = new CompoundEntry[pRes.length + rRes.length];
    System.arraycopy(pRes, 0, result, 0, pRes.length);
    System.arraycopy(rRes, 0, result, pRes.length, rRes.length);
    return result;
  }
  
  /**
   * calculate the position at which the long input should be divided into two segments.
   * @param input the input string
   * @return  the position
   */
  private int calculatePos(String input, boolean hasSuffix) {
  
    int pos = -1;
    int len = input.length();
    
    int maxlen = 0;
    for(int i=len-2;i>=0;i--) {
      
      String text = input.substring(i); 
      String prvText = input.substring(0,i+1);
      
      int curmax = maxWord(text, hasSuffix, prvText);
      
      if(curmax>maxlen) {
        maxlen = curmax;
        if(i==0) pos = curmax;
        else pos = i;
      }
    }
    
    return pos;
  }
  
  /**
   * find the max length of a word contained in a input text
   * @param text  input text
   * @param hasSuffix   whether the input text is including a suffix character at the end
   * @return  the max length
   */
  private int maxWord(String text, boolean hasSuffix, String prvText) {    
    int max = DictionaryUtil.longestMatchAllNoun(text);
    
    if (max < 2) {
      return 0; // matches this short don't count
    }
    
    // TODO: try to clean this up
    if (max == text.length()-1 && hasSuffix) {
      boolean existPrv = false;
      if (prvText.length() >= 2) {
        existPrv = (DictionaryUtil.hasNoun(prvText.substring(prvText.length()-2)));
      }
      if (!existPrv && prvText.length() >= 3) {
        existPrv = (DictionaryUtil.hasNoun(prvText.substring(prvText.length()-3)));
      }
      if (!existPrv) {
        max++; // adjust for suffix
      }
    }
    
    return max;
  }
  
  private CompoundEntry[] analysisBySplited(int[] units, String input, boolean isFirst) {
    CompoundEntry[] entries = new CompoundEntry[units.length];
    
    int pos = 0;
    String prev = null;
    
    for (int i = 0; i < units.length; i++) {
      String str = input.substring(pos, pos + units[i]);

      if (i != 0 && !validCompound(prev, str, isFirst && (i==1), i)) {
        return null;
      }
      
      entries[i] = analyzeSingle(str); // CompoundEntry 로 변환

      pos += units[i];
      prev = str;
    }
    
    return entries;
  }
  
  /**
   * 입력된 String 을 CompoundEntry 로 변환
   * @param input input
   * @return compound entry
   */
  private CompoundEntry analyzeSingle(String input) {
    if (input.length() == 1) {
      return new CompoundEntry(input, true);
    } else {
      return new CompoundEntry(input, DictionaryUtil.hasWordExceptVerb(input));
    }
  }
  
  private static boolean isAlphaNumeric(String text) {
    
    for(int i=0;i<text.length();i++)
    {
      int c = text.charAt(i);
      if((c>=48 && c<=57) || (c>=65 && c<=90) || (c>=97 && c<=122)) {
        continue;
      }
      return false;
    }   
    return true;
  }
  
  private boolean validCompound(String before, String after, boolean isFirst, int pos) {

    if (pos == 1 && before.length() == 1 &&
        (!isFirst || !(existPrefix(before.charAt(0)) || isAlphaNumeric(before)))) {
      return false;    
    }

    if (after.length() == 1 && !isFirst && !existSuffix(after.charAt(0))) return false;

    if(pos!=1&&before.length()==1) {
      if (DictionaryUtil.isUncompound(before, after)) {
        return false;
      }
    }
    
    if (after.length() != 1) {
      if (DictionaryUtil.isUncompound("*", after)) {
        return false;
      }
    }
    
    return true;
  }
  
  private static boolean existPrefix(char ch) {
    switch (ch) {
      case '최':  case '고':  case '남':  case '여':  case '비':  
      case '유':  case '무':  case '군':  case '각':  case '기': 
        return true;
      default: return false;
    }
  }
  
  private static boolean existSuffix(char ch) {
    switch (ch) {
      case '각': case '감': case '값': case '객': case '계': case '길': case '고': case '공':
      case '관': case '국': case '권': case '금': case '급': case '기': case '내': case '난':
      case '단': case '대': case '땅': case '량': case '록': case '론': case '력': case '령':
      case '료': case '류': case '률': case '말': case '망': case '맵': case '문': case '물':
      case '면': case '밤': case '방': case '법': case '부': case '분': case '병': case '비':
      case '사': case '생': case '서': case '세': case '선': case '성': case '시': case '식':
      case '심': case '실': case '쇼': case '수': case '속': case '안': case '어': case '액':
      case '염': case '율': case '원': case '용': case '음': case '인': case '일': case '위':
      case '자': case '장': case '족': case '제': case '증': case '주': case '중': case '직':
      case '진': case '집': case '적': case '전': case '점': case '죄': case '컴': case '폭':
      case '품': case '표': case '판': case '팀': case '차': case '창': case '책': case '청':
      case '철': case '체': case '층': case '학': case '항': case '화': case '형': case '회':
        return true;
      default: return false;
    }
  }
}

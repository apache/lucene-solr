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

    boolean hasSuffix = isFirst && DictionaryUtil.existSuffix(input.substring(len-1));        
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
       
    for(int i=text.length();i>1;i--) {
      String seg = text.substring(0,i);
      WordEntry entry = DictionaryUtil.getAllNoun(seg);
      if(entry==null) continue;
      
      if (i == text.length()-1 && hasSuffix) {
        // if previous text exist in the dictionary.
        boolean existPrv = false;
        if(prvText.length()>=2) 
          existPrv = (DictionaryUtil.getNoun(prvText.substring(prvText.length()-2))!=null);
        if(!existPrv&&prvText.length()>=3)
          existPrv = (DictionaryUtil.getNoun(prvText.substring(prvText.length()-3))!=null);
        return existPrv ? i : i+1;
      } else {
        return i;
      }
    }   
    
    return 0;
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
    }
    
    WordEntry entry = DictionaryUtil.getWordExceptVerb(input);

    return new CompoundEntry(input, entry != null);
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

    if(pos==1&&before.length()==1&&
        (!isFirst||!(DictionaryUtil.existPrefix(before) || isAlphaNumeric(before)))) return false;    

    if(after.length()==1&&!isFirst&&!DictionaryUtil.existSuffix(after)) return false;

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
}

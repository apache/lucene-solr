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
import java.util.Arrays;
import java.util.List;

import org.apache.lucene.analysis.ko.dic.CompoundEntry;
import org.apache.lucene.analysis.ko.dic.DictionaryUtil;
import org.apache.lucene.analysis.ko.dic.WordEntry;

/**
 * 복합명사를 분해한다.
 */
public class CompoundNounAnalyzer {
  
  private boolean exactMach  = true;
    
  public boolean isExactMach() {
    return exactMach;
  }

  public void setExactMach(boolean exactMach) {
    this.exactMach = exactMach;
  }

  public List<CompoundEntry> analyze(String input) {
    
    WordEntry entry = DictionaryUtil.getAllNoun(input);
    if(entry!=null && entry.isCompoundNoun()) 
      return entry.getCompounds();
    
    return analyze(input,true);
    
  }
  
  public List<CompoundEntry> analyze(String input, boolean isFirst) {
    
    int len = input.length();
    if(len<3) return new ArrayList<CompoundEntry>(); 
    
    List<CompoundEntry> outputs = new ArrayList<CompoundEntry>();
    
    analyze(input, outputs, isFirst);

    return outputs;
    
  }
    
  public boolean analyze(String input, List<CompoundEntry> outputs, boolean isFirst) {
    
    int len = input.length();
    boolean success = false;
    
    switch(len) {
      case  3 :
        success = analyze3Word(input,outputs,isFirst);
        break;
      case  4 :
        success = analyze4Word(input,outputs,isFirst);
        break;  
      case  5 :
        success = analyze5Word(input,outputs,isFirst);
        break;
//      case  6 :
//        analyze6Word(input,outputs,isFirst);
//        break;  
      default :
        success = analyzeLongText(input,outputs,isFirst);       
    }
    
    return success;
  }
  
  private boolean analyze3Word(String input,List<CompoundEntry> outputs, boolean isFirst) {

    int[] units1 = {2,1};
    CompoundEntry[] entries1 = analysisBySplited(units1,input,isFirst);
    if(entries1!=null && entries1[0].isExist()&&entries1[1].isExist()) {
      outputs.addAll(Arrays.asList(entries1));
      return true;    
    }

    int[] units2 = {1,2};
    CompoundEntry[] entries2 = analysisBySplited(units2,input,isFirst);
    if(entries2!=null && entries2[0].isExist()&&entries2[1].isExist()) {
      outputs.addAll(Arrays.asList(entries2));
      return true;
    }
          
    return false;
  } 
  
  private boolean analyze4Word(String input,List<CompoundEntry> outputs, boolean isFirst) {
  
    if(!isFirst) {
      int[] units0 = {1,3};
      CompoundEntry[] entries0 = analysisBySplited(units0,input,isFirst);
      if(entries0!=null && entries0[0].isExist()&&entries0[1].isExist()) {
        outputs.addAll(Arrays.asList(entries0));
        return true;    
      }
    }

    int[] units3 = {3,1};
    CompoundEntry[] entries3 = analysisBySplited(units3,input,isFirst);
    if(entries3!=null && entries3[0].isExist()&&entries3[1].isExist()) {
      outputs.addAll(Arrays.asList(entries3));    
      return true;    
    }
    
    int[] units1 = {2,2};
    CompoundEntry[] entries1 = analysisBySplited(units1,input,isFirst);
    if(entries1!=null && entries1[0].isExist()&&entries1[1].isExist()) {
      outputs.addAll(Arrays.asList(entries1));    
      return true;    
    }
    
    int[] units2 = {1,2,1};
    CompoundEntry[] entries2 = analysisBySplited(units2,input,isFirst); 
    if(entries2!=null && entries2[0].isExist()&&entries2[1].isExist()&&entries2[2].isExist()) {
      outputs.addAll(Arrays.asList(entries2));  
      return true;
    }


    if(!exactMach&&entries1!=null && (entries1[0].isExist()||entries1[1].isExist())) {
      outputs.addAll(Arrays.asList(entries1));  
      return true;
    }
    
    return false;
  }
  
  private boolean analyze5Word(String input,List<CompoundEntry> outputs, boolean isFirst) {
      
    int[] units1 = {2,3};
    CompoundEntry[] entries1 = analysisBySplited(units1,input,isFirst);
    if(entries1!=null && entries1[0].isExist()&&entries1[1].isExist()) {
      outputs.addAll(Arrays.asList(entries1));
      return true;    
    }
    
    int[] units2 = {3,2};
    CompoundEntry[] entries2 = analysisBySplited(units2,input,isFirst);
    if(entries2!=null && entries2[0].isExist()&&entries2[1].isExist()) {
      outputs.addAll(Arrays.asList(entries2));
      return true;    
    }
    
    int[] units_1 = {4,1};
    CompoundEntry[] entries_1 = analysisBySplited(units_1,input,isFirst);
    if(entries_1!=null && entries_1[0].isExist()&&entries_1[1].isExist()) {     
      outputs.addAll(Arrays.asList(entries_1));
      return true;    
    }
    
    int[] units3 = {2,2,1};
    CompoundEntry[] entries3 = analysisBySplited(units3,input,isFirst);
    if(entries3!=null && entries3[0].isExist()&&entries3[1].isExist()&&entries3[2].isExist()) {     
      outputs.addAll(Arrays.asList(entries3));
      return true;
    }
    
    int[] units4 = {2,1,2};
    CompoundEntry[] entries4 = analysisBySplited(units4,input,isFirst);
    if(entries4!=null && entries4[0].isExist()&&entries4[1].isExist()&&entries4[2].isExist()) {     
      outputs.addAll(Arrays.asList(entries4));
      return true;
    }
    
    if(!exactMach&&entries1!=null && (entries1[0].isExist()||entries1[1].isExist())) {
      outputs.addAll(Arrays.asList(entries1));  
      return true;
    }
    
    if(!exactMach&&entries2!=null && (entries2[0].isExist()||entries2[1].isExist())) {  
      outputs.addAll(Arrays.asList(entries2));  
      return true;
    }
    
    boolean is = false;
    if(!exactMach&&entries3!=null && (entries3[0].isExist()||entries3[1].isExist())) {
      outputs.addAll(Arrays.asList(entries3));
      is = true;
    } 
    
    if(!exactMach&&entries4!=null && (entries4[0].isExist()||entries4[2].isExist())) {
      outputs.addAll(Arrays.asList(entries4));
      is = true;
    }   
    
    return is;
  }
   
  private boolean analyzeLongText(String input,List<CompoundEntry> outputs, boolean isFirst) {
    
    int len = input.length();
    
    // ignore less than 3 letters or more than 20 letters.
    if(len>20) return false; 

    boolean hasSuffix = isFirst && DictionaryUtil.existSuffix(input.substring(len-1));        
    int pos = caculatePos(input, hasSuffix);
    if(pos<1) return false; // fail to search a valid word segment
    
    if(pos==input.length()) {     
      if(hasSuffix) {
        outputs.add(
            new CompoundEntry(input.substring(0,len-1), true));
        outputs.add(
            new CompoundEntry(input.substring(len-1), true));
      } else {
        outputs.add(
            new CompoundEntry(input, true));

      } 
      
      return true;
    }
    
    List<CompoundEntry> results = new ArrayList<CompoundEntry>();
        
    String prev = input.substring(0,pos);
    String rear = input.substring(pos);
    
    boolean pSucess = false;
    boolean rSuccess = false;
    WordEntry prvEntry = DictionaryUtil.getAllNoun(prev);
    if(prvEntry==null) {
      pSucess = analyze(prev, results, false);
      if(!pSucess) results.add(new CompoundEntry(prev, false));
    } else {
      pSucess = true;
      if(prvEntry.isCompoundNoun())
        results.addAll(prvEntry.getCompounds());
      else
        results.add(new CompoundEntry(prev, true));
    }
    
    WordEntry rearEntry = DictionaryUtil.getAllNoun(rear);
    if(rearEntry==null) {
      rSuccess = analyze(rear, results, false);
      if(!rSuccess) results.add(new CompoundEntry(rear, false));
    } else {
      rSuccess = true;
      if(rearEntry.isCompoundNoun())
        results.addAll(rearEntry.getCompounds());
      else
        results.add(new CompoundEntry(rear, true));
    }
    
    if(!pSucess&&!rSuccess) {
      return false;
    }
    
    outputs.addAll(results);
    
    return true;
  }
  
  /**
   * calculate the position at which the long input should be divided into two segments.
   * @param input the input string
   * @return  the position
   */
  private int caculatePos(String input, boolean hasSuffix) {
  
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
    
    int maxlen = 0;
    boolean existPrv = false;
    
    // if previous text exist in the dictionary.
    if(prvText.length()>=2) 
      existPrv = (DictionaryUtil.getNoun(prvText.substring(prvText.length()-2))!=null);
    if(!existPrv&&prvText.length()>=3)
      existPrv = (DictionaryUtil.getNoun(prvText.substring(prvText.length()-3))!=null);
    
    for(int i=text.length();i>1;i--) {
      
      String seg = text.substring(0,i);
      WordEntry entry = DictionaryUtil.getAllNoun(seg);
      if(entry==null) continue;
      
      int len = 0;
      if(i==text.length()-1 && hasSuffix && !existPrv)
        len = i+1;
      else
        len = i;
      
      if(len>maxlen) maxlen = len;
    }   
    
    return maxlen;
  }
  
  private CompoundEntry[] analysisBySplited(int[] units, String input, boolean isFirst) {
  
    CompoundEntry[] entries = new CompoundEntry[units.length];
    
    int pos = 0;
    String prev = null;
    
    for(int i=0;i<units.length;i++) {
      
      String str = input.substring(pos,pos+units[i]);

      if(i!=0&&!validCompound(prev,str,isFirst&&(i==1),i)) return null;
      
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
    if(input.length()==1) return  new CompoundEntry(input, true);
    
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

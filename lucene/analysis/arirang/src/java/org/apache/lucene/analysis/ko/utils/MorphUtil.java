package org.apache.lucene.analysis.ko.utils;

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

import org.apache.lucene.analysis.ko.dic.DictionaryUtil;
import org.apache.lucene.analysis.ko.morph.AnalysisOutput;
import org.apache.lucene.analysis.ko.morph.MorphException;
import org.apache.lucene.analysis.ko.morph.PatternConstants;
import org.apache.lucene.analysis.ko.morph.WordEntry;

public class MorphUtil {

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
  public static char[] decompose(char c) {
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
  
  public static char compound(int first, int middle, int last) {    
    return (char)(0xAC00 + first* JUNG_JONG + middle * JONGSEONG.length + last);
  }
  

  public static char makeChar(char ch, int mdl, int last) {    
    ch -= 0xAC00;    
    int first = ch/JUNG_JONG;     
    return compound(first,mdl,last);
  }
  
  public static char makeChar(char ch, int last) {
    ch -= 0xAC00;    
    int first = ch/JUNG_JONG;  
    ch = (char)(ch % JUNG_JONG);
    int middle = ch/JONGSEONG.length;
    
    return compound(first,middle,last);    
  }
  
  public static char replaceJongsung(char dest, char source) {
    source -= 0xAC00;    
    int last = source % JONGSEONG.length;
      
    return makeChar(dest,last);  
  }

  /**
   * 형태소 유형 출력을 위한 문자열을 생성한다.
   * @param word  word to be printed
   * @param type  the type of the input word
   */
  public static String buildTypeString(String word, char type) {
    StringBuffer sb = new StringBuffer();
    sb.append(word);
    sb.append("(");
    sb.append(type);
    sb.append(")");
    
    return sb.toString();
  }
  
  
  public static void buildPtnVM(AnalysisOutput output, List<AnalysisOutput> candidates) throws MorphException {
    
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
  
  /**
   * 용언 + '음/기' + '이' + 어미, 체언 + '에서/부터/에서부터' + '이' + 어미
   * @param output  the output text
   * @param candidates  the candidates
   * @throws MorphException throw exception
   */
  public static void buildPtnCM(AnalysisOutput output, List<AnalysisOutput> candidates) throws MorphException {
    
    char ch = output.getStem().charAt(output.getStem().length()-2);
    char[] jasos = MorphUtil.decompose(ch);
    if(jasos.length==3||ch=='기') {
      buildPtnVMCM(output,candidates);      
    } else {
      
    }
  }
  
  private static void buildPtnVMCM(AnalysisOutput output, List<AnalysisOutput> candidates) throws MorphException {
    String stem = output.getStem();
  
    output.setPatn(PatternConstants.PTN_VMCM);
    output.setPos(PatternConstants.POS_VERB);
    
    char ch = stem.charAt(stem.length()-2);
    char[] jasos = MorphUtil.decompose(ch);

    if(ch=='기') {
      output.addElist("기");
      output.addElist("이");
      output.setStem(stem.substring(0,stem.length()-2));
      
      if(DictionaryUtil.getVerb(output.getStem())!=null)
        candidates.add(output);
    }else if(jasos[2]=='ㅁ') {
      if(stem.length()>1) stem = stem.substring(0,stem.length()-2);
      stem += MorphUtil.makeChar(ch, 0);
      output.addElist("ㅁ");
      output.addElist("이");
      output.setStem(stem);

      if(DictionaryUtil.getVerb(stem)!=null) 
        candidates.add(output);
      else {
        String[] morphs = IrregularUtil.restoreIrregularVerb(stem,"ㅁ");
        if(morphs!=null) {
          output.setScore(AnalysisOutput.SCORE_CORRECT);
          output.setStem(morphs[0]);
          candidates.add(output);
        }
      }
    }
  }

  public static boolean hasVerbOnly(String input) throws MorphException {
    
    for(int i=input.length()-1;i>=0;i--) {
      char[] feature = SyllableUtil.getFeature(input.charAt(i));
      if(feature[SyllableUtil.IDX_WDSURF]=='1'&&input.length()>i) return true;
    }
    return false;
  }
  
  /**
   * 시제 선어미말을 만들어서 반환한다.
   * @param preword  '아' 또는 '어'
   * @param endword  어미[선어미말을 포함]
   * @return '았' 또는 '었'을 만들어서 반환한다.
   */
  public static String makeTesnseEomi(String preword, String endword) {

    if(preword==null||preword.length()==0) return endword;
    if(endword==null||endword.length()==0) return preword;

    if(endword.charAt(0)=='ㅆ') {
      return preword.substring(0,preword.length()-1)+
          makeChar(preword.charAt(preword.length()-1),20)+endword.substring(1,endword.length());    
    } else if(endword.charAt(0)=='ㄴ') {
      return preword.substring(0,preword.length()-1)+
          makeChar(preword.charAt(preword.length()-1),4)+endword.substring(1,endword.length());
    } else if(endword.charAt(0)=='ㄹ') {
      return preword.substring(0,preword.length()-1)+
          makeChar(preword.charAt(preword.length()-1),8)+endword.substring(1,endword.length());  
    } else if(endword.charAt(0)=='ㅁ') {
      return preword.substring(0,preword.length()-1)+
          makeChar(preword.charAt(preword.length()-1),16)+endword.substring(1,endword.length());          
    } else if(endword.charAt(0)=='ㅂ') {
      return preword.substring(0,preword.length()-1)+
          makeChar(preword.charAt(preword.length()-1),17)+endword.substring(1,endword.length());
    }
    return preword+endword;    
  }
  
  /**
   * 용언화접미사가 결합될 수 있는지 여부를 점검한다.
   * 특히 사전에 등록된 되다, 하다형 의 접속이 가능한지를 조사한다.
   */
  public static boolean isValidSuffix(WordEntry entry, AnalysisOutput o) {
    
    return true;
  }
}

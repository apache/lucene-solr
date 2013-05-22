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

import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.analysis.ko.morph.AnalysisOutput;
import org.apache.lucene.analysis.ko.morph.MorphException;
import org.apache.lucene.analysis.ko.morph.PatternConstants;

public class EomiUtil {
  
  public static final String RESULT_FAIL = "0";
  
  public static final String RESULT_SUCCESS = "1";
  
  public static final String[] verbSuffix = {
      "이","하","되","스럽","스러우","시키","있","없","같","당하","만하","드리","받","나","내"
  };
  
  /**
   * 가장 길이가 긴 어미를 분리한다.
   * @param term
   * @throws MorphException
   */
  public static String[] longestEomi(String term) throws MorphException  {
    
    String[] result = new String[2];
    result[0] = term;
    
    String stem;
    String eomi;
    char[] efeature;
    
    for(int i=term.length();i>0;i--) {
      
      stem = term.substring(0,i);      
    
      if(i!=term.length()) {
        eomi = term.substring(i);
        efeature  = SyllableUtil.getFeature(eomi.charAt(0));        
      } else {
        efeature = SyllableUtil.getFeature(stem.charAt(i-1));
        eomi="";
      }

      if(SyllableUtil.isAlpanumeric(stem.charAt(i-1))) break;
      
      char[] jasos = MorphUtil.decompose(stem.charAt(i-1));
  
      if(!"".equals(eomi)&&!DictionaryUtil.existEomi(eomi)) {
        // do not anything.
      } else if(jasos.length>2&&
          (jasos[2]=='ㄴ'||jasos[2]=='ㄹ'||jasos[2]=='ㅁ'||jasos[2]=='ㅂ')&&
          DictionaryUtil.combineAndEomiCheck(jasos[2], eomi)!=null) {
        result[0] = Character.toString(MorphUtil.makeChar(stem.charAt(i-1), 0));
        if(i!=0) result[0] = stem.substring(0,i-1)+result[0];
        result[1] = Character.toString(jasos[2]);
      }else if(i>0&&(stem.endsWith("하")&&"여".equals(eomi))||
          (stem.endsWith("가")&&"거라".equals(eomi))||
          (stem.endsWith("오")&&"너라".equals(eomi))) {
        result[0] = stem;
        result[1] = eomi;      
      }else if(jasos.length==2&&(!stem.endsWith("아")&&!stem.endsWith("어"))&&
          (jasos[1]=='ㅏ'||jasos[1]=='ㅓ'||jasos[1]=='ㅔ'||jasos[1]=='ㅐ')&&
          (DictionaryUtil.combineAndEomiCheck('어', eomi)!=null)) {    
        char[] chs = MorphUtil.decompose(stem.charAt(stem.length()-1));        
        result[0] = stem;
        result[1] = "어"+eomi;
      }else if((jasos[1]=='ㅘ'||jasos[1]=='ㅝ'||jasos[1]=='ㅕ'||jasos[1]=='ㅐ'||jasos[1]=='ㅒ')&&
          (DictionaryUtil.combineAndEomiCheck('어', eomi)!=null)) {        
        String end = "";        
        if(jasos[1]=='ㅘ')
          end=MorphUtil.makeChar(stem.charAt(i-1), 8, 0)+"아";  
        else if(jasos[1]=='ㅝ')
          end=MorphUtil.makeChar(stem.charAt(i-1), 13, 0)+"어";  
        else if(jasos[1]=='ㅕ')
          end=Character.toString(MorphUtil.makeChar(stem.charAt(i-1), 6, 0));
        else if(jasos[1]=='ㅐ')
          end=MorphUtil.makeChar(stem.charAt(i-1), 0, 0)+"어";  
        else if(jasos[1]=='ㅒ')
          end=MorphUtil.makeChar(stem.charAt(i-1), 20, 0)+"애";                    
        
        if(jasos.length==3) {          
          end = end.substring(0,end.length()-1)+MorphUtil.replaceJongsung(end.charAt(end.length()-1),stem.charAt(i-1));
        }
        
        if(stem.length()<2) result[0] = end;
        else result[0] = stem.substring(0,stem.length()-1)+end;
        result[1] = eomi;  
        
      }else if(efeature!=null&&efeature[SyllableUtil.IDX_EOMI1]!='0'&&
          DictionaryUtil.existEomi(eomi)) {
        if(!(((jasos.length==2&&jasos[0]=='ㄹ')||(jasos.length==3&&jasos[2]=='ㄹ'))&&eomi.equals("러"))) { // ㄹ 불규칙은 예외
          result[0] = stem;
          result[1] = eomi;
        }
      }

      if(efeature!=null&&efeature[SyllableUtil.IDX_EOMI2]=='0') break;
    }  

    return result;
    
  }  
  
  /**
   * 선어말어미를 분석한다.
   */
  public static String[] splitPomi(String stem) throws MorphException  {

    //   results[0]:성공(1)/실패(0), results[1]: 어근, results[2]: 선어말어미
    String[] results = new String[2];  
    results[0] = stem;

    if(stem==null||stem.length()==0||"있".equals(stem)) return results;
  
    char[] chrs = stem.toCharArray();
    int len = chrs.length;
    String pomi = "";
    int index = len-1;
  
    char[] jaso = MorphUtil.decompose(chrs[index]);
    if(chrs[index]!='시'&&chrs[index]!='ㅆ'&&jaso[jaso.length-1]!='ㅆ') return results;  // 선어말어미가 발견되지 않았다
    
    if(chrs[index]=='겠') {
      pomi = "겠";
      setPomiResult(results,stem.substring(0,index),pomi);    
      if(--index<=0||
          (chrs[index]!='시'&&chrs[index]!='ㅆ'&&jaso[jaso.length-1]!='ㅆ')) 
        return results; // 다음이거나 선어말어미가 없다면...
      jaso = MorphUtil.decompose(chrs[index]);
    }

    if(chrs[index]=='었') { // 시었, ㅆ었, 었
      pomi = chrs[index]+pomi;  
      setPomiResult(results,stem.substring(0,index),pomi);    
      if(--index<=0||
          (chrs[index]!='시'&&chrs[index]!='ㅆ'&&jaso[jaso.length-1]!='ㅆ')) 
        return results; // 다음이거나 선어말어미가 없다면...        
      jaso = MorphUtil.decompose(chrs[index]);
    }

    if(chrs[index]=='였'){
      pomi = MorphUtil.replaceJongsung('어',chrs[index])+pomi;  
      if(index>0&&chrs[index-1]=='하') 
        stem = stem.substring(0,index);  
      else
        stem = stem.substring(0,index)+"이";
      setPomiResult(results,stem,pomi);  
    }else if(chrs[index]=='셨'){
      pomi = MorphUtil.replaceJongsung('어',chrs[index])+pomi;  
      stem = stem.substring(0,index);    
      setPomiResult(results,stem,"시"+pomi);        
    }else if(chrs[index]=='았'||chrs[index]=='었') {
      pomi = chrs[index]+pomi;  
      setPomiResult(results,stem.substring(0,index),pomi);    
      if(--index<=0||
          (chrs[index]!='시'&&chrs[index]!='으')) return results; // 다음이거나 선어말어미가 없다면...        
      jaso = MorphUtil.decompose(chrs[index]);    
    }else if(jaso.length==3&&jaso[2]=='ㅆ') {
    
      if(jaso[0]=='ㅎ'&&jaso[1]=='ㅐ') {       
        pomi = MorphUtil.replaceJongsung('어',chrs[index])+pomi;  
        stem = stem.substring(0,index)+"하";  
      }else if(jaso[0]!='ㅇ'&&(jaso[1]=='ㅏ'||jaso[1]=='ㅓ'||jaso[1]=='ㅔ'||jaso[1]=='ㅐ')) {    
        pomi = "었"+pomi;
        stem = stem.substring(0,index)+MorphUtil.makeChar(chrs[index], 0);        
      }else if(jaso[0]!='ㅇ'&&(jaso[1]=='ㅙ')) {
        pomi = "었"+pomi;
        stem = stem.substring(0,index)+MorphUtil.makeChar(chrs[index],11, 0);        
      } else if(jaso[1]=='ㅘ') {      
        pomi = MorphUtil.replaceJongsung('아',chrs[index])+pomi;  
        stem = stem.substring(0,index)+MorphUtil.makeChar(chrs[index],8, 0);
      } else if(jaso[1]=='ㅝ') {
        pomi = MorphUtil.replaceJongsung('어',chrs[index])+pomi;  
        stem = stem.substring(0,index)+MorphUtil.makeChar(chrs[index],13, 0);
      } else if(jaso[1]=='ㅕ') {          
        pomi = MorphUtil.replaceJongsung('어',chrs[index])+pomi;        
        stem = stem.substring(0,index)+MorphUtil.makeChar(chrs[index],20, 0);          
      } else if(jaso[1]=='ㅐ') {
        pomi = MorphUtil.replaceJongsung('어',chrs[index])+pomi;
        stem = stem.substring(0,index);
      } else if(jaso[1]=='ㅒ') {
        pomi = MorphUtil.replaceJongsung('애',chrs[index])+pomi;  
        stem = stem.substring(0,index);
      } else {
        pomi = "었"+pomi;
      }
      setPomiResult(results,stem,pomi);        
      if(chrs[index]!='시'&&chrs[index]!='으') return results; // 다음이거나 선어말어미가 없다면...        
      jaso = MorphUtil.decompose(chrs[index]);        
    }

    char[] nChrs = null;
    if(index>0) nChrs = MorphUtil.decompose(chrs[index-1]);
    else nChrs = new char[2];

    if(nChrs.length==2&&chrs[index]=='시'&&(chrs.length<=index+1||
        (chrs.length>index+1&&chrs[index+1]!='셨'))) {
      if(DictionaryUtil.getWord(results[0])!=null) return results;  //'시'가 포함된 단어가 있다. 성가시다/도시다/들쑤시다 
      pomi = chrs[index]+pomi;  
      setPomiResult(results,stem.substring(0,index),pomi);      
      if(--index==0||chrs[index]!='으') return results; // 다음이거나 선어말어미가 없다면...        
      jaso = MorphUtil.decompose(chrs[index]);
    }
    
    if(index>0) nChrs = MorphUtil.decompose(chrs[index-1]);
    else nChrs = new char[2];
    if(chrs.length>index+1&&nChrs.length==3&&(chrs[index+1]=='셨'||chrs[index+1]=='시')&&chrs[index]=='으') {
      pomi = chrs[index]+pomi;  
      setPomiResult(results,stem.substring(0,index),pomi);    
    }
  
    return results;
  }
  
  /**
   * 불규칙 용언의 원형을 구한다.
   */
  public static List<AnalysisOutput> irregular(AnalysisOutput output) throws MorphException {
    
    List<AnalysisOutput> results = new ArrayList<AnalysisOutput>();
  
    if(output.getStem()==null||output.getStem().length()==0) 
      return results;    
    
    String ending = output.getEomi();
    if(output.getPomi()!=null) ending = output.getPomi();
    
    List<String[]> irrs = new ArrayList<String[]>();
    
    irregularStem(irrs,output.getStem(),ending);
    irregularEnding(irrs,output.getStem(),ending);
    irregularAO(irrs,output.getStem(),ending);

    try {
      for(String[] irr: irrs) {
        AnalysisOutput result = output.clone();
        result.setStem(irr[0]);
        if(output.getPatn()==PatternConstants.PTN_VM) {
          if(output.getPomi()==null) result.setEomi(irr[1]);
          else result.setPomi(irr[1]);
        }  
        results.add(result);
      }        
    } catch (CloneNotSupportedException e) {
      throw new MorphException(e.getMessage(),e);
    }
        
    return results;
    
  }
  
  /**
   * 어간만 변하는 경우
   * @param results
   * @param stem
   * @param ending
   */
  private static void irregularStem(List<String[]> results, String stem, String ending) {  

    char feCh = ending.charAt(0);
    char[] fechJaso =  MorphUtil.decompose(feCh);
    char ls = stem.charAt(stem.length()-1);
    char[] lsJaso = MorphUtil.decompose(ls);
  
    if(feCh=='아'||feCh=='어'||feCh=='으') {
      if(lsJaso[lsJaso.length-1]=='ㄹ') { // ㄷ 불규칙
        results.add(
            new String[]{stem.substring(0,stem.length()-1)+
                MorphUtil.makeChar(stem.charAt(stem.length()-1),7)
                ,ending
                ,String.valueOf(PatternConstants.IRR_TYPE_DI)});
      } else if(lsJaso.length==2) { // ㅅ 불규칙
        results.add(
            new String[]{stem.substring(0,stem.length()-1)+
                MorphUtil.makeChar(stem.charAt(stem.length()-1),19)
                ,ending
                ,String.valueOf(PatternConstants.IRR_TYPE_SI)});        
      }      
    }
    
    if((fechJaso[0]=='ㄴ'||fechJaso[0]=='ㄹ'||fechJaso[0]=='ㅁ'||  feCh=='오'||feCh=='시')
        &&(ls=='우')) { // ㅂ 불규칙
      results.add(
          new String[]{stem.substring(0,stem.length()-1)+
              MorphUtil.makeChar(stem.charAt(stem.length()-1),17)
              ,ending
              ,String.valueOf(PatternConstants.IRR_TYPE_BI)});        
    }
    
    if((fechJaso[0]=='ㄴ'||fechJaso[0]=='ㅂ'||fechJaso[0]=='ㅅ'||  feCh=='오')
        &&(lsJaso.length==2)) { // ㄹ 탈락

      results.add(
          new String[]{stem.substring(0,stem.length()-1)+
              MorphUtil.makeChar(stem.charAt(stem.length()-1),8)
              ,ending
              ,String.valueOf(PatternConstants.IRR_TYPE_LI)});      
    }
    
    if(lsJaso.length==2
        &&(fechJaso[0]=='ㄴ'||fechJaso[0]=='ㄹ'||fechJaso[0]=='ㅁ'||fechJaso[0]=='ㅂ'||
        lsJaso[1]=='ㅏ'||lsJaso[1]=='ㅓ'||lsJaso[1]=='ㅑ'||lsJaso[1]=='ㅕ')
        &&!"나".equals(stem)) { // ㅎ 불규칙, 그러나 [낳다]는 ㅎ 불규칙이 아니다.
      results.add(
          new String[]{stem.substring(0,stem.length()-1)+
              MorphUtil.makeChar(stem.charAt(stem.length()-1),27)
              ,ending
              ,String.valueOf(PatternConstants.IRR_TYPE_HI)});      
    }    
  }
  
  /**
   * 어미만 변하는 경우
   * @param results
   * @param stem
   * @param ending
   */
  private static void irregularEnding(List<String[]> results, String stem, String ending) {
    if(ending.startsWith("ㅆ")) return;
    
    char feCh = ending.charAt(0);
    char ls = stem.charAt(stem.length()-1);

    if(feCh=='러'&&ls=='르') { // '러' 불규칙
      results.add(
          new String[]{stem
              ,"어"+ending.substring(1)
              ,String.valueOf(PatternConstants.IRR_TYPE_RO)});        
    } else if("라".equals(ending)&&"가거".equals(stem)) { // '거라' 불규칙
      results.add( 
          new String[]{stem.substring(0,stem.length()-1)
              ,"어라"
              ,String.valueOf(PatternConstants.IRR_TYPE_GU)});              
    } else if("라".equals(ending)&&"오너".equals(stem)) { // '너라' 불규칙
      results.add(
          new String[]{stem.substring(0,stem.length()-1)
              ,"어라"
              ,String.valueOf(PatternConstants.IRR_TYPE_NU)});      
    }
    
    if("여".equals(ending)&&ls=='하') { // '여' 불규칙
      results.add(
          new String[]{stem
              ,"어"
              ,String.valueOf(PatternConstants.IRR_TYPE_NU)});        
    }
  }
  
  /**
   * 어간과 어미가 모두 변하는 경우
   * @param results
   * @param stem
   * @param ending
   */
  private static void irregularAO(List<String[]> results, String stem, String ending) {
    
    char ls = stem.charAt(stem.length()-1);
    char[] lsJaso = MorphUtil.decompose(ls);
    
    if(lsJaso.length<2) return;
    
    if(lsJaso[1]=='ㅘ') {
      if(stem.endsWith("도와")||stem.endsWith("고와")) { // '곱다', '돕다'의 'ㅂ' 불규칙
        results.add(
            new String[]{stem.substring(0,stem.length()-2)+
                MorphUtil.makeChar(stem.charAt(stem.length()-2),17) // + 'ㅂ'
                ,makeTesnseEomi("아",ending)
                ,String.valueOf(PatternConstants.IRR_TYPE_BI)});          
      }else { // '와' 축약
        results.add(
            new String[]{stem.substring(0,stem.length()-1)+
                MorphUtil.makeChar(stem.charAt(stem.length()-1),8,0) // 자음 + ㅗ 
                ,makeTesnseEomi("아",ending)
                ,String.valueOf(PatternConstants.IRR_TYPE_WA)});        
      }
    } else if(stem.endsWith("퍼")) {
      results.add(
          new String[]{stem.substring(0,stem.length()-1)+
              MorphUtil.makeChar(stem.charAt(stem.length()-1),18,0) // 자음 + - 
              ,makeTesnseEomi("어",ending)
              ,String.valueOf(PatternConstants.IRR_TYPE_WA)});  
    } else if(lsJaso[1]=='ㅝ') {
      if(stem.length()>=2) // 'ㅂ' 불규칙
        results.add(
            new String[]{stem.substring(0,stem.length()-2)+
                MorphUtil.makeChar(stem.charAt(stem.length()-2),17) // + 'ㅂ'
                ,makeTesnseEomi("어",ending)
                ,String.valueOf(PatternConstants.IRR_TYPE_BI)});  

      results.add(
          new String[]{stem.substring(0,stem.length()-1)+
              MorphUtil.makeChar(stem.charAt(stem.length()-1),13,0) // 자음 + ㅗ 
              ,makeTesnseEomi("어",ending)
              ,String.valueOf(PatternConstants.IRR_TYPE_WA)});  
    } else if(stem.length()>=2&&ls=='라') {
      char[] ns = MorphUtil.decompose(stem.charAt(stem.length()-2));
      if(ns.length==3&&ns[2]=='ㄹ') { // 르 불규칙
        results.add(
            new String[]{stem.substring(0,stem.length()-2)+
                MorphUtil.makeChar(stem.charAt(stem.length()-2),0) + "르"
                ,makeTesnseEomi("아",ending)
                ,String.valueOf(PatternConstants.IRR_TYPE_RO)});          
      }      
    } else if(stem.length()>=2&&ls=='러') {
      char[] ns = MorphUtil.decompose(stem.charAt(stem.length()-2));
      if(stem.charAt(stem.length()-2)=='르') { // 러 불규칙
        results.add(
            new String[]{stem.substring(0,stem.length()-1)
                ,makeTesnseEomi("어",ending)
                ,String.valueOf(PatternConstants.IRR_TYPE_LO)});  
      } else if(ns.length==3&&ns[2]=='ㄹ') { // 르 불규칙
        results.add(
            new String[]{stem.substring(0,stem.length()-2)+
                MorphUtil.makeChar(stem.charAt(stem.length()-2),0) + "르"
                ,makeTesnseEomi("어",ending)
                ,String.valueOf(PatternConstants.IRR_TYPE_RO)});  
      }
    } else if(stem.endsWith("펴")||stem.endsWith("켜")) {
      results.add(
          new String[]{stem.substring(0,stem.length()-1)+
              MorphUtil.makeChar(stem.charAt(stem.length()-1),20,0)
              ,makeTesnseEomi("어",ending)
              ,String.valueOf(PatternConstants.IRR_TYPE_EI)});  
    } else if(stem.endsWith("해")) {
      results.add(
          new String[]{stem.substring(0,stem.length()-1)+
              MorphUtil.makeChar(stem.charAt(stem.length()-1),0,0)
              ,makeTesnseEomi("어",ending)
              ,String.valueOf(PatternConstants.IRR_TYPE_EI)});        
    } else if(lsJaso.length==2&&lsJaso[1]=='ㅏ') {
      results.add(
          new String[]{stem.substring(0,stem.length()-1)+
              MorphUtil.makeChar(stem.charAt(stem.length()-1),18,0)
              ,makeTesnseEomi("어",ending)
              ,String.valueOf(PatternConstants.IRR_TYPE_UO)});  
    } else if(lsJaso.length==2&&lsJaso[1]=='ㅓ') {
      // 으 탈락
      results.add(
          new String[]{stem.substring(0,stem.length()-1)+
              MorphUtil.makeChar(stem.charAt(stem.length()-1),18,0)
              ,makeTesnseEomi("어",ending)
              ,String.valueOf(PatternConstants.IRR_TYPE_UO)});  
      //   아 불규칙
      results.add(
          new String[]{stem
              ,makeTesnseEomi("어",ending)
              ,String.valueOf(PatternConstants.IRR_TYPE_AH)});  
    } else if(lsJaso[1]=='ㅕ') {
      results.add(
          new String[]{stem.substring(0,stem.length()-1)+
              MorphUtil.makeChar(stem.charAt(stem.length()-1),20,0)
              ,makeTesnseEomi("어",ending)
              ,String.valueOf(PatternConstants.IRR_TYPE_EI)});  
    } else if(lsJaso[1]=='ㅙ') {
      results.add(
          new String[]{stem.substring(0,stem.length()-1)+
              MorphUtil.makeChar(stem.charAt(stem.length()-1),11,0)
              ,makeTesnseEomi("어",ending)
              ,String.valueOf(PatternConstants.IRR_TYPE_OE)});  
    } else if(lsJaso[1]=='ㅐ') {
      results.add(
          new String[]{stem.substring(0,stem.length()-1)+
              MorphUtil.makeChar(stem.charAt(stem.length()-1),0,27)
              ,makeTesnseEomi("아",ending)
              ,String.valueOf(PatternConstants.IRR_TYPE_HI)});
    } else if(lsJaso[1]=='ㅒ') {
      results.add(
          new String[]{stem.substring(0,stem.length()-1)+
              MorphUtil.makeChar(stem.charAt(stem.length()-1),2,27)
              ,makeTesnseEomi("아",ending)
              ,String.valueOf(PatternConstants.IRR_TYPE_HI)});              
    }
  }
  
  /**
   * 시제 선어미말을 만들어서 반환한다.
   * @param preword  '아' 또는 '어'
   * @param endword  어미[선어미말을 포함]
   * return '았' 또는 '었'을 만들어서 반환한다.
   */
  public static String makeTesnseEomi(String preword, String endword) {

    if(preword==null||preword.length()==0) return endword;
    if(endword==null||endword.length()==0) return preword;

    if(endword.charAt(0)=='ㅆ') {
      return preword.substring(0,preword.length()-1)+
          MorphUtil.makeChar(preword.charAt(preword.length()-1),20)+endword.substring(1,endword.length());    
    } else if(endword.charAt(0)=='ㄴ') {
      return preword.substring(0,preword.length()-1)+
          MorphUtil.makeChar(preword.charAt(preword.length()-1),4)+endword.substring(1,endword.length());
    } else if(endword.charAt(0)=='ㄹ') {
      return preword.substring(0,preword.length()-1)+
          MorphUtil.makeChar(preword.charAt(preword.length()-1),8)+endword.substring(1,endword.length());  
    } else if(endword.charAt(0)=='ㅁ') {
      return preword.substring(0,preword.length()-1)+
          MorphUtil.makeChar(preword.charAt(preword.length()-1),16)+endword.substring(1,endword.length());          
    } else if(endword.charAt(0)=='ㅂ') {
      return preword.substring(0,preword.length()-1)+
          MorphUtil.makeChar(preword.charAt(preword.length()-1),17)+endword.substring(1,endword.length());
    }
    return preword+endword;    
  }
 
   /**
    * '음/기' + '이' + 어미, '에서/부터/에서부터' + '이' + 어미 인지 조사한다.
    */
   public static boolean endsWithEEomi(String stem) {
     int len = stem.length();
     if(len<2||!stem.endsWith("이")) return false;
    
     char[] jasos = MorphUtil.decompose(stem.charAt(len-2));
     if(jasos.length==3&&jasos[2]=='ㅁ')
       return true;
     else {
       int index = stem.lastIndexOf("기");
       if(index==-1) index = stem.lastIndexOf("에서");
       if(index==-1) index = stem.lastIndexOf("부터");
       if(index==-1) return false;
       return true;
     }
   }
   
  private static void setPomiResult(String[] results,String stem, String pomi ) {
    results[0] = stem;
    results[1] = pomi;
  }  
  
  public static boolean IsNLMBSyl(char ech, char lch) throws MorphException {
  
    char[] features = SyllableUtil.getFeature(ech);

    switch(lch) {

      case 'ㄴ' :
        return (features[SyllableUtil.IDX_YNPNA]=='1' || features[SyllableUtil.IDX_YNPLN]=='1');        
      case 'ㄹ' :
        return (features[SyllableUtil.IDX_YNPLA]=='1');
      case 'ㅁ' :
        return (features[SyllableUtil.IDX_YNPMA]=='1');    
      case 'ㅂ' :
        return (features[SyllableUtil.IDX_YNPBA]=='1');          
    }
  
    return false;
  }
  
  /**
   * 어미를 분리한다.
   * 
   * 1. 규칙용언과 어간만 바뀌는 불규칙 용언
   * 2. 어미가 종성 'ㄴ/ㄹ/ㅁ/ㅂ'으로 시작되는 어절
   * 3. '여/거라/너라'의 불규칙 어절
   * 4. 어미 '아/어'가 탈락되는 어절
   * 5. '아/어'의 변이체 분리
   */
  public static String[] splitEomi(String stem, String end) throws MorphException {

    String[] strs = new String[2];
    int strlen = stem.length();
    if(strlen==0) return strs;

    char estem = stem.charAt(strlen-1);
    char[] chrs = MorphUtil.decompose(estem);
    if(chrs.length==1) return strs; // 한글이 아니라면...

    if((chrs.length==3)&&(chrs[2]=='ㄴ'||chrs[2]=='ㄹ'||chrs[2]=='ㅁ'||chrs[2]=='ㅂ')&&
        EomiUtil.IsNLMBSyl(estem,chrs[2])&&
        DictionaryUtil.combineAndEomiCheck(chrs[2], end)!=null) {    
      strs[1] = Character.toString(chrs[2]);
      if(end.length()>0) strs[1] += end;
      strs[0] = stem.substring(0,strlen-1) + MorphUtil.makeChar(estem, 0);  
    } else if(estem=='해'&&DictionaryUtil.existEomi("어"+end)) {      
      strs[0] = stem.substring(0,strlen-1)+"하";
      strs[1] = "어"+end;  
    } else if(estem=='히'&&DictionaryUtil.existEomi("이"+end)) {      
      strs[0] = stem.substring(0,strlen-1)+"하";
      strs[1] = "이"+end;        
    } else if(chrs[0]!='ㅇ'&&
        (chrs[1]=='ㅏ'||chrs[1]=='ㅓ'||chrs[1]=='ㅔ'||chrs[1]=='ㅐ')&&
        (chrs.length==2 || SyllableUtil.getFeature(estem)[SyllableUtil.IDX_YNPAH]=='1')&&
        (DictionaryUtil.combineAndEomiCheck('어', end)!=null)) {    
    
      strs[0] = stem;
      if(chrs.length==2) strs[1] = "어"+end;  
      else strs[1] = end;  
    } else if(stem.endsWith("하")&&"여".equals(end)) {      
      strs[0] = stem;
      strs[1] = "어";  
    }else if((chrs.length==2)&&(chrs[1]=='ㅘ'||chrs[1]=='ㅙ'||chrs[1]=='ㅝ'||chrs[1]=='ㅕ'||chrs[1]=='ㅐ'||chrs[1]=='ㅒ')&&
        (DictionaryUtil.combineAndEomiCheck('어', end)!=null)) {    
  
      StringBuffer sb = new StringBuffer();
      
      if(strlen>1) sb.append(stem.substring(0,strlen-1));
      
      if(chrs[1]=='ㅘ')
        sb.append(MorphUtil.makeChar(estem, 8, 0)).append(MorphUtil.replaceJongsung('아',estem));  
      else if(chrs[1]=='ㅝ')
        sb.append(MorphUtil.makeChar(estem, 13, 0)).append(MorphUtil.replaceJongsung('어',estem));  
      else if(chrs[1]=='ㅙ')
        sb.append(MorphUtil.makeChar(estem, 11, 0)).append(MorphUtil.replaceJongsung('어',estem));        
      else if(chrs[1]=='ㅕ')
        sb.append(Character.toString(MorphUtil.makeChar(estem, 20, 0))).append(MorphUtil.replaceJongsung('어',estem));
      else if(chrs[1]=='ㅐ')
        sb.append(MorphUtil.makeChar(estem, 0, 0)).append(MorphUtil.replaceJongsung('어',estem));
      else if(chrs[1]=='ㅒ')
        sb.append(MorphUtil.makeChar(estem, 20, 0)).append(MorphUtil.replaceJongsung('애',estem));  
    
      strs[0] = sb.toString();
    
      end = strs[0].substring(strs[0].length()-1)+end;        
      strs[0] = strs[0].substring(0,strs[0].length()-1);
      
      strs[1] = end;    

    }else if(!"".equals(end)&&DictionaryUtil.existEomi(end)) {    
      strs = new String[]{stem, end};
    }

    return strs;
  }
}

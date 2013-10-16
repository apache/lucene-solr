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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.analysis.ko.dic.DictionaryResources;
import org.apache.lucene.analysis.ko.morph.MorphException;

public class SyllableUtil {
  private SyllableUtil() {}

  public static int IDX_JOSA1 = 0; // 조사의 첫음절로 사용되는 음절 49개
  public static int IDX_JOSA2 = 1; // 조사의 두 번째 이상의 음절로 사용되는 음절 58개
  public static int IDX_EOMI1 = 2; // 어미의 첫음절로 사용되는 음절 72개
  public static int IDX_EOMI2 = 3; // 어미의 두 번째 이상의 음절로 사용되는 음절 105개
  public static int IDX_YONG1 = 4; // 1음절 용언에 사용되는 음절 362개
  public static int IDX_YONG2 = 5; // 2음절 용언의 마지막 음절로 사용되는 음절 316개
  public static int IDX_YONG3 = 6; // 3음절 이상 용언의 마지막 음절로 사용되는 음절 195개
  public static int IDX_CHEON1 = 7; // 1음절 체언에 사용되는 음절 680개
  public static int IDX_CHEON2 = 8; // 2음절 체언의 마지막 음절로 사용되는 음절 916개
  public static int IDX_CHEON3 = 9; // 3음절 체언의 마지막 음절로 사용되는 음절 800개
  public static int IDX_CHEON4 = 10; // 4음절 체언의 마지막 음절로 사용되는 음절 610개
  public static int IDX_CHEON5 = 11; // 5음절 이상 체언의 마지막 음절로 사용되는 음절 330개
  public static int IDX_BUSA1 = 12; // 1음절 부사의 마지막 음절로 사용되는 음절 191개
  public static int IDX_BUSA2 = 13; // 2음절 부사의 마지막 음절로 사용되는 음절 519개
  public static int IDX_BUSA3 = 14; // 3음절 부사의 마지막 음절로 사용되는 음절 139개
  public static int IDX_BUSA4 = 15; // 4음절 부사의 마지막 음절로 사용되는 음절 366개
  public static int IDX_BUSA5 = 16; // 5음절 부사의 마지막 음절로 사용되는 음절 79개
  public static int IDX_PRONOUN = 17; // 대명사의 마지막 음절로 사용되는 음절 77개
  public static int IDX_EXCLAM = 18; // 관형사와 감탄사의 마지막 음절로 사용되는 음절 241개
  
  public static int IDX_YNPNA = 19; // (용언+'-ㄴ')에 의하여 생성되는 음절 129개
  public static int IDX_YNPLA = 20; // (용언+'-ㄹ')에 의해 생성되는 음절 129개
  public static int IDX_YNPMA = 21; // (용언+'-ㅁ')에 의해 생성되는 음절 129개
  public static int IDX_YNPBA = 22; // (용언+'-ㅂ')에 의해 생성되는 음절 129개
  public static int IDX_YNPAH = 23; // 모음으로 끝나는 음절 129개중 'ㅏ/ㅓ/ㅐ/ㅔ/ㅕ'로 끝나는 것이 선어말 어미 '-었-'과 결합할 때 생성되는 음절
  public static int IDX_YNPOU = 24; // 모음 'ㅗ/ㅜ'로 끝나는 음절이 '아/어'로 시작되는 어미나 선어말 어미 '-었-'과 결합할 때 생성되는 음절
  public static int IDX_YNPEI = 25; // 모음 'ㅣ'로 끝나는 용언이 '아/어'로 시작되는 어미나 선어말 어미 '-었-'과 결합할 때 생성되는 음절
  public static int IDX_YNPOI = 26; // 모음 'ㅚ'로 끝나는 용언이 '아/어'로 시작되는 어미나 선어말 어미 '-었-'과 결합할 때 생성되는 음절
  public static int IDX_YNPLN = 27; // 받침 'ㄹ'로 끝나는 용언이 어미 '-ㄴ'과 결합할 때 생성되는 음절
  public static int IDX_IRRLO = 28; // '러' 불규칙(8개)에 의하여 생성되는 음절 : 러, 렀
  public static int IDX_IRRPLE = 29; // '르' 불규칙(193개)에 의하여 생성되는 음절 
  public static int IDX_IRROO = 30; // '우' 불규칙에 의하여 생성되는 음절 : 퍼, 펐
  public static int IDX_IRROU = 31; // '어' 불규칙에 의하여 생성되는 음절 : 해, 했
  public static int IDX_IRRDA = 32; // 'ㄷ' 불규칙(37개)에 의하여 생성되는 음절
  public static int IDX_IRRBA = 33; // 'ㅂ' 불규칙(446개)에 의하여 생성되는 음절
  public static int IDX_IRRSA = 34; // 'ㅅ' 불규칙(39개)에 의하여 생성되는 음절
  public static int IDX_IRRHA = 35; // 'ㅎ' 불규칙(96개)에 의하여 생성되는 음절 
  public static int IDX_PEND = 36; // 선어말 어미 : 시 셨 았 었 였 겠
  
  public static int IDX_YNPEOMI = 37; // 용언이 어미와 결합할 때 생성되는 음절의 수 734개
  
  /**   용언의 표층 형태로만 사용되는 음절 */
  public static int IDX_WDSURF = 38; 
  
  public static int IDX_EOGAN = 39; // 어미 또는 어미의 변형으로 존재할 수 있는 음 (즉 IDX_EOMI 이거나 IDX_YNPNA 이후에 1이 있는 음절)
  
  private static List<char[]> Syllables;  // 음절특성 정보
  
  /**
   * 인덱스 값에 해당하는 음절의 특성을 반환한다.
   * 영자 또는 숫자일 경우는 모두 해당이 안되므로 가장 마지막 글자인 '힣' 의 음절특성을 반환한다.
   * 
   * @param idx '가'(0xAC00)이 0부터 유니코드에 의해 한글음절을 순차적으로 나열한 값
   * @throws MorphException throw exceptioin
   */
  public static char[] getFeature(int idx)  throws MorphException {
    
    if(Syllables==null) Syllables = getSyllableFeature();
  
    if(idx<0||idx>=Syllables.size()) 
      return Syllables.get(Syllables.size()-1);
    else 
      return Syllables.get(idx);
    
  }
  
  /**
   * 각 음절의 특성을 반환한다.
   * @param syl  음절 하나
   * @throws MorphException throw exception 
   */
  public static char[] getFeature(char syl) throws MorphException {
    
    int idx = syl - 0xAC00;
    return getFeature(idx);
    
  }
  
  /**
   * 음절정보특성을 파일에서 읽는다.
   * 
   * @throws MorphException throw exception
   */  
  private static List<char[]> getSyllableFeature() throws MorphException {
  
    try{
      Syllables = new ArrayList<char[]>();

      List<String> line = DictionaryResources.readLines(DictionaryResources.FILE_SYLLABLE_FEATURE);  
      for(int i=0;i<line.size();i++) {        
        if(i!=0)
          Syllables.add(line.get(i).toCharArray());
      }
    }catch(IOException e) {
      throw new MorphException(e.getMessage());
    } 

    return Syllables;
  }  
  
  public static boolean isAlpanumeric(char ch) {
    return (ch>='0'&&ch<='z');
  }
}

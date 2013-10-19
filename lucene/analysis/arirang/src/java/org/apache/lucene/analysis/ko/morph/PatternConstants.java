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

public interface PatternConstants {

  /** Hangul word patterns for KMA
   */
  public static int PTN_N = 1;  //* 체언 : N/PN/NM/XN/CN/UN/AS/HJ/ET */
  public static int PTN_NJ = 2;  //* 체언 + 조사 */
  public static int PTN_NSM = 3;  //* 체언 + 용언화접미사 + 어미 */
  public static int PTN_NSMJ = 4;  //* 체언 + 용언화접미사 + '음/기' + 조사 */
  public static int PTN_NSMXM =5;  //* 체언 + 용언화접미사 + '아/어' + 보조용언 + 어미 */
  public static int PTN_NJCM =  6; //* 체언 + '에서/부터/에서부터' + '이' + 어미 */
  public static int PTN_NSMXMJ = 7;  //* 체언 + 용언화접미사 + '아/어' + 보조용언 + '음/기' + 조사 */
  
  public static int PTN_VM  =  11;  //* 용언 + 어미 */
  public static int PTN_VMJ =  12; //* 용언 + '음/기' + 조사 */
  public static int PTN_VMCM = 13;  //* 용언 + '음/기' + '이' + 어미 */
  public static int PTN_VMXM = 14;  //* 용언 + '아/어' + 보조용언 + 어미 */
  public static int PTN_VMXMJ= 15;  //* 용언 + '아/어' + 보조용언 + '음/기' + 조사 */

  public static int PTN_AID =  21;  //* 단일어 : 부사, 관형사, 감탄사 */
  public static int PTN_ADVJ =  22;  //* 부사 + 조사 : '빨리도' */

  public static int PTN_NVM =  31;  //* 체언 + 동사 + 어미 */

  public static int PTN_ZZZ =  35;  //* 문장부호, KS 완성형 기호열, 단독조사/어미 */  
  
  //*          CLASSIFICATION OF PARTS OF SPEECH               */
  //  3(basic) + 2(special) types of stem for 'pos'
  public static char POS_NPXM  =   'N';       //* noun, pnoun, xn, nume */
  public static char POS_VJXV  =   'V';       //* verb, adj, xverb      */
  public static char POS_AID   =   'Z';       //* adv, det, excl        */

  public static char POS_PUNC  =   'q';       //* punctuation mark:./,/( */
  public static char POS_SYMB  =   'Q';       //* special symbols       */

  //  normal types of stem for 'pos2'.
  //  Only some of following symbols are used.
  public static char POS_NOUN  =   'N';       //* noun                  */
  public static char POS_PNOUN  =  'P';       //* pronoun               */
  public static char POS_XNOUN  =  'U';       //* dependent noun        */
  public static char POS_NUMERAL = 'M';       //* numeral               */

  public static char POS_PROPER  = 'O';       //* proper noun: NOT USED */

  public static char POS_CNOUN  =  'C';       //* compound noun guessed */
  public static char POS_NOUNK  =  'u';       //* guessed as noun       */

  public static char POS_ASCall =  '@';       //* all alphanumeric chars*/
  public static char POS_ASCend =  '$';       //* end with alphanumeric */
  public static char POS_ASCmid =  '*';       //* ..+alphanumeric+Hangul*/

  //* defined for numeral to digit conversion */
  public static char POS_digits =  '1';       //* digit-string */
  public static char POS_digitH  = '2';       //* digit-string + Hangul*/

  public static char POS_VERB  =   'V';       //* verb                  */
  public static char POS_ADJ   =   'J';       //* adjective             */
  public static char POS_XVERB =   'W';       //* auxiliary verb        */
  public static char POS_XADJ  =   'K';       //* NOT USED YET          */

  public static char POS_ADV   =   'B';       //* adverb                */
  public static char POS_DET   =   'D';       //* determiner            */
  public static char POS_EXCL  =   'L';       //* exclamation           */

  public static char POS_JOSA   =  'j';       //* Korean Josa           */
  public static char POS_COPULA =  'c';       //* copula '-Wi-'         */
  public static char POS_EOMI   =  'e';       //* final Ending          */
  public static char POS_PEOMI  =  'f';       //* prefinal Ending       */
  public static char POS_NEOMI  =  'n';       //* nominalizing Eomi     */

  public static char POS_PREFIX =  'p';       //* prefixes              */
  public static char POS_SFX_N  =  's';       //* noun suffixes: '들/적'*/
  public static char POS_SFX_V  =  't';       //* verb suffixes: '하/되'*/

  public static char POS_ETC   =   'Z';       //* not decided yet       */

}

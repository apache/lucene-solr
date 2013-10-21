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

  //*          CLASSIFICATION OF PARTS OF SPEECH               */
  // types of stem for 'pos'
  public static char POS_AID   =   'Z';       //* adv, det, excl        */

  //  normal types of stem for 'pos2'.
  public static char POS_NOUN  =   'N';       //* noun                  */
  public static char POS_VERB  =   'V';       //* verb                  */
  public static char POS_ETC   =   'Z';       //* not decided yet       */
}

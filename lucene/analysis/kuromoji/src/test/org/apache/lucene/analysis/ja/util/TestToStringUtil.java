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
package org.apache.lucene.analysis.ja.util;


import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.util.LuceneTestCase;

public class TestToStringUtil extends LuceneTestCase {
  public void testPOS() {
    assertEquals("noun-suffix-verbal", ToStringUtil.getPOSTranslation("名詞-接尾-サ変接続"));
  }
  
  public void testHepburn() {
    assertEquals("majan", ToStringUtil.getRomanization("マージャン"));
    assertEquals("uroncha", ToStringUtil.getRomanization("ウーロンチャ"));
    assertEquals("chahan", ToStringUtil.getRomanization("チャーハン"));
    assertEquals("chashu", ToStringUtil.getRomanization("チャーシュー"));
    assertEquals("shumai", ToStringUtil.getRomanization("シューマイ"));
  }
  
  // see http://en.wikipedia.org/wiki/Hepburn_romanization,
  // but this isnt even thorough or really probably what we want!
  public void testHepburnTable() {
    Map<String,String> table = new HashMap<String,String>() {{
      put("ア", "a");   put("イ", "i");   put("ウ", "u");   put("エ", "e");   put("オ", "o");
      put("カ", "ka");  put("キ", "ki");  put("ク", "ku");  put("ケ", "ke");  put("コ", "ko");
      put("サ", "sa");  put("シ", "shi"); put("ス", "su");  put("セ", "se");  put("ソ", "so");
      put("タ", "ta");  put("チ", "chi"); put("ツ", "tsu"); put("テ", "te");  put("ト", "to");
      put("ナ", "na");  put("ニ", "ni");  put("ヌ", "nu");  put("ネ", "ne");  put("ノ", "no");
      put("ハ", "ha");  put("ヒ", "hi");  put("フ", "fu");  put("ヘ", "he");  put("ホ", "ho");
      put("マ", "ma");  put("ミ", "mi");  put("ム", "mu");  put("メ", "me");  put("モ", "mo");
      put("ヤ", "ya");                  put("ユ", "yu");                 put("ヨ", "yo");
      put("ラ", "ra");  put("リ", "ri");  put("ル", "ru");  put("レ", "re");  put("ロ", "ro");
      put("ワ", "wa");  put("ヰ", "i");                   put("ヱ", "e");   put("ヲ", "o");
                                                                     put("ン", "n");
      put("ガ", "ga");  put("ギ", "gi");  put("グ", "gu");  put("ゲ", "ge");  put("ゴ", "go");
      put("ザ", "za");  put("ジ", "ji");  put("ズ", "zu");  put("ゼ", "ze");  put("ゾ", "zo");
      put("ダ", "da");  put("ヂ", "ji");  put("ヅ", "zu");  put("デ", "de");  put("ド", "do");
      put("バ", "ba");  put("ビ", "bi");  put("ブ", "bu");  put("ベ", "be");  put("ボ", "bo");
      put("パ", "pa");  put("ピ", "pi");  put("プ", "pu");  put("ペ", "pe");  put("ポ", "po");
      
                   put("キャ", "kya");   put("キュ", "kyu");   put("キョ", "kyo");
                   put("シャ", "sha");   put("シュ", "shu");   put("ショ", "sho");
                   put("チャ", "cha");   put("チュ", "chu");   put("チョ", "cho");
                   put("ニャ", "nya");   put("ニュ", "nyu");   put("ニョ", "nyo");
                   put("ヒャ", "hya");   put("ヒュ", "hyu");   put("ヒョ", "hyo");
                   put("ミャ", "mya");   put("ミュ", "myu");   put("ミョ", "myo");
                   put("リャ", "rya");   put("リュ", "ryu");   put("リョ", "ryo");
                   put("ギャ", "gya");   put("ギュ", "gyu");   put("ギョ", "gyo");
                   put("ジャ", "ja");    put("ジュ", "ju");    put("ジョ", "jo");
                   put("ヂャ", "ja");    put("ヂュ", "ju");    put("ヂョ", "jo");
                   put("ビャ", "bya");   put("ビュ", "byu");   put("ビョ", "byo");
                   put("ピャ", "pya");   put("ピュ", "pyu");   put("ピョ", "pyo");
      
                      put("イィ", "yi");                 put("イェ", "ye");
      put("ウァ", "wa"); put("ウィ", "wi"); put("ウゥ", "wu"); put("ウェ", "we"); put("ウォ", "wo");
                                     put("ウュ", "wyu");
                                   // TODO: really should be vu
      put("ヴァ", "va"); put("ヴィ", "vi"); put("ヴ", "v");  put("ヴェ", "ve"); put("ヴォ", "vo");
      put("ヴャ", "vya");              put("ヴュ", "vyu"); put("ヴィェ", "vye"); put("ヴョ", "vyo");
                                                     put("キェ", "kye");
                                                     put("ギェ", "gye");
      put("クァ", "kwa"); put("クィ", "kwi");              put("クェ", "kwe"); put("クォ", "kwo");
      put("クヮ", "kwa");
      put("グァ", "gwa"); put("グィ", "gwi");              put("グェ", "gwe"); put("グォ", "gwo");
      put("グヮ", "gwa");
                                                     put("シェ", "she");
                                                     put("ジェ", "je");
                       put("スィ", "si");
                       put("ズィ", "zi");
                                                     put("チェ", "che");
      put("ツァ", "tsa"); put("ツィ", "tsi");              put("ツェ", "tse"); put("ツォ", "tso");
                                     put("ツュ", "tsyu");
                      put("ティ", "ti"); put("トゥ", "tu");
                                     put("テュ", "tyu");
                      put("ディ", "di"); put("ドゥ", "du");
                                     put("デュ", "dyu");
                                                     put("ニェ", "nye");
                                                     put("ヒェ", "hye");
                                                     put("ビェ", "bye");
                                                     put("ピェ", "pye");
      put("ファ", "fa");  put("フィ", "fi");               put("フェ", "fe");  put("フォ", "fo");
      put("フャ", "fya");              put("フュ", "fyu"); put("フィェ", "fye"); put("フョ", "fyo");
                                    put("ホゥ", "hu");
                                                     put("ミェ", "mye");
                                                     put("リェ", "rye");
      put("ラ゜", "la");  put("リ゜", "li");  put("ル゜", "lu");  put("レ゜", "le");  put("ロ゜", "lo");
      put("ヷ", "va");  put("ヸ", "vi");                  put("ヹ", "ve");  put("ヺ", "vo");
    }};
    
    for (String s : table.keySet()) {
      assertEquals(s, table.get(s), ToStringUtil.getRomanization(s));
    }
  }
}

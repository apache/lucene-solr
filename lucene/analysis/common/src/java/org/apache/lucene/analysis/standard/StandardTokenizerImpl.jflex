package org.apache.lucene.analysis.standard;

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

import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;

/**
 * This class implements Word Break rules from the Unicode Text Segmentation 
 * algorithm, as specified in 
 * <a href="http://unicode.org/reports/tr29/">Unicode Standard Annex #29</a>. 
 * <p/>
 * Tokens produced are of the following types:
 * <ul>
 *   <li>&lt;ALPHANUM&gt;: A sequence of alphabetic and numeric characters</li>
 *   <li>&lt;NUM&gt;: A number</li>
 *   <li>&lt;SOUTHEAST_ASIAN&gt;: A sequence of characters from South and Southeast
 *       Asian languages, including Thai, Lao, Myanmar, and Khmer</li>
 *   <li>&lt;IDEOGRAPHIC&gt;: A single CJKV ideographic character</li>
 *   <li>&lt;HIRAGANA&gt;: A single hiragana character</li>
 * </ul>
 */
%%

%unicode 6.1
%integer
%final
%public
%class StandardTokenizerImpl
%implements StandardTokenizerInterface
%function getNextToken
%char
%buffer 4096

%include SUPPLEMENTARY.jflex-macro
ALetter = ([\p{WB:ALetter}] | {ALetterSupp})
Format =  ([\p{WB:Format}] | {FormatSupp})
Numeric = ([\p{WB:Numeric}] | {NumericSupp})
Extend =  ([\p{WB:Extend}] | {ExtendSupp})
Katakana = ([\p{WB:Katakana}] | {KatakanaSupp})
MidLetter = ([\p{WB:MidLetter}] | {MidLetterSupp})
MidNum = ([\p{WB:MidNum}] | {MidNumSupp})
MidNumLet = ([\p{WB:MidNumLet}] | {MidNumLetSupp})
ExtendNumLet = ([\p{WB:ExtendNumLet}] | {ExtendNumLetSupp})
ComplexContext = ([\p{LB:Complex_Context}] | {ComplexContextSupp})
Han = ([\p{Script:Han}] | {HanSupp})
Hiragana = ([\p{Script:Hiragana}] | {HiraganaSupp})

// Script=Hangul & Aletter
HangulEx       = (!(!\p{Script:Hangul}|!\p{WB:ALetter})) ({Format} | {Extend})*
// UAX#29 WB4. X (Extend | Format)* --> X
//
ALetterEx      = {ALetter}                     ({Format} | {Extend})*
// TODO: Convert hard-coded full-width numeric range to property intersection (something like [\p{Full-Width}&&\p{Numeric}]) once JFlex supports it
NumericEx      = ({Numeric} | [\uFF10-\uFF19]) ({Format} | {Extend})*
KatakanaEx     = {Katakana}                    ({Format} | {Extend})* 
MidLetterEx    = ({MidLetter} | {MidNumLet})   ({Format} | {Extend})* 
MidNumericEx   = ({MidNum} | {MidNumLet})      ({Format} | {Extend})*
ExtendNumLetEx = {ExtendNumLet}                ({Format} | {Extend})*

HanEx = {Han} ({Format} | {Extend})*
HiraganaEx = {Hiragana} ({Format} | {Extend})*

%{
  /** Alphanumeric sequences */
  public static final int WORD_TYPE = StandardTokenizer.ALPHANUM;
  
  /** Numbers */
  public static final int NUMERIC_TYPE = StandardTokenizer.NUM;
  
  /**
   * Chars in class \p{Line_Break = Complex_Context} are from South East Asian
   * scripts (Thai, Lao, Myanmar, Khmer, etc.).  Sequences of these are kept 
   * together as as a single token rather than broken up, because the logic
   * required to break them at word boundaries is too complex for UAX#29.
   * <p>
   * See Unicode Line Breaking Algorithm: http://www.unicode.org/reports/tr14/#SA
   */
  public static final int SOUTH_EAST_ASIAN_TYPE = StandardTokenizer.SOUTHEAST_ASIAN;
  
  public static final int IDEOGRAPHIC_TYPE = StandardTokenizer.IDEOGRAPHIC;
  
  public static final int HIRAGANA_TYPE = StandardTokenizer.HIRAGANA;
  
  public static final int KATAKANA_TYPE = StandardTokenizer.KATAKANA;
  
  public static final int HANGUL_TYPE = StandardTokenizer.HANGUL;

  public final int yychar()
  {
    return yychar;
  }

  /**
   * Fills CharTermAttribute with the current token text.
   */
  public final void getText(CharTermAttribute t) {
    t.copyBuffer(zzBuffer, zzStartRead, zzMarkedPos-zzStartRead);
  }
%}

%%

// UAX#29 WB1.   sot   ÷
//        WB2.     ÷   eot
//
<<EOF>> { return StandardTokenizerInterface.YYEOF; }

// UAX#29 WB8.   Numeric × Numeric
//        WB11.  Numeric (MidNum | MidNumLet) × Numeric
//        WB12.  Numeric × (MidNum | MidNumLet) Numeric
//        WB13a. (ALetter | Numeric | Katakana | ExtendNumLet) × ExtendNumLet
//        WB13b. ExtendNumLet × (ALetter | Numeric | Katakana)
//
{ExtendNumLetEx}* {NumericEx} ({ExtendNumLetEx}+ {NumericEx} 
                              | {MidNumericEx} {NumericEx} 
                              | {NumericEx})*
{ExtendNumLetEx}* 
  { return NUMERIC_TYPE; }

// subset of the below for typing purposes only!
{HangulEx}+
  { return HANGUL_TYPE; }
  
{KatakanaEx}+
  { return KATAKANA_TYPE; }

// UAX#29 WB5.   ALetter × ALetter
//        WB6.   ALetter × (MidLetter | MidNumLet) ALetter
//        WB7.   ALetter (MidLetter | MidNumLet) × ALetter
//        WB9.   ALetter × Numeric
//        WB10.  Numeric × ALetter
//        WB13.  Katakana × Katakana
//        WB13a. (ALetter | Numeric | Katakana | ExtendNumLet) × ExtendNumLet
//        WB13b. ExtendNumLet × (ALetter | Numeric | Katakana)
//
{ExtendNumLetEx}*  ( {KatakanaEx} ({ExtendNumLetEx}* {KatakanaEx})* 
                   | ( {NumericEx}  ({ExtendNumLetEx}+ {NumericEx} | {MidNumericEx} {NumericEx} | {NumericEx})*
                     | {ALetterEx}  ({ExtendNumLetEx}+ {ALetterEx} | {MidLetterEx}  {ALetterEx} | {ALetterEx})* )+ ) 
({ExtendNumLetEx}+ ( {KatakanaEx} ({ExtendNumLetEx}* {KatakanaEx})* 
                   | ( {NumericEx}  ({ExtendNumLetEx}+ {NumericEx} | {MidNumericEx} {NumericEx} | {NumericEx})*
                     | {ALetterEx}  ({ExtendNumLetEx}+ {ALetterEx} | {MidLetterEx}  {ALetterEx} | {ALetterEx})* )+ ) )*
{ExtendNumLetEx}*  
  { return WORD_TYPE; }


// From UAX #29:
//
//    [C]haracters with the Line_Break property values of Contingent_Break (CB), 
//    Complex_Context (SA/South East Asian), and XX (Unknown) are assigned word 
//    boundary property values based on criteria outside of the scope of this
//    annex.  That means that satisfactory treatment of languages like Chinese
//    or Thai requires special handling.
// 
// In Unicode 6.1, only one character has the \p{Line_Break = Contingent_Break}
// property: U+FFFC ( ￼ ) OBJECT REPLACEMENT CHARACTER.
//
// In the ICU implementation of UAX#29, \p{Line_Break = Complex_Context}
// character sequences (from South East Asian scripts like Thai, Myanmar, Khmer,
// Lao, etc.) are kept together.  This grammar does the same below.
//
// See also the Unicode Line Breaking Algorithm:
//
//    http://www.unicode.org/reports/tr14/#SA
//
{ComplexContext}+ { return SOUTH_EAST_ASIAN_TYPE; }

// UAX#29 WB14.  Any ÷ Any
//
{HanEx} { return IDEOGRAPHIC_TYPE; }
{HiraganaEx} { return HIRAGANA_TYPE; }


// UAX#29 WB3.   CR × LF
//        WB3a.  (Newline | CR | LF) ÷
//        WB3b.  ÷ (Newline | CR | LF)
//        WB14.  Any ÷ Any
//
[^] { /* Break so we don't hit fall-through warning: */ break; /* Not numeric, word, ideographic, hiragana, or SE Asian -- ignore it. */ }

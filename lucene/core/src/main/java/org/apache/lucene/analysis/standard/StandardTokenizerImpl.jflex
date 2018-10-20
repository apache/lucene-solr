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

package org.apache.lucene.analysis.standard;

import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;

/**
 * This class implements Word Break rules from the Unicode Text Segmentation 
 * algorithm, as specified in 
 * <a href="http://unicode.org/reports/tr29/">Unicode Standard Annex #29</a>. 
 * <p>
 * Tokens produced are of the following types:
 * <ul>
 *   <li>&lt;ALPHANUM&gt;: A sequence of alphabetic and numeric characters</li>
 *   <li>&lt;NUM&gt;: A number</li>
 *   <li>&lt;SOUTHEAST_ASIAN&gt;: A sequence of characters from South and Southeast
 *       Asian languages, including Thai, Lao, Myanmar, and Khmer</li>
 *   <li>&lt;IDEOGRAPHIC&gt;: A single CJKV ideographic character</li>
 *   <li>&lt;HIRAGANA&gt;: A single hiragana character</li>
 *   <li>&lt;KATAKANA&gt;: A sequence of katakana characters</li>
 *   <li>&lt;HANGUL&gt;: A sequence of Hangul characters</li>
 * </ul>
 */
@SuppressWarnings("fallthrough")
%%

%unicode 6.3
%integer
%final
%public
%class StandardTokenizerImpl
%function getNextToken
%char
%buffer 255

// UAX#29 WB4. X (Extend | Format)* --> X
//
HangulEx            = [\p{Script:Hangul}&&[\p{WB:ALetter}\p{WB:Hebrew_Letter}]] [\p{WB:Format}\p{WB:Extend}]*
HebrewOrALetterEx   = [\p{WB:HebrewLetter}\p{WB:ALetter}]                       [\p{WB:Format}\p{WB:Extend}]*
NumericEx           = [\p{WB:Numeric}[\p{Blk:HalfAndFullForms}&&\p{Nd}]]        [\p{WB:Format}\p{WB:Extend}]*
KatakanaEx          = \p{WB:Katakana}                                           [\p{WB:Format}\p{WB:Extend}]* 
MidLetterEx         = [\p{WB:MidLetter}\p{WB:MidNumLet}\p{WB:SingleQuote}]      [\p{WB:Format}\p{WB:Extend}]* 
MidNumericEx        = [\p{WB:MidNum}\p{WB:MidNumLet}\p{WB:SingleQuote}]         [\p{WB:Format}\p{WB:Extend}]*
ExtendNumLetEx      = \p{WB:ExtendNumLet}                                       [\p{WB:Format}\p{WB:Extend}]*
HanEx               = \p{Script:Han}                                            [\p{WB:Format}\p{WB:Extend}]*
HiraganaEx          = \p{Script:Hiragana}                                       [\p{WB:Format}\p{WB:Extend}]*
SingleQuoteEx       = \p{WB:Single_Quote}                                       [\p{WB:Format}\p{WB:Extend}]*
DoubleQuoteEx       = \p{WB:Double_Quote}                                       [\p{WB:Format}\p{WB:Extend}]*
HebrewLetterEx      = \p{WB:Hebrew_Letter}                                      [\p{WB:Format}\p{WB:Extend}]*
RegionalIndicatorEx = \p{WB:RegionalIndicator}                                  [\p{WB:Format}\p{WB:Extend}]*
ComplexContextEx    = \p{LB:Complex_Context}                                    [\p{WB:Format}\p{WB:Extend}]*

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
  
  /** Ideographic token type */
  public static final int IDEOGRAPHIC_TYPE = StandardTokenizer.IDEOGRAPHIC;
  
  /** Hiragana token type */
  public static final int HIRAGANA_TYPE = StandardTokenizer.HIRAGANA;
  
  /** Katakana token type */
  public static final int KATAKANA_TYPE = StandardTokenizer.KATAKANA;

  /** Hangul token type */
  public static final int HANGUL_TYPE = StandardTokenizer.HANGUL;

  /** Character count processed so far */
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
  
  /**
   * Sets the scanner buffer size in chars
   */
   public final void setBufferSize(int numChars) {
     ZZ_BUFFERSIZE = numChars;
     char[] newZzBuffer = new char[ZZ_BUFFERSIZE];
     System.arraycopy(zzBuffer, 0, newZzBuffer, 0, Math.min(zzBuffer.length, ZZ_BUFFERSIZE));
     zzBuffer = newZzBuffer;
   }
%}

%%

// UAX#29 WB1.   sot   ÷
//        WB2.     ÷   eot
//
<<EOF>> { return YYEOF; }

// UAX#29 WB8.   Numeric × Numeric
//        WB11.  Numeric (MidNum | MidNumLet | Single_Quote) × Numeric
//        WB12.  Numeric × (MidNum | MidNumLet | Single_Quote) Numeric
//        WB13a. (ALetter | Hebrew_Letter | Numeric | Katakana | ExtendNumLet) × ExtendNumLet
//        WB13b. ExtendNumLet × (ALetter | Hebrew_Letter | Numeric | Katakana) 
//
{ExtendNumLetEx}* {NumericEx} ( ( {ExtendNumLetEx}* | {MidNumericEx} ) {NumericEx} )* {ExtendNumLetEx}* 
  { return NUMERIC_TYPE; }

// subset of the below for typing purposes only!
{HangulEx}+
  { return HANGUL_TYPE; }
  
{KatakanaEx}+
  { return KATAKANA_TYPE; }

// UAX#29 WB5.   (ALetter | Hebrew_Letter) × (ALetter | Hebrew_Letter)
//        WB6.   (ALetter | Hebrew_Letter) × (MidLetter | MidNumLet | Single_Quote) (ALetter | Hebrew_Letter)
//        WB7.   (ALetter | Hebrew_Letter) (MidLetter | MidNumLet | Single_Quote) × (ALetter | Hebrew_Letter)
//        WB7a.  Hebrew_Letter × Single_Quote
//        WB7b.  Hebrew_Letter × Double_Quote Hebrew_Letter
//        WB7c.  Hebrew_Letter Double_Quote × Hebrew_Letter
//        WB9.   (ALetter | Hebrew_Letter) × Numeric
//        WB10.  Numeric × (ALetter | Hebrew_Letter)
//        WB13.  Katakana × Katakana
//        WB13a. (ALetter | Hebrew_Letter | Numeric | Katakana | ExtendNumLet) × ExtendNumLet
//        WB13b. ExtendNumLet × (ALetter | Hebrew_Letter | Numeric | Katakana) 
//
{ExtendNumLetEx}*  ( {KatakanaEx}          ( {ExtendNumLetEx}*   {KatakanaEx}                           )*
                   | ( {HebrewLetterEx}    ( {SingleQuoteEx}     | {DoubleQuoteEx}  {HebrewLetterEx}    )
                     | {NumericEx}         ( ( {ExtendNumLetEx}* | {MidNumericEx} ) {NumericEx}         )*
                     | {HebrewOrALetterEx} ( ( {ExtendNumLetEx}* | {MidLetterEx}  ) {HebrewOrALetterEx} )*
                     )+
                   )
({ExtendNumLetEx}+ ( {KatakanaEx}          ( {ExtendNumLetEx}*   {KatakanaEx}                           )*
                   | ( {HebrewLetterEx}    ( {SingleQuoteEx}     | {DoubleQuoteEx}  {HebrewLetterEx}    )
                     | {NumericEx}         ( ( {ExtendNumLetEx}* | {MidNumericEx} ) {NumericEx}         )*
                     | {HebrewOrALetterEx} ( ( {ExtendNumLetEx}* | {MidLetterEx}  ) {HebrewOrALetterEx} )*
                     )+
                   )
)*
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
// In Unicode 6.3, only one character has the \p{Line_Break = Contingent_Break}
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
{ComplexContextEx}+ { return SOUTH_EAST_ASIAN_TYPE; }

// UAX#29 WB14.  Any ÷ Any
//
{HanEx} { return IDEOGRAPHIC_TYPE; }
{HiraganaEx} { return HIRAGANA_TYPE; }


// UAX#29 WB3.   CR × LF
//        WB3a.  (Newline | CR | LF) ÷
//        WB3b.  ÷ (Newline | CR | LF)
//        WB13c. Regional_Indicator × Regional_Indicator
//        WB14.  Any ÷ Any
//
{RegionalIndicatorEx} {RegionalIndicatorEx}+ | [^]
  { /* Break so we don't hit fall-through warning: */ break; /* Not numeric, word, ideographic, hiragana, or SE Asian -- ignore it. */ }

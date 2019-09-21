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
 *   <li>&lt;EMOJI&gt;: A sequence of Emoji characters</li>
 * </ul>
 */
@SuppressWarnings("fallthrough")
%%

%unicode 9.0
%integer
%final
%public
%class StandardTokenizerImpl
%function getNextToken
%char
%buffer 255


//////////////////////////////////////////////////////////////////////////
// Begin Emoji Macros - see documentation below, near the EMOJI_TYPE rule

// TODO: Remove this include file when JFlex supports these properties directly (in Unicode 11.0+)
%include ../../../../../../data/jflex/UnicodeEmojiProperties.jflex

// UAX#29 WB4.  X (Extend | Format | ZWJ)* --> X
//
//   \uFE0E (Text Presentation Selector) and \uFE0F (Emoji Presentation Selector) - included in \p{WB:Extend}
//   - are explicitly excluded here so that we can properly handle Emoji sequences.
//
ExtFmtZwjSansPresSel = [[\p{WB:Format}\p{WB:Extend}\p{WB:ZWJ}]--[\uFE0E\uFE0F]]*

KeyCapBaseChar = [0-9#*]
KeyCapBaseCharEx = {KeyCapBaseChar} {ExtFmtZwjSansPresSel}
KeyCap = \u20E3
KeyCapEx = {KeyCap} {ExtFmtZwjSansPresSel}

// # \u3030 = WAVY DASH; \u303D = PART ALTERNATION MARK
AccidentalEmoji = [©®™\u3030\u303D]
EmojiRKAM = ( \p{WB:Regional_Indicator} | {KeyCapBaseChar} | {AccidentalEmoji} | {Emoji_Modifier} )

// Unlike Unicode properties, macros are not allowed in character classes, so we achieve set difference
// by applying DeMorgan: the expression that matches everything of 'a' not matched by 'b' is: !(!a|b)
// TODO: Convert this expression to character class difference when JFlex supports the properties directly (in Unicode 11.0+)
EmojiSansRKAM = !( ! {Emoji} | {EmojiRKAM} )

EmojiChar = ( {Extended_Pictographic} | {EmojiSansRKAM} )

EmojiCharEx         = {EmojiChar}           {ExtFmtZwjSansPresSel}
EmojiModifierBaseEx = {Emoji_Modifier_Base} {ExtFmtZwjSansPresSel}
EmojiModifierEx     = {Emoji_Modifier}      {ExtFmtZwjSansPresSel}

EmojiPresentationSelector = \uFE0F
EmojiCharOrPresSeqOrModSeq = ( \p{WB:ZWJ}* {EmojiCharEx} {EmojiPresentationSelector}? ) | ( ( \p{WB:ZWJ}* {EmojiModifierBaseEx} )? {EmojiModifierEx} )
TagSpec = [\u{E0020}-\u{E007E}]
TagTerm = \u{E007F}

// End Emoji Macros
//////////////////////////////////////////////////////////////////////////


// UAX#29 WB4.  X (Extend | Format | ZWJ)* --> X
//
ExtFmtZwj           = [\p{WB:Format}\p{WB:Extend}\p{WB:ZWJ}]*

HangulEx            = [\p{Script:Hangul}&&[\p{WB:ALetter}\p{WB:Hebrew_Letter}]] {ExtFmtZwj}
AHLetterEx          = [\p{WB:ALetter}\p{WB:Hebrew_Letter}]                      {ExtFmtZwj}
NumericEx           = [\p{WB:Numeric}[\p{Blk:HalfAndFullForms}&&\p{Nd}]]        {ExtFmtZwj}
KatakanaEx          = \p{WB:Katakana}                                           {ExtFmtZwj} 
MidLetterEx         = [\p{WB:MidLetter}\p{WB:MidNumLet}\p{WB:SingleQuote}]      {ExtFmtZwj} 
MidNumericEx        = [\p{WB:MidNum}\p{WB:MidNumLet}\p{WB:SingleQuote}]         {ExtFmtZwj}
ExtendNumLetEx      = \p{WB:ExtendNumLet}                                       {ExtFmtZwj}
HanEx               = \p{Script:Han}                                            {ExtFmtZwj}
HiraganaEx          = \p{Script:Hiragana}                                       {ExtFmtZwj}
SingleQuoteEx       = \p{WB:Single_Quote}                                       {ExtFmtZwj}
DoubleQuoteEx       = \p{WB:Double_Quote}                                       {ExtFmtZwj}
HebrewLetterEx      = \p{WB:Hebrew_Letter}                                      {ExtFmtZwj}
RegionalIndicatorEx = \p{WB:Regional_Indicator}                                 {ExtFmtZwj}
ComplexContextEx    = \p{LB:Complex_Context}                                    {ExtFmtZwj}

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
  
  /** Emoji token type */
  public static final int EMOJI_TYPE = StandardTokenizer.EMOJI;

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

// UAX#29 WB1.    sot ÷ Any
//        WB2.    Any ÷ eot
//
<<EOF>> { return YYEOF; }

// Instead of these: UAX#29 WB3c. ZWJ × (Glue_After_Zwj | EBG)
//                          WB14. (E_Base | EBG) × E_Modifier
//                          WB15. ^ (RI RI)* RI × RI
//                          WB16. [^RI] (RI RI)* RI × RI
//
// We use the "emoji_sequence" rule from http://www.unicode.org/reports/tr51/tr51-14.html (Unicode 11.0)
// and the Emoji data from http://unicode.org/Public/emoji/11.0/emoji-data.txt (in included file UnicodeEmojiProperties.jflex)
// 
// emoji_sequence :=
//    Top-level EBNF           Expanded #1                       Expanded #2                       Expanded #3
//    ---------------------    ----------------------------      -----------------------------     ----------------------------------------------
//      emoji_core_sequence      emoji_combining_sequence          emoji_character                 ( \p{Emoji}
//                                                               | emoji_presentation_sequence     | \p{Emoji} \uFE0F
//                                                               | emoji_keycap_sequence           | [0-9#*] \u{FE0F 20E3}      [1]
//                             | emoji_modifier_sequence                                           | \p{Emoji_Modifier_Base} \p{Emoji_Modifier}
//                             | emoji_flag_sequence                                               | \p{WB:Regional_Indicator}{2}               )
//
//    | emoji_zwj_sequence       emoji_zwj_element                 emoji_character                 ( \p{Emoji}
//                                                               | emoji_presentation_sequence     | \p{Emoji} \uFE0F
//                                                               | emoji_modifier_sequence         | \p{Emoji_Modifier_Base} \p{Emoji_Modifier} )
//                             ( ZWJ emoji_zwj_element )+                                          ( \p{WB:ZWJ} ^^ )+
// 
//    | emoji_tag_sequence     tag_base                            emoji_character                 ( \p{Emoji}
//                                                               | emoji_presentation_sequence     | \p{Emoji} \uFE0F
//                                                               | emoji_modifier_sequence         | \p{Emoji_Modifier_Base} \p{Emoji_Modifier} )
//                             tag_spec                                                            [\u{E0020}-\u{E007E}]+
//                             tag_term                                                            \u{E007F}
//
// [1] https://unicode.org/Public/emoji/11.0/emoji-test.txt includes key cap sequences 
//     WITHOUT \uFE0F (emoji presentation indicator), annotating them as "non-fully-qualified";
//     TR#51 says about non-fully-qualified *ZWJ sequences* that implementations may
//     choose whether to support them for segmentation.  This implementation will
//     recognize /[0-9#*]\u20E3/ - i.e. without \uFE0F - as Emoji. 
//
// See also: http://www.unicode.org/L2/L2016/16315-handling-seg-emoji.pdf
//           https://docs.google.com/document/d/1yDZ5TUZNVVKaM9zYCCLbRIAKGNZANsAGl0bcNzGGvn8
//
//     In particular, the above docs recommend a modified UAX#29 WB3c rule (covered by TR#51's "emoji_zwj_sequence"):
//
//         WB3c′ ZWJ × ​(Extended_Pictographic | EmojiNRK)
//
  {EmojiCharOrPresSeqOrModSeq} ( ( \p{WB:ZWJ} {EmojiCharOrPresSeqOrModSeq} )* | {TagSpec}+ {TagTerm} ) 
| {KeyCapBaseCharEx} {EmojiPresentationSelector}? {KeyCapEx} 
| {RegionalIndicatorEx}{2} 
  { return EMOJI_TYPE; }

// UAX#29 WB8.    Numeric × Numeric
//        WB11.   Numeric (MidNum | MidNumLetQ) × Numeric
//        WB12.   Numeric × (MidNum | MidNumLetQ) Numeric
//        WB13a.  (AHLetter | Numeric | Katakana | ExtendNumLet) × ExtendNumLet
//        WB13b.  ExtendNumLet × (AHLetter | Numeric | Katakana)
//
{ExtendNumLetEx}* {NumericEx} ( ( {ExtendNumLetEx}* | {MidNumericEx} ) {NumericEx} )* {ExtendNumLetEx}*
  { return NUMERIC_TYPE; }

// subset of the below for typing purposes only!
{HangulEx}+
  { return HANGUL_TYPE; }
  
{KatakanaEx}+
  { return KATAKANA_TYPE; }

// UAX#29 WB5.    AHLetter × AHLetter
//        WB6.    AHLetter × (MidLetter | MidNumLetQ) AHLetter
//        WB7.    AHLetter (MidLetter | MidNumLetQ) × AHLetter
//        WB7a.   Hebrew_Letter × Single_Quote
//        WB7b.   Hebrew_Letter × Double_Quote Hebrew_Letter
//        WB7c.   Hebrew_Letter Double_Quote × Hebrew_Letter
//        WB9.    AHLetter × Numeric
//        WB10.   Numeric × AHLetter
//        WB13.   Katakana × Katakana
//        WB13a.  (ALetter | Hebrew_Letter | Numeric | Katakana | ExtendNumLet) × ExtendNumLet
//        WB13b.  ExtendNumLet × (ALetter | Hebrew_Letter | Numeric | Katakana) 
//
{ExtendNumLetEx}*  ( {KatakanaEx}          ( {ExtendNumLetEx}*   {KatakanaEx}                        )*
                   | ( {HebrewLetterEx}    ( {SingleQuoteEx}     | {DoubleQuoteEx}  {HebrewLetterEx} )
                     | {NumericEx}         ( ( {ExtendNumLetEx}* | {MidNumericEx} ) {NumericEx}      )*
                     | {AHLetterEx}        ( ( {ExtendNumLetEx}* | {MidLetterEx}  ) {AHLetterEx}     )*
                     )+
                   )
({ExtendNumLetEx}+ ( {KatakanaEx}          ( {ExtendNumLetEx}*   {KatakanaEx}                        )*
                   | ( {HebrewLetterEx}    ( {SingleQuoteEx}     | {DoubleQuoteEx}  {HebrewLetterEx} )
                     | {NumericEx}         ( ( {ExtendNumLetEx}* | {MidNumericEx} ) {NumericEx}      )*
                     | {AHLetterEx}        ( ( {ExtendNumLetEx}* | {MidLetterEx}  ) {AHLetterEx}     )*
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
// In Unicode 9.0, only one character has the \p{Line_Break = Contingent_Break}
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

// UAX#29 WB999.  Any ÷ Any
//
{HanEx} { return IDEOGRAPHIC_TYPE; }
{HiraganaEx} { return HIRAGANA_TYPE; }

// UAX#29 WB3.    CR × LF
//        WB3a.   (Newline | CR | LF) ÷
//        WB3b.   ÷ (Newline | CR | LF)
//        WB999.  Any ÷ Any
//
[^] { /* Break so we don't hit fall-through warning: */ break; /* Not numeric, word, ideographic, hiragana, emoji or SE Asian -- ignore it. */ }

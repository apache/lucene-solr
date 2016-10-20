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
 * <a href="http://unicode.org/reports/tr29/">Unicode Standard Annex #29</a> 
 * URLs and email addresses are also tokenized according to the relevant RFCs.
 * <p>
 * Tokens produced are of the following types:
 * <ul>
 *   <li>&lt;ALPHANUM&gt;: A sequence of alphabetic and numeric characters</li>
 *   <li>&lt;NUM&gt;: A number</li>
 *   <li>&lt;URL&gt;: A URL</li>
 *   <li>&lt;EMAIL&gt;: An email address</li>
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
%class UAX29URLEmailTokenizerImpl
%function getNextToken
%char
%xstate AVOID_BAD_URL
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

// URL and E-mail syntax specifications:
//
//     RFC-952:  DOD INTERNET HOST TABLE SPECIFICATION
//     RFC-1035: DOMAIN NAMES - IMPLEMENTATION AND SPECIFICATION
//     RFC-1123: Requirements for Internet Hosts - Application and Support
//     RFC-1738: Uniform Resource Locators (URL)
//     RFC-3986: Uniform Resource Identifier (URI): Generic Syntax
//     RFC-5234: Augmented BNF for Syntax Specifications: ABNF
//     RFC-5321: Simple Mail Transfer Protocol
//     RFC-5322: Internet Message Format

%include ASCIITLD.jflex-macro

DomainLabel = [A-Za-z0-9] ([-A-Za-z0-9]* [A-Za-z0-9])?
DomainNameStrict = {DomainLabel} ("." {DomainLabel})* {ASCIITLD}
DomainNameLoose  = {DomainLabel} ("." {DomainLabel})*

IPv4DecimalOctet = "0"{0,2} [0-9] | "0"? [1-9][0-9] | "1" [0-9][0-9] | "2" ([0-4][0-9] | "5" [0-5])
IPv4Address  = {IPv4DecimalOctet} ("." {IPv4DecimalOctet}){3} 
IPv6Hex16Bit = [0-9A-Fa-f]{1,4}
IPv6LeastSignificant32Bits = {IPv4Address} | ({IPv6Hex16Bit} ":" {IPv6Hex16Bit})
IPv6Address =                                                  ({IPv6Hex16Bit} ":"){6} {IPv6LeastSignificant32Bits}
            |                                             "::" ({IPv6Hex16Bit} ":"){5} {IPv6LeastSignificant32Bits}
            |                            {IPv6Hex16Bit}?  "::" ({IPv6Hex16Bit} ":"){4} {IPv6LeastSignificant32Bits}
            | (({IPv6Hex16Bit} ":"){0,1} {IPv6Hex16Bit})? "::" ({IPv6Hex16Bit} ":"){3} {IPv6LeastSignificant32Bits}
            | (({IPv6Hex16Bit} ":"){0,2} {IPv6Hex16Bit})? "::" ({IPv6Hex16Bit} ":"){2} {IPv6LeastSignificant32Bits}
            | (({IPv6Hex16Bit} ":"){0,3} {IPv6Hex16Bit})? "::"  {IPv6Hex16Bit} ":"     {IPv6LeastSignificant32Bits}
            | (({IPv6Hex16Bit} ":"){0,4} {IPv6Hex16Bit})? "::"                         {IPv6LeastSignificant32Bits}
            | (({IPv6Hex16Bit} ":"){0,5} {IPv6Hex16Bit})? "::"                         {IPv6Hex16Bit}
            | (({IPv6Hex16Bit} ":"){0,6} {IPv6Hex16Bit})? "::"

URIunreserved = [-._~A-Za-z0-9]
URIpercentEncoded = "%" [0-9A-Fa-f]{2}
URIsubDelims = [!$&'()*+,;=]
URIloginSegment = ({URIunreserved} | {URIpercentEncoded} | {URIsubDelims})*
URIlogin = {URIloginSegment} (":" {URIloginSegment})? "@"
URIquery    = "?" ({URIunreserved} | {URIpercentEncoded} | {URIsubDelims} | [:@/?])*
URIfragment = "#" ({URIunreserved} | {URIpercentEncoded} | {URIsubDelims} | [:@/?])*
URIport = ":" [0-9]{1,5}
URIhostStrict = ("[" {IPv6Address} "]") | {IPv4Address} | {DomainNameStrict}  
URIhostLoose  = ("[" {IPv6Address} "]") | {IPv4Address} | {DomainNameLoose} 
URIauthorityLoose  = {URIlogin}? {URIhostLoose}  {URIport}?

HTTPsegment = ({URIunreserved} | {URIpercentEncoded} | [;:@&=])*
HTTPpath = ("/" {HTTPsegment})+
HTTPscheme = [hH][tT][tT][pP][sS]? "://"
HTTPurlFull = {HTTPscheme} {URIlogin}? {URIhostLoose} {URIport}? {HTTPpath}? {URIquery}? {URIfragment}?
URIportRequired = {URIport} {HTTPpath}? {URIquery}? {URIfragment}?
HTTPpathRequired = {URIport}? {HTTPpath} {URIquery}? {URIfragment}?
URIqueryRequired = {URIport}? {HTTPpath}? {URIquery} {URIfragment}?
URIfragmentRequired = {URIport}? {HTTPpath}? {URIquery}? {URIfragment}
// {HTTPurlNoScheme} excludes {URIlogin}, because it could otherwise accept e-mail addresses
HTTPurlNoScheme = {URIhostStrict} ({URIportRequired} | {HTTPpathRequired} | {URIqueryRequired} | {URIfragmentRequired})
HTTPurl = {HTTPurlFull} | {HTTPurlNoScheme}

FTPorFILEsegment = ({URIunreserved} | {URIpercentEncoded} | [?:@&=])*
FTPorFILEpath = "/" {FTPorFILEsegment} ("/" {FTPorFILEsegment})*
FTPtype = ";" [tT][yY][pP][eE] "=" [aAiIdD]
FTPscheme = [fF][tT][pP] "://"
FTPurl = {FTPscheme} {URIauthorityLoose} {FTPorFILEpath} {FTPtype}? {URIfragment}?

FILEscheme = [fF][iI][lL][eE] "://"
FILEurl = {FILEscheme} {URIhostLoose}? {FTPorFILEpath} {URIfragment}?

URL = {HTTPurl} | {FTPurl} | {FILEurl}

EMAILquotedString = [\"] ([\u0001-\u0008\u000B\u000C\u000E-\u0021\u0023-\u005B\u005D-\u007E] | [\\] [\u0000-\u007F])* [\"]
EMAILatomText = [A-Za-z0-9!#$%&'*+-/=?\^_`{|}~]
EMAILlabel = {EMAILatomText}+ | {EMAILquotedString}
EMAILlocalPart = {EMAILlabel} ("." {EMAILlabel})*
EMAILdomainLiteralText = [\u0001-\u0008\u000B\u000C\u000E-\u005A\u005E-\u007F] | [\\] [\u0000-\u007F]
// DFA minimization allows {IPv6Address} and {IPv4Address} to be included 
// in the {EMAILbracketedHost} definition without incurring any size penalties, 
// since {EMAILdomainLiteralText} recognizes all valid IP addresses.
// The IP address regexes are included in {EMAILbracketedHost} simply as a 
// reminder that they are acceptable bracketed host forms.
EMAILbracketedHost = "[" ({EMAILdomainLiteralText}* | {IPv4Address} | [iI][pP][vV] "6:" {IPv6Address}) "]"
EMAIL = {EMAILlocalPart} "@" ({DomainNameStrict} | {EMAILbracketedHost})


%{
  /** Alphanumeric sequences */
  public static final int WORD_TYPE = UAX29URLEmailTokenizer.ALPHANUM;
  
  /** Numbers */
  public static final int NUMERIC_TYPE = UAX29URLEmailTokenizer.NUM;
  
  /**
   * Chars in class \p{Line_Break = Complex_Context} are from South East Asian
   * scripts (Thai, Lao, Myanmar, Khmer, etc.).  Sequences of these are kept 
   * together as as a single token rather than broken up, because the logic
   * required to break them at word boundaries is too complex for UAX#29.
   * <p>
   * See Unicode Line Breaking Algorithm: http://www.unicode.org/reports/tr14/#SA
   */
  public static final int SOUTH_EAST_ASIAN_TYPE = UAX29URLEmailTokenizer.SOUTHEAST_ASIAN;
  
  public static final int IDEOGRAPHIC_TYPE = UAX29URLEmailTokenizer.IDEOGRAPHIC;
  
  public static final int HIRAGANA_TYPE = UAX29URLEmailTokenizer.HIRAGANA;
  
  public static final int KATAKANA_TYPE = UAX29URLEmailTokenizer.KATAKANA;
  
  public static final int HANGUL_TYPE = UAX29URLEmailTokenizer.HANGUL;
  
  public static final int EMAIL_TYPE = UAX29URLEmailTokenizer.EMAIL;
  
  public static final int URL_TYPE = UAX29URLEmailTokenizer.URL;

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

<YYINITIAL, AVOID_BAD_URL> {

// UAX#29 WB1.   sot   ÷
//        WB2.     ÷   eot
//
  <<EOF>> { return YYEOF; }

  {URL}   { yybegin(YYINITIAL); return URL_TYPE; }

  // LUCENE-5391: Don't recognize no-scheme domain-only URLs with a following alphanumeric character
  {URIhostStrict} / [^-\w] { yybegin(YYINITIAL); return URL_TYPE; }
}

// Match bad URL (no scheme domain-only URL with a following alphanumeric character)
// then change to AVOID_BAD_URL state and pushback the match.
// This rule won't match when in AVOID_BAD_URL state
{URIhostStrict} / [-\w]  { yybegin(AVOID_BAD_URL); yypushback(yylength()); }

// Match a no-schema domain at EOF
// This rule won't match when in AVOID_BAD_URL state
{URIhostStrict} { return URL_TYPE; }

<YYINITIAL, AVOID_BAD_URL> {

  // LUCENE-3880: Disrupt recognition of "mailto:test" as <ALPHANUM> from "mailto:test@example.org"
  [mM][aA][iI][lL][tT][oO] / ":" {EMAIL} {  yybegin(YYINITIAL); return WORD_TYPE; }

  {EMAIL} { yybegin(YYINITIAL); return EMAIL_TYPE; }

  // UAX#29 WB8.   Numeric × Numeric
  //        WB11.  Numeric (MidNum | MidNumLet | Single_Quote) × Numeric
  //        WB12.  Numeric × (MidNum | MidNumLet | Single_Quote) Numeric
  //        WB13a. (ALetter | Hebrew_Letter | Numeric | Katakana | ExtendNumLet) × ExtendNumLet
  //        WB13b. ExtendNumLet × (ALetter | Hebrew_Letter | Numeric | Katakana)
  //
  {ExtendNumLetEx}* {NumericEx} ( ( {ExtendNumLetEx}* | {MidNumericEx} ) {NumericEx} )* {ExtendNumLetEx}*
    {  yybegin(YYINITIAL); return NUMERIC_TYPE; }

  // subset of the below for typing purposes only!
  {HangulEx}+
    { yybegin(YYINITIAL); return HANGUL_TYPE; }
  
  {KatakanaEx}+
    { yybegin(YYINITIAL); return KATAKANA_TYPE; }

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
    { yybegin(YYINITIAL); return WORD_TYPE; }


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
  {ComplexContextEx}+ { yybegin(YYINITIAL); return SOUTH_EAST_ASIAN_TYPE; }

  // UAX#29 WB14.  Any ÷ Any
  //
  {HanEx} { yybegin(YYINITIAL); return IDEOGRAPHIC_TYPE; }
  {HiraganaEx} { yybegin(YYINITIAL); return HIRAGANA_TYPE; }


  // UAX#29 WB3.   CR × LF
  //        WB3a.  (Newline | CR | LF) ÷
  //        WB3b.  ÷ (Newline | CR | LF)
  //        WB13c. Regional_Indicator × Regional_Indicator
  //        WB14.  Any ÷ Any
  //
  {RegionalIndicatorEx} {RegionalIndicatorEx}+ | [^]
    { yybegin(YYINITIAL); /* Not numeric, word, ideographic, hiragana, or SE Asian -- ignore it. */ }
}
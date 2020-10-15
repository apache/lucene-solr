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
package org.apache.lucene.analysis.miscellaneous;


import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.KeywordTokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;

import java.io.IOException;
import java.util.List;
import java.util.ArrayList;
import java.util.Iterator;

public class TestASCIIFoldingFilter extends BaseTokenStreamTestCase {
  /**
   * Pop one input token's worth of tokens off the filter and verify that they are as expected.
   */
  void assertNextTerms(String expectedUnfolded, String expectedFolded, ASCIIFoldingFilter filter,
      CharTermAttribute termAtt) throws Exception {
    assertTrue(filter.incrementToken());
    assertEquals(expectedFolded, termAtt.toString());
    if (filter.isPreserveOriginal() && !expectedUnfolded.equals(expectedFolded)) {
      assertTrue(filter.incrementToken());
      assertEquals(expectedUnfolded, termAtt.toString());
    }
  }

  // testLain1Accents() is a copy of TestLatin1AccentFilter.testU().
  public void testLatin1Accents() throws Exception {
    TokenStream stream = whitespaceMockTokenizer("Des mot clés À LA CHAÎNE À Á Â Ã Ä Å Æ Ç È É Ê Ë Ì Í Î Ï Ĳ Ð Ñ"
      +" Ò Ó Ô Õ Ö Ø Œ Þ Ù Ú Û Ü Ý Ÿ à á â ã ä å æ ç è é ê ë ì í î ï ĳ"
      +" ð ñ ò ó ô õ ö ø œ ß þ ù ú û ü ý ÿ ﬁ ﬂ");
    ASCIIFoldingFilter filter = new ASCIIFoldingFilter(stream, random().nextBoolean());

    CharTermAttribute termAtt = filter.getAttribute(CharTermAttribute.class);
    filter.reset();
    assertNextTerms("Des", "Des", filter, termAtt);
    assertNextTerms("mot", "mot", filter, termAtt);
    assertNextTerms("clés", "cles", filter, termAtt);
    assertNextTerms("À", "A", filter, termAtt);
    assertNextTerms("LA", "LA", filter, termAtt);
    assertNextTerms("CHAÎNE", "CHAINE", filter, termAtt);
    assertNextTerms("À", "A", filter, termAtt);
    assertNextTerms("Á", "A", filter, termAtt);
    assertNextTerms("Â", "A", filter, termAtt);
    assertNextTerms("Ã", "A", filter, termAtt);
    assertNextTerms("Ä", "A", filter, termAtt);
    assertNextTerms("Å", "A", filter, termAtt);
    assertNextTerms("Æ", "AE", filter, termAtt);
    assertNextTerms("Ç", "C", filter, termAtt);
    assertNextTerms("È", "E", filter, termAtt);
    assertNextTerms("É", "E", filter, termAtt);
    assertNextTerms("Ê", "E", filter, termAtt);
    assertNextTerms("Ë", "E", filter, termAtt);
    assertNextTerms("Ì", "I", filter, termAtt);
    assertNextTerms("Í", "I", filter, termAtt);
    assertNextTerms("Î", "I", filter, termAtt);
    assertNextTerms("Ï", "I", filter, termAtt);
    assertNextTerms("Ĳ", "IJ", filter, termAtt);
    assertNextTerms("Ð", "D", filter, termAtt);
    assertNextTerms("Ñ", "N", filter, termAtt);
    assertNextTerms("Ò", "O", filter, termAtt);
    assertNextTerms("Ó", "O", filter, termAtt);
    assertNextTerms("Ô", "O", filter, termAtt);
    assertNextTerms("Õ", "O", filter, termAtt);
    assertNextTerms("Ö", "O", filter, termAtt);
    assertNextTerms("Ø", "O", filter, termAtt);
    assertNextTerms("Œ", "OE", filter, termAtt);
    assertNextTerms("Þ", "TH", filter, termAtt);
    assertNextTerms("Ù", "U", filter, termAtt);
    assertNextTerms("Ú", "U", filter, termAtt);
    assertNextTerms("Û", "U", filter, termAtt);
    assertNextTerms("Ü", "U", filter, termAtt);
    assertNextTerms("Ý", "Y", filter, termAtt);
    assertNextTerms("Ÿ", "Y", filter, termAtt);
    assertNextTerms("à", "a", filter, termAtt);
    assertNextTerms("á", "a", filter, termAtt);
    assertNextTerms("â", "a", filter, termAtt);
    assertNextTerms("ã", "a", filter, termAtt);
    assertNextTerms("ä", "a", filter, termAtt);
    assertNextTerms("å", "a", filter, termAtt);
    assertNextTerms("æ", "ae", filter, termAtt);
    assertNextTerms("ç", "c", filter, termAtt);
    assertNextTerms("è", "e", filter, termAtt);
    assertNextTerms("é", "e", filter, termAtt);
    assertNextTerms("ê", "e", filter, termAtt);
    assertNextTerms("ë", "e", filter, termAtt);
    assertNextTerms("ì", "i", filter, termAtt);
    assertNextTerms("í", "i", filter, termAtt);
    assertNextTerms("î", "i", filter, termAtt);
    assertNextTerms("ï", "i", filter, termAtt);
    assertNextTerms("ĳ", "ij", filter, termAtt);
    assertNextTerms("ð", "d", filter, termAtt);
    assertNextTerms("ñ", "n", filter, termAtt);
    assertNextTerms("ò", "o", filter, termAtt);
    assertNextTerms("ó", "o", filter, termAtt);
    assertNextTerms("ô", "o", filter, termAtt);
    assertNextTerms("õ", "o", filter, termAtt);
    assertNextTerms("ö", "o", filter, termAtt);
    assertNextTerms("ø", "o", filter, termAtt);
    assertNextTerms("œ", "oe", filter, termAtt);
    assertNextTerms("ß", "ss", filter, termAtt);
    assertNextTerms("þ", "th", filter, termAtt);
    assertNextTerms("ù", "u", filter, termAtt);
    assertNextTerms("ú", "u", filter, termAtt);
    assertNextTerms("û", "u", filter, termAtt);
    assertNextTerms("ü", "u", filter, termAtt);
    assertNextTerms("ý", "y", filter, termAtt);
    assertNextTerms("ÿ", "y", filter, termAtt);
    assertNextTerms("ﬁ", "fi", filter, termAtt);
    assertNextTerms("ﬂ", "fl", filter, termAtt);
    assertFalse(filter.incrementToken());
  }

  // Test that we do not emit duplicated tokens when preserve original is on
  public void testUnmodifiedLetters() throws Exception {
    TokenStream stream = whitespaceMockTokenizer("§ ¦ ¤ END");
    ASCIIFoldingFilter filter = new ASCIIFoldingFilter(stream, true);

    CharTermAttribute termAtt = filter.getAttribute(CharTermAttribute.class);
    filter.reset();
    assertNextTerms("§", "§", filter, termAtt);
    assertNextTerms("¦", "¦", filter, termAtt);
    assertNextTerms("¤", "¤", filter, termAtt);
    assertNextTerms("END", "END", filter, termAtt);
    assertFalse(filter.incrementToken());
  }

  // The following Perl script generated the foldings[] array automatically
  // from ASCIIFoldingFilter.java:
  //
  //    ============== begin get.test.cases.pl ==============
  //
  //    use strict;
  //    use warnings;
  //
  //    my $file = "ASCIIFoldingFilter.java";
  //    my $output = "testcases.txt";
  //    my %codes = ();
  //    my $folded = '';
  //
  //    open IN, "<:utf8", $file || die "Error opening input file '$file': $!";
  //    open OUT, ">:utf8", $output || die "Error opening output file '$output': $!";
  //
  //    while (my $line = <IN>) {
  //      chomp($line);
  //      # case '\u0133': // <char> <maybe URL> [ description ]
  //      if ($line =~ /case\s+'\\u(....)':.*\[([^\]]+)\]/) {
  //        my $code = $1;
  //        my $desc = $2;
  //        $codes{$code} = $desc;
  //      }
  //      # output[outputPos++] = 'A';
  //      elsif ($line =~ /output\[outputPos\+\+\] = '(.+)';/) {
  //        my $output_char = $1;
  //        $folded .= $output_char;
  //      }
  //      elsif ($line =~ /break;/ && length($folded) > 0) {
  //        my $first = 1;
  //        for my $code (sort { hex($a) <=> hex($b) } keys %codes) {
  //          my $desc = $codes{$code};
  //          print OUT '      ';
  //          print OUT '+ ' if (not $first);
  //          $first = 0;
  //          print OUT '"', chr(hex($code)), qq!"  // U+$code: $desc\n!;
  //        }
  //        print OUT qq!      ,"$folded", // Folded result\n\n!;
  //        %codes = ();
  //        $folded = '';
  //      }
  //    }
  //    close OUT;
  //
  //    ============== end get.test.cases.pl ==============
  //
  public void testAllFoldings() throws Exception {
    // Alternating strings of:
    //   1. All non-ASCII characters to be folded, concatenated together as a
    //      single string.
    //   2. The string of ASCII characters to which each of the above
    //      characters should be folded.
    String[] foldings = {
      "À"  // U+00C0: LATIN CAPITAL LETTER A WITH GRAVE
      + "Á"  // U+00C1: LATIN CAPITAL LETTER A WITH ACUTE
      + "Â"  // U+00C2: LATIN CAPITAL LETTER A WITH CIRCUMFLEX
      + "Ã"  // U+00C3: LATIN CAPITAL LETTER A WITH TILDE
      + "Ä"  // U+00C4: LATIN CAPITAL LETTER A WITH DIAERESIS
      + "Å"  // U+00C5: LATIN CAPITAL LETTER A WITH RING ABOVE
      + "Ā"  // U+0100: LATIN CAPITAL LETTER A WITH MACRON
      + "Ă"  // U+0102: LATIN CAPITAL LETTER A WITH BREVE
      + "Ą"  // U+0104: LATIN CAPITAL LETTER A WITH OGONEK
      + "Ə"  // U+018F: LATIN CAPITAL LETTER SCHWA
      + "Ǎ"  // U+01CD: LATIN CAPITAL LETTER A WITH CARON
      + "Ǟ"  // U+01DE: LATIN CAPITAL LETTER A WITH DIAERESIS AND MACRON
      + "Ǡ"  // U+01E0: LATIN CAPITAL LETTER A WITH DOT ABOVE AND MACRON
      + "Ǻ"  // U+01FA: LATIN CAPITAL LETTER A WITH RING ABOVE AND ACUTE
      + "Ȁ"  // U+0200: LATIN CAPITAL LETTER A WITH DOUBLE GRAVE
      + "Ȃ"  // U+0202: LATIN CAPITAL LETTER A WITH INVERTED BREVE
      + "Ȧ"  // U+0226: LATIN CAPITAL LETTER A WITH DOT ABOVE
      + "Ⱥ"  // U+023A: LATIN CAPITAL LETTER A WITH STROKE
      + "ᴀ"  // U+1D00: LATIN LETTER SMALL CAPITAL A
      + "Ḁ"  // U+1E00: LATIN CAPITAL LETTER A WITH RING BELOW
      + "Ạ"  // U+1EA0: LATIN CAPITAL LETTER A WITH DOT BELOW
      + "Ả"  // U+1EA2: LATIN CAPITAL LETTER A WITH HOOK ABOVE
      + "Ấ"  // U+1EA4: LATIN CAPITAL LETTER A WITH CIRCUMFLEX AND ACUTE
      + "Ầ"  // U+1EA6: LATIN CAPITAL LETTER A WITH CIRCUMFLEX AND GRAVE
      + "Ẩ"  // U+1EA8: LATIN CAPITAL LETTER A WITH CIRCUMFLEX AND HOOK ABOVE
      + "Ẫ"  // U+1EAA: LATIN CAPITAL LETTER A WITH CIRCUMFLEX AND TILDE
      + "Ậ"  // U+1EAC: LATIN CAPITAL LETTER A WITH CIRCUMFLEX AND DOT BELOW
      + "Ắ"  // U+1EAE: LATIN CAPITAL LETTER A WITH BREVE AND ACUTE
      + "Ằ"  // U+1EB0: LATIN CAPITAL LETTER A WITH BREVE AND GRAVE
      + "Ẳ"  // U+1EB2: LATIN CAPITAL LETTER A WITH BREVE AND HOOK ABOVE
      + "Ẵ"  // U+1EB4: LATIN CAPITAL LETTER A WITH BREVE AND TILDE
      + "Ặ"  // U+1EB6: LATIN CAPITAL LETTER A WITH BREVE AND DOT BELOW
      + "Ⓐ"  // U+24B6: CIRCLED LATIN CAPITAL LETTER A
      + "Ａ"  // U+FF21: FULLWIDTH LATIN CAPITAL LETTER A
      ,"A", // Folded result

       "à"  // U+00E0: LATIN SMALL LETTER A WITH GRAVE
       + "á"  // U+00E1: LATIN SMALL LETTER A WITH ACUTE
       + "â"  // U+00E2: LATIN SMALL LETTER A WITH CIRCUMFLEX
       + "ã"  // U+00E3: LATIN SMALL LETTER A WITH TILDE
       + "ä"  // U+00E4: LATIN SMALL LETTER A WITH DIAERESIS
       + "å"  // U+00E5: LATIN SMALL LETTER A WITH RING ABOVE
       + "ā"  // U+0101: LATIN SMALL LETTER A WITH MACRON
       + "ă"  // U+0103: LATIN SMALL LETTER A WITH BREVE
       + "ą"  // U+0105: LATIN SMALL LETTER A WITH OGONEK
       + "ǎ"  // U+01CE: LATIN SMALL LETTER A WITH CARON
       + "ǟ"  // U+01DF: LATIN SMALL LETTER A WITH DIAERESIS AND MACRON
       + "ǡ"  // U+01E1: LATIN SMALL LETTER A WITH DOT ABOVE AND MACRON
       + "ǻ"  // U+01FB: LATIN SMALL LETTER A WITH RING ABOVE AND ACUTE
       + "ȁ"  // U+0201: LATIN SMALL LETTER A WITH DOUBLE GRAVE
       + "ȃ"  // U+0203: LATIN SMALL LETTER A WITH INVERTED BREVE
       + "ȧ"  // U+0227: LATIN SMALL LETTER A WITH DOT ABOVE
       + "ɐ"  // U+0250: LATIN SMALL LETTER TURNED A
       + "ə"  // U+0259: LATIN SMALL LETTER SCHWA
       + "ɚ"  // U+025A: LATIN SMALL LETTER SCHWA WITH HOOK
       + "ᶏ"  // U+1D8F: LATIN SMALL LETTER A WITH RETROFLEX HOOK
       + "ḁ"  // U+1E01: LATIN SMALL LETTER A WITH RING BELOW
       + "ᶕ"  // U+1D95: LATIN SMALL LETTER SCHWA WITH RETROFLEX HOOK
       + "ẚ"  // U+1E9A: LATIN SMALL LETTER A WITH RIGHT HALF RING
       + "ạ"  // U+1EA1: LATIN SMALL LETTER A WITH DOT BELOW
       + "ả"  // U+1EA3: LATIN SMALL LETTER A WITH HOOK ABOVE
       + "ấ"  // U+1EA5: LATIN SMALL LETTER A WITH CIRCUMFLEX AND ACUTE
       + "ầ"  // U+1EA7: LATIN SMALL LETTER A WITH CIRCUMFLEX AND GRAVE
       + "ẩ"  // U+1EA9: LATIN SMALL LETTER A WITH CIRCUMFLEX AND HOOK ABOVE
       + "ẫ"  // U+1EAB: LATIN SMALL LETTER A WITH CIRCUMFLEX AND TILDE
       + "ậ"  // U+1EAD: LATIN SMALL LETTER A WITH CIRCUMFLEX AND DOT BELOW
       + "ắ"  // U+1EAF: LATIN SMALL LETTER A WITH BREVE AND ACUTE
       + "ằ"  // U+1EB1: LATIN SMALL LETTER A WITH BREVE AND GRAVE
       + "ẳ"  // U+1EB3: LATIN SMALL LETTER A WITH BREVE AND HOOK ABOVE
       + "ẵ"  // U+1EB5: LATIN SMALL LETTER A WITH BREVE AND TILDE
       + "ặ"  // U+1EB7: LATIN SMALL LETTER A WITH BREVE AND DOT BELOW
       + "ₐ"  // U+2090: LATIN SUBSCRIPT SMALL LETTER A
       + "ₔ"  // U+2094: LATIN SUBSCRIPT SMALL LETTER SCHWA
       + "ⓐ"  // U+24D0: CIRCLED LATIN SMALL LETTER A
       + "ⱥ"  // U+2C65: LATIN SMALL LETTER A WITH STROKE
       + "Ɐ"  // U+2C6F: LATIN CAPITAL LETTER TURNED A
       + "ａ"  // U+FF41: FULLWIDTH LATIN SMALL LETTER A
      ,"a", // Folded result

       "Ꜳ"  // U+A732: LATIN CAPITAL LETTER AA
      ,"AA", // Folded result

       "Æ"  // U+00C6: LATIN CAPITAL LETTER AE
       + "Ǣ"  // U+01E2: LATIN CAPITAL LETTER AE WITH MACRON
       + "Ǽ"  // U+01FC: LATIN CAPITAL LETTER AE WITH ACUTE
       + "ᴁ"  // U+1D01: LATIN LETTER SMALL CAPITAL AE
      ,"AE", // Folded result

       "Ꜵ"  // U+A734: LATIN CAPITAL LETTER AO
      ,"AO", // Folded result

       "Ꜷ"  // U+A736: LATIN CAPITAL LETTER AU
      ,"AU", // Folded result

       "Ꜹ"  // U+A738: LATIN CAPITAL LETTER AV
       + "Ꜻ"  // U+A73A: LATIN CAPITAL LETTER AV WITH HORIZONTAL BAR
      ,"AV", // Folded result

       "Ꜽ"  // U+A73C: LATIN CAPITAL LETTER AY
      ,"AY", // Folded result

       "⒜"  // U+249C: PARENTHESIZED LATIN SMALL LETTER A
      ,"(a)", // Folded result

       "ꜳ"  // U+A733: LATIN SMALL LETTER AA
      ,"aa", // Folded result

       "æ"  // U+00E6: LATIN SMALL LETTER AE
       + "ǣ"  // U+01E3: LATIN SMALL LETTER AE WITH MACRON
       + "ǽ"  // U+01FD: LATIN SMALL LETTER AE WITH ACUTE
       + "ᴂ"  // U+1D02: LATIN SMALL LETTER TURNED AE
      ,"ae", // Folded result

       "ꜵ"  // U+A735: LATIN SMALL LETTER AO
      ,"ao", // Folded result

       "ꜷ"  // U+A737: LATIN SMALL LETTER AU
      ,"au", // Folded result

       "ꜹ"  // U+A739: LATIN SMALL LETTER AV
       + "ꜻ"  // U+A73B: LATIN SMALL LETTER AV WITH HORIZONTAL BAR
      ,"av", // Folded result

       "ꜽ"  // U+A73D: LATIN SMALL LETTER AY
      ,"ay", // Folded result

       "Ɓ"  // U+0181: LATIN CAPITAL LETTER B WITH HOOK
       + "Ƃ"  // U+0182: LATIN CAPITAL LETTER B WITH TOPBAR
       + "Ƀ"  // U+0243: LATIN CAPITAL LETTER B WITH STROKE
       + "ʙ"  // U+0299: LATIN LETTER SMALL CAPITAL B
       + "ᴃ"  // U+1D03: LATIN LETTER SMALL CAPITAL BARRED B
       + "Ḃ"  // U+1E02: LATIN CAPITAL LETTER B WITH DOT ABOVE
       + "Ḅ"  // U+1E04: LATIN CAPITAL LETTER B WITH DOT BELOW
       + "Ḇ"  // U+1E06: LATIN CAPITAL LETTER B WITH LINE BELOW
       + "Ⓑ"  // U+24B7: CIRCLED LATIN CAPITAL LETTER B
       + "Ｂ"  // U+FF22: FULLWIDTH LATIN CAPITAL LETTER B
      ,"B", // Folded result

       "ƀ"  // U+0180: LATIN SMALL LETTER B WITH STROKE
       + "ƃ"  // U+0183: LATIN SMALL LETTER B WITH TOPBAR
       + "ɓ"  // U+0253: LATIN SMALL LETTER B WITH HOOK
       + "ᵬ"  // U+1D6C: LATIN SMALL LETTER B WITH MIDDLE TILDE
       + "ᶀ"  // U+1D80: LATIN SMALL LETTER B WITH PALATAL HOOK
       + "ḃ"  // U+1E03: LATIN SMALL LETTER B WITH DOT ABOVE
       + "ḅ"  // U+1E05: LATIN SMALL LETTER B WITH DOT BELOW
       + "ḇ"  // U+1E07: LATIN SMALL LETTER B WITH LINE BELOW
       + "ⓑ"  // U+24D1: CIRCLED LATIN SMALL LETTER B
       + "ｂ"  // U+FF42: FULLWIDTH LATIN SMALL LETTER B
      ,"b", // Folded result

       "⒝"  // U+249D: PARENTHESIZED LATIN SMALL LETTER B
      ,"(b)", // Folded result

       "Ç"  // U+00C7: LATIN CAPITAL LETTER C WITH CEDILLA
       + "Ć"  // U+0106: LATIN CAPITAL LETTER C WITH ACUTE
       + "Ĉ"  // U+0108: LATIN CAPITAL LETTER C WITH CIRCUMFLEX
       + "Ċ"  // U+010A: LATIN CAPITAL LETTER C WITH DOT ABOVE
       + "Č"  // U+010C: LATIN CAPITAL LETTER C WITH CARON
       + "Ƈ"  // U+0187: LATIN CAPITAL LETTER C WITH HOOK
       + "Ȼ"  // U+023B: LATIN CAPITAL LETTER C WITH STROKE
       + "ʗ"  // U+0297: LATIN LETTER STRETCHED C
       + "ᴄ"  // U+1D04: LATIN LETTER SMALL CAPITAL C
       + "Ḉ"  // U+1E08: LATIN CAPITAL LETTER C WITH CEDILLA AND ACUTE
       + "Ⓒ"  // U+24B8: CIRCLED LATIN CAPITAL LETTER C
       + "Ｃ"  // U+FF23: FULLWIDTH LATIN CAPITAL LETTER C
      ,"C", // Folded result

       "ç"  // U+00E7: LATIN SMALL LETTER C WITH CEDILLA
       + "ć"  // U+0107: LATIN SMALL LETTER C WITH ACUTE
       + "ĉ"  // U+0109: LATIN SMALL LETTER C WITH CIRCUMFLEX
       + "ċ"  // U+010B: LATIN SMALL LETTER C WITH DOT ABOVE
       + "č"  // U+010D: LATIN SMALL LETTER C WITH CARON
       + "ƈ"  // U+0188: LATIN SMALL LETTER C WITH HOOK
       + "ȼ"  // U+023C: LATIN SMALL LETTER C WITH STROKE
       + "ɕ"  // U+0255: LATIN SMALL LETTER C WITH CURL
       + "ḉ"  // U+1E09: LATIN SMALL LETTER C WITH CEDILLA AND ACUTE
       + "ↄ"  // U+2184: LATIN SMALL LETTER REVERSED C
       + "ⓒ"  // U+24D2: CIRCLED LATIN SMALL LETTER C
       + "Ꜿ"  // U+A73E: LATIN CAPITAL LETTER REVERSED C WITH DOT
       + "ꜿ"  // U+A73F: LATIN SMALL LETTER REVERSED C WITH DOT
       + "ｃ"  // U+FF43: FULLWIDTH LATIN SMALL LETTER C
      ,"c", // Folded result

       "⒞"  // U+249E: PARENTHESIZED LATIN SMALL LETTER C
      ,"(c)", // Folded result

       "Ð"  // U+00D0: LATIN CAPITAL LETTER ETH
       + "Ď"  // U+010E: LATIN CAPITAL LETTER D WITH CARON
       + "Đ"  // U+0110: LATIN CAPITAL LETTER D WITH STROKE
       + "Ɖ"  // U+0189: LATIN CAPITAL LETTER AFRICAN D
       + "Ɗ"  // U+018A: LATIN CAPITAL LETTER D WITH HOOK
       + "Ƌ"  // U+018B: LATIN CAPITAL LETTER D WITH TOPBAR
       + "ᴅ"  // U+1D05: LATIN LETTER SMALL CAPITAL D
       + "ᴆ"  // U+1D06: LATIN LETTER SMALL CAPITAL ETH
       + "Ḋ"  // U+1E0A: LATIN CAPITAL LETTER D WITH DOT ABOVE
       + "Ḍ"  // U+1E0C: LATIN CAPITAL LETTER D WITH DOT BELOW
       + "Ḏ"  // U+1E0E: LATIN CAPITAL LETTER D WITH LINE BELOW
       + "Ḑ"  // U+1E10: LATIN CAPITAL LETTER D WITH CEDILLA
       + "Ḓ"  // U+1E12: LATIN CAPITAL LETTER D WITH CIRCUMFLEX BELOW
       + "Ⓓ"  // U+24B9: CIRCLED LATIN CAPITAL LETTER D
       + "Ꝺ"  // U+A779: LATIN CAPITAL LETTER INSULAR D
       + "Ｄ"  // U+FF24: FULLWIDTH LATIN CAPITAL LETTER D
      ,"D", // Folded result

       "ð"  // U+00F0: LATIN SMALL LETTER ETH
       + "ď"  // U+010F: LATIN SMALL LETTER D WITH CARON
       + "đ"  // U+0111: LATIN SMALL LETTER D WITH STROKE
       + "ƌ"  // U+018C: LATIN SMALL LETTER D WITH TOPBAR
       + "ȡ"  // U+0221: LATIN SMALL LETTER D WITH CURL
       + "ɖ"  // U+0256: LATIN SMALL LETTER D WITH TAIL
       + "ɗ"  // U+0257: LATIN SMALL LETTER D WITH HOOK
       + "ᵭ"  // U+1D6D: LATIN SMALL LETTER D WITH MIDDLE TILDE
       + "ᶁ"  // U+1D81: LATIN SMALL LETTER D WITH PALATAL HOOK
       + "ᶑ"  // U+1D91: LATIN SMALL LETTER D WITH HOOK AND TAIL
       + "ḋ"  // U+1E0B: LATIN SMALL LETTER D WITH DOT ABOVE
       + "ḍ"  // U+1E0D: LATIN SMALL LETTER D WITH DOT BELOW
       + "ḏ"  // U+1E0F: LATIN SMALL LETTER D WITH LINE BELOW
       + "ḑ"  // U+1E11: LATIN SMALL LETTER D WITH CEDILLA
       + "ḓ"  // U+1E13: LATIN SMALL LETTER D WITH CIRCUMFLEX BELOW
       + "ⓓ"  // U+24D3: CIRCLED LATIN SMALL LETTER D
       + "ꝺ"  // U+A77A: LATIN SMALL LETTER INSULAR D
       + "ｄ"  // U+FF44: FULLWIDTH LATIN SMALL LETTER D
      ,"d", // Folded result

       "Ǆ"  // U+01C4: LATIN CAPITAL LETTER DZ WITH CARON
       + "Ǳ"  // U+01F1: LATIN CAPITAL LETTER DZ
      ,"DZ", // Folded result

       "ǅ"  // U+01C5: LATIN CAPITAL LETTER D WITH SMALL LETTER Z WITH CARON
       + "ǲ"  // U+01F2: LATIN CAPITAL LETTER D WITH SMALL LETTER Z
      ,"Dz", // Folded result

       "⒟"  // U+249F: PARENTHESIZED LATIN SMALL LETTER D
      ,"(d)", // Folded result

       "ȸ"  // U+0238: LATIN SMALL LETTER DB DIGRAPH
      ,"db", // Folded result

       "ǆ"  // U+01C6: LATIN SMALL LETTER DZ WITH CARON
       + "ǳ"  // U+01F3: LATIN SMALL LETTER DZ
       + "ʣ"  // U+02A3: LATIN SMALL LETTER DZ DIGRAPH
       + "ʥ"  // U+02A5: LATIN SMALL LETTER DZ DIGRAPH WITH CURL
      ,"dz", // Folded result

       "È"  // U+00C8: LATIN CAPITAL LETTER E WITH GRAVE
       + "É"  // U+00C9: LATIN CAPITAL LETTER E WITH ACUTE
       + "Ê"  // U+00CA: LATIN CAPITAL LETTER E WITH CIRCUMFLEX
       + "Ë"  // U+00CB: LATIN CAPITAL LETTER E WITH DIAERESIS
       + "Ē"  // U+0112: LATIN CAPITAL LETTER E WITH MACRON
       + "Ĕ"  // U+0114: LATIN CAPITAL LETTER E WITH BREVE
       + "Ė"  // U+0116: LATIN CAPITAL LETTER E WITH DOT ABOVE
       + "Ę"  // U+0118: LATIN CAPITAL LETTER E WITH OGONEK
       + "Ě"  // U+011A: LATIN CAPITAL LETTER E WITH CARON
       + "Ǝ"  // U+018E: LATIN CAPITAL LETTER REVERSED E
       + "Ɛ"  // U+0190: LATIN CAPITAL LETTER OPEN E
       + "Ȅ"  // U+0204: LATIN CAPITAL LETTER E WITH DOUBLE GRAVE
       + "Ȇ"  // U+0206: LATIN CAPITAL LETTER E WITH INVERTED BREVE
       + "Ȩ"  // U+0228: LATIN CAPITAL LETTER E WITH CEDILLA
       + "Ɇ"  // U+0246: LATIN CAPITAL LETTER E WITH STROKE
       + "ᴇ"  // U+1D07: LATIN LETTER SMALL CAPITAL E
       + "Ḕ"  // U+1E14: LATIN CAPITAL LETTER E WITH MACRON AND GRAVE
       + "Ḗ"  // U+1E16: LATIN CAPITAL LETTER E WITH MACRON AND ACUTE
       + "Ḙ"  // U+1E18: LATIN CAPITAL LETTER E WITH CIRCUMFLEX BELOW
       + "Ḛ"  // U+1E1A: LATIN CAPITAL LETTER E WITH TILDE BELOW
       + "Ḝ"  // U+1E1C: LATIN CAPITAL LETTER E WITH CEDILLA AND BREVE
       + "Ẹ"  // U+1EB8: LATIN CAPITAL LETTER E WITH DOT BELOW
       + "Ẻ"  // U+1EBA: LATIN CAPITAL LETTER E WITH HOOK ABOVE
       + "Ẽ"  // U+1EBC: LATIN CAPITAL LETTER E WITH TILDE
       + "Ế"  // U+1EBE: LATIN CAPITAL LETTER E WITH CIRCUMFLEX AND ACUTE
       + "Ề"  // U+1EC0: LATIN CAPITAL LETTER E WITH CIRCUMFLEX AND GRAVE
       + "Ể"  // U+1EC2: LATIN CAPITAL LETTER E WITH CIRCUMFLEX AND HOOK ABOVE
       + "Ễ"  // U+1EC4: LATIN CAPITAL LETTER E WITH CIRCUMFLEX AND TILDE
       + "Ệ"  // U+1EC6: LATIN CAPITAL LETTER E WITH CIRCUMFLEX AND DOT BELOW
       + "Ⓔ"  // U+24BA: CIRCLED LATIN CAPITAL LETTER E
       + "ⱻ"  // U+2C7B: LATIN LETTER SMALL CAPITAL TURNED E
       + "Ｅ"  // U+FF25: FULLWIDTH LATIN CAPITAL LETTER E
      ,"E", // Folded result

       "è"  // U+00E8: LATIN SMALL LETTER E WITH GRAVE
       + "é"  // U+00E9: LATIN SMALL LETTER E WITH ACUTE
       + "ê"  // U+00EA: LATIN SMALL LETTER E WITH CIRCUMFLEX
       + "ë"  // U+00EB: LATIN SMALL LETTER E WITH DIAERESIS
       + "ē"  // U+0113: LATIN SMALL LETTER E WITH MACRON
       + "ĕ"  // U+0115: LATIN SMALL LETTER E WITH BREVE
       + "ė"  // U+0117: LATIN SMALL LETTER E WITH DOT ABOVE
       + "ę"  // U+0119: LATIN SMALL LETTER E WITH OGONEK
       + "ě"  // U+011B: LATIN SMALL LETTER E WITH CARON
       + "ǝ"  // U+01DD: LATIN SMALL LETTER TURNED E
       + "ȅ"  // U+0205: LATIN SMALL LETTER E WITH DOUBLE GRAVE
       + "ȇ"  // U+0207: LATIN SMALL LETTER E WITH INVERTED BREVE
       + "ȩ"  // U+0229: LATIN SMALL LETTER E WITH CEDILLA
       + "ɇ"  // U+0247: LATIN SMALL LETTER E WITH STROKE
       + "ɘ"  // U+0258: LATIN SMALL LETTER REVERSED E
       + "ɛ"  // U+025B: LATIN SMALL LETTER OPEN E
       + "ɜ"  // U+025C: LATIN SMALL LETTER REVERSED OPEN E
       + "ɝ"  // U+025D: LATIN SMALL LETTER REVERSED OPEN E WITH HOOK
       + "ɞ"  // U+025E: LATIN SMALL LETTER CLOSED REVERSED OPEN E
       + "ʚ"  // U+029A: LATIN SMALL LETTER CLOSED OPEN E
       + "ᴈ"  // U+1D08: LATIN SMALL LETTER TURNED OPEN E
       + "ᶒ"  // U+1D92: LATIN SMALL LETTER E WITH RETROFLEX HOOK
       + "ᶓ"  // U+1D93: LATIN SMALL LETTER OPEN E WITH RETROFLEX HOOK
       + "ᶔ"  // U+1D94: LATIN SMALL LETTER REVERSED OPEN E WITH RETROFLEX HOOK
       + "ḕ"  // U+1E15: LATIN SMALL LETTER E WITH MACRON AND GRAVE
       + "ḗ"  // U+1E17: LATIN SMALL LETTER E WITH MACRON AND ACUTE
       + "ḙ"  // U+1E19: LATIN SMALL LETTER E WITH CIRCUMFLEX BELOW
       + "ḛ"  // U+1E1B: LATIN SMALL LETTER E WITH TILDE BELOW
       + "ḝ"  // U+1E1D: LATIN SMALL LETTER E WITH CEDILLA AND BREVE
       + "ẹ"  // U+1EB9: LATIN SMALL LETTER E WITH DOT BELOW
       + "ẻ"  // U+1EBB: LATIN SMALL LETTER E WITH HOOK ABOVE
       + "ẽ"  // U+1EBD: LATIN SMALL LETTER E WITH TILDE
       + "ế"  // U+1EBF: LATIN SMALL LETTER E WITH CIRCUMFLEX AND ACUTE
       + "ề"  // U+1EC1: LATIN SMALL LETTER E WITH CIRCUMFLEX AND GRAVE
       + "ể"  // U+1EC3: LATIN SMALL LETTER E WITH CIRCUMFLEX AND HOOK ABOVE
       + "ễ"  // U+1EC5: LATIN SMALL LETTER E WITH CIRCUMFLEX AND TILDE
       + "ệ"  // U+1EC7: LATIN SMALL LETTER E WITH CIRCUMFLEX AND DOT BELOW
       + "ₑ"  // U+2091: LATIN SUBSCRIPT SMALL LETTER E
       + "ⓔ"  // U+24D4: CIRCLED LATIN SMALL LETTER E
       + "ⱸ"  // U+2C78: LATIN SMALL LETTER E WITH NOTCH
       + "ｅ"  // U+FF45: FULLWIDTH LATIN SMALL LETTER E
      ,"e", // Folded result

       "⒠"  // U+24A0: PARENTHESIZED LATIN SMALL LETTER E
      ,"(e)", // Folded result

       "Ƒ"  // U+0191: LATIN CAPITAL LETTER F WITH HOOK
       + "Ḟ"  // U+1E1E: LATIN CAPITAL LETTER F WITH DOT ABOVE
       + "Ⓕ"  // U+24BB: CIRCLED LATIN CAPITAL LETTER F
       + "ꜰ"  // U+A730: LATIN LETTER SMALL CAPITAL F
       + "Ꝼ"  // U+A77B: LATIN CAPITAL LETTER INSULAR F
       + "ꟻ"  // U+A7FB: LATIN EPIGRAPHIC LETTER REVERSED F
       + "Ｆ"  // U+FF26: FULLWIDTH LATIN CAPITAL LETTER F
      ,"F", // Folded result

       "ƒ"  // U+0192: LATIN SMALL LETTER F WITH HOOK
       + "ᵮ"  // U+1D6E: LATIN SMALL LETTER F WITH MIDDLE TILDE
       + "ᶂ"  // U+1D82: LATIN SMALL LETTER F WITH PALATAL HOOK
       + "ḟ"  // U+1E1F: LATIN SMALL LETTER F WITH DOT ABOVE
       + "ẛ"  // U+1E9B: LATIN SMALL LETTER LONG S WITH DOT ABOVE
       + "ⓕ"  // U+24D5: CIRCLED LATIN SMALL LETTER F
       + "ꝼ"  // U+A77C: LATIN SMALL LETTER INSULAR F
       + "ｆ"  // U+FF46: FULLWIDTH LATIN SMALL LETTER F
      ,"f", // Folded result

       "⒡"  // U+24A1: PARENTHESIZED LATIN SMALL LETTER F
      ,"(f)", // Folded result

       "ﬀ"  // U+FB00: LATIN SMALL LIGATURE FF
      ,"ff", // Folded result

       "ﬃ"  // U+FB03: LATIN SMALL LIGATURE FFI
      ,"ffi", // Folded result

       "ﬄ"  // U+FB04: LATIN SMALL LIGATURE FFL
      ,"ffl", // Folded result

       "ﬁ"  // U+FB01: LATIN SMALL LIGATURE FI
      ,"fi", // Folded result

       "ﬂ"  // U+FB02: LATIN SMALL LIGATURE FL
      ,"fl", // Folded result

       "Ĝ"  // U+011C: LATIN CAPITAL LETTER G WITH CIRCUMFLEX
       + "Ğ"  // U+011E: LATIN CAPITAL LETTER G WITH BREVE
       + "Ġ"  // U+0120: LATIN CAPITAL LETTER G WITH DOT ABOVE
       + "Ģ"  // U+0122: LATIN CAPITAL LETTER G WITH CEDILLA
       + "Ɠ"  // U+0193: LATIN CAPITAL LETTER G WITH HOOK
       + "Ǥ"  // U+01E4: LATIN CAPITAL LETTER G WITH STROKE
       + "ǥ"  // U+01E5: LATIN SMALL LETTER G WITH STROKE
       + "Ǧ"  // U+01E6: LATIN CAPITAL LETTER G WITH CARON
       + "ǧ"  // U+01E7: LATIN SMALL LETTER G WITH CARON
       + "Ǵ"  // U+01F4: LATIN CAPITAL LETTER G WITH ACUTE
       + "ɢ"  // U+0262: LATIN LETTER SMALL CAPITAL G
       + "ʛ"  // U+029B: LATIN LETTER SMALL CAPITAL G WITH HOOK
       + "Ḡ"  // U+1E20: LATIN CAPITAL LETTER G WITH MACRON
       + "Ⓖ"  // U+24BC: CIRCLED LATIN CAPITAL LETTER G
       + "Ᵹ"  // U+A77D: LATIN CAPITAL LETTER INSULAR G
       + "Ꝿ"  // U+A77E: LATIN CAPITAL LETTER TURNED INSULAR G
       + "Ｇ"  // U+FF27: FULLWIDTH LATIN CAPITAL LETTER G
      ,"G", // Folded result

       "ĝ"  // U+011D: LATIN SMALL LETTER G WITH CIRCUMFLEX
       + "ğ"  // U+011F: LATIN SMALL LETTER G WITH BREVE
       + "ġ"  // U+0121: LATIN SMALL LETTER G WITH DOT ABOVE
       + "ģ"  // U+0123: LATIN SMALL LETTER G WITH CEDILLA
       + "ǵ"  // U+01F5: LATIN SMALL LETTER G WITH ACUTE
       + "ɠ"  // U+0260: LATIN SMALL LETTER G WITH HOOK
       + "ɡ"  // U+0261: LATIN SMALL LETTER SCRIPT G
       + "ᵷ"  // U+1D77: LATIN SMALL LETTER TURNED G
       + "ᵹ"  // U+1D79: LATIN SMALL LETTER INSULAR G
       + "ᶃ"  // U+1D83: LATIN SMALL LETTER G WITH PALATAL HOOK
       + "ḡ"  // U+1E21: LATIN SMALL LETTER G WITH MACRON
       + "ⓖ"  // U+24D6: CIRCLED LATIN SMALL LETTER G
       + "ꝿ"  // U+A77F: LATIN SMALL LETTER TURNED INSULAR G
       + "ｇ"  // U+FF47: FULLWIDTH LATIN SMALL LETTER G
      ,"g", // Folded result

       "⒢"  // U+24A2: PARENTHESIZED LATIN SMALL LETTER G
      ,"(g)", // Folded result

       "Ĥ"  // U+0124: LATIN CAPITAL LETTER H WITH CIRCUMFLEX
       + "Ħ"  // U+0126: LATIN CAPITAL LETTER H WITH STROKE
       + "Ȟ"  // U+021E: LATIN CAPITAL LETTER H WITH CARON
       + "ʜ"  // U+029C: LATIN LETTER SMALL CAPITAL H
       + "Ḣ"  // U+1E22: LATIN CAPITAL LETTER H WITH DOT ABOVE
       + "Ḥ"  // U+1E24: LATIN CAPITAL LETTER H WITH DOT BELOW
       + "Ḧ"  // U+1E26: LATIN CAPITAL LETTER H WITH DIAERESIS
       + "Ḩ"  // U+1E28: LATIN CAPITAL LETTER H WITH CEDILLA
       + "Ḫ"  // U+1E2A: LATIN CAPITAL LETTER H WITH BREVE BELOW
       + "Ⓗ"  // U+24BD: CIRCLED LATIN CAPITAL LETTER H
       + "Ⱨ"  // U+2C67: LATIN CAPITAL LETTER H WITH DESCENDER
       + "Ⱶ"  // U+2C75: LATIN CAPITAL LETTER HALF H
       + "Ｈ"  // U+FF28: FULLWIDTH LATIN CAPITAL LETTER H
      ,"H", // Folded result

       "ĥ"  // U+0125: LATIN SMALL LETTER H WITH CIRCUMFLEX
       + "ħ"  // U+0127: LATIN SMALL LETTER H WITH STROKE
       + "ȟ"  // U+021F: LATIN SMALL LETTER H WITH CARON
       + "ɥ"  // U+0265: LATIN SMALL LETTER TURNED H
       + "ɦ"  // U+0266: LATIN SMALL LETTER H WITH HOOK
       + "ʮ"  // U+02AE: LATIN SMALL LETTER TURNED H WITH FISHHOOK
       + "ʯ"  // U+02AF: LATIN SMALL LETTER TURNED H WITH FISHHOOK AND TAIL
       + "ḣ"  // U+1E23: LATIN SMALL LETTER H WITH DOT ABOVE
       + "ḥ"  // U+1E25: LATIN SMALL LETTER H WITH DOT BELOW
       + "ḧ"  // U+1E27: LATIN SMALL LETTER H WITH DIAERESIS
       + "ḩ"  // U+1E29: LATIN SMALL LETTER H WITH CEDILLA
       + "ḫ"  // U+1E2B: LATIN SMALL LETTER H WITH BREVE BELOW
       + "ẖ"  // U+1E96: LATIN SMALL LETTER H WITH LINE BELOW
       + "ⓗ"  // U+24D7: CIRCLED LATIN SMALL LETTER H
       + "ⱨ"  // U+2C68: LATIN SMALL LETTER H WITH DESCENDER
       + "ⱶ"  // U+2C76: LATIN SMALL LETTER HALF H
       + "ｈ"  // U+FF48: FULLWIDTH LATIN SMALL LETTER H
      ,"h", // Folded result

       "Ƕ"  // U+01F6: LATIN CAPITAL LETTER HWAIR
      ,"HV", // Folded result

       "⒣"  // U+24A3: PARENTHESIZED LATIN SMALL LETTER H
      ,"(h)", // Folded result

       "ƕ"  // U+0195: LATIN SMALL LETTER HV
      ,"hv", // Folded result

       "Ì"  // U+00CC: LATIN CAPITAL LETTER I WITH GRAVE
       + "Í"  // U+00CD: LATIN CAPITAL LETTER I WITH ACUTE
       + "Î"  // U+00CE: LATIN CAPITAL LETTER I WITH CIRCUMFLEX
       + "Ï"  // U+00CF: LATIN CAPITAL LETTER I WITH DIAERESIS
       + "Ĩ"  // U+0128: LATIN CAPITAL LETTER I WITH TILDE
       + "Ī"  // U+012A: LATIN CAPITAL LETTER I WITH MACRON
       + "Ĭ"  // U+012C: LATIN CAPITAL LETTER I WITH BREVE
       + "Į"  // U+012E: LATIN CAPITAL LETTER I WITH OGONEK
       + "İ"  // U+0130: LATIN CAPITAL LETTER I WITH DOT ABOVE
       + "Ɩ"  // U+0196: LATIN CAPITAL LETTER IOTA
       + "Ɨ"  // U+0197: LATIN CAPITAL LETTER I WITH STROKE
       + "Ǐ"  // U+01CF: LATIN CAPITAL LETTER I WITH CARON
       + "Ȉ"  // U+0208: LATIN CAPITAL LETTER I WITH DOUBLE GRAVE
       + "Ȋ"  // U+020A: LATIN CAPITAL LETTER I WITH INVERTED BREVE
       + "ɪ"  // U+026A: LATIN LETTER SMALL CAPITAL I
       + "ᵻ"  // U+1D7B: LATIN SMALL CAPITAL LETTER I WITH STROKE
       + "Ḭ"  // U+1E2C: LATIN CAPITAL LETTER I WITH TILDE BELOW
       + "Ḯ"  // U+1E2E: LATIN CAPITAL LETTER I WITH DIAERESIS AND ACUTE
       + "Ỉ"  // U+1EC8: LATIN CAPITAL LETTER I WITH HOOK ABOVE
       + "Ị"  // U+1ECA: LATIN CAPITAL LETTER I WITH DOT BELOW
       + "Ⓘ"  // U+24BE: CIRCLED LATIN CAPITAL LETTER I
       + "ꟾ"  // U+A7FE: LATIN EPIGRAPHIC LETTER I LONGA
       + "Ｉ"  // U+FF29: FULLWIDTH LATIN CAPITAL LETTER I
      ,"I", // Folded result

       "ì"  // U+00EC: LATIN SMALL LETTER I WITH GRAVE
       + "í"  // U+00ED: LATIN SMALL LETTER I WITH ACUTE
       + "î"  // U+00EE: LATIN SMALL LETTER I WITH CIRCUMFLEX
       + "ï"  // U+00EF: LATIN SMALL LETTER I WITH DIAERESIS
       + "ĩ"  // U+0129: LATIN SMALL LETTER I WITH TILDE
       + "ī"  // U+012B: LATIN SMALL LETTER I WITH MACRON
       + "ĭ"  // U+012D: LATIN SMALL LETTER I WITH BREVE
       + "į"  // U+012F: LATIN SMALL LETTER I WITH OGONEK
       + "ı"  // U+0131: LATIN SMALL LETTER DOTLESS I
       + "ǐ"  // U+01D0: LATIN SMALL LETTER I WITH CARON
       + "ȉ"  // U+0209: LATIN SMALL LETTER I WITH DOUBLE GRAVE
       + "ȋ"  // U+020B: LATIN SMALL LETTER I WITH INVERTED BREVE
       + "ɨ"  // U+0268: LATIN SMALL LETTER I WITH STROKE
       + "ᴉ"  // U+1D09: LATIN SMALL LETTER TURNED I
       + "ᵢ"  // U+1D62: LATIN SUBSCRIPT SMALL LETTER I
       + "ᵼ"  // U+1D7C: LATIN SMALL LETTER IOTA WITH STROKE
       + "ᶖ"  // U+1D96: LATIN SMALL LETTER I WITH RETROFLEX HOOK
       + "ḭ"  // U+1E2D: LATIN SMALL LETTER I WITH TILDE BELOW
       + "ḯ"  // U+1E2F: LATIN SMALL LETTER I WITH DIAERESIS AND ACUTE
       + "ỉ"  // U+1EC9: LATIN SMALL LETTER I WITH HOOK ABOVE
       + "ị"  // U+1ECB: LATIN SMALL LETTER I WITH DOT BELOW
       + "ⁱ"  // U+2071: SUPERSCRIPT LATIN SMALL LETTER I
       + "ⓘ"  // U+24D8: CIRCLED LATIN SMALL LETTER I
       + "ｉ"  // U+FF49: FULLWIDTH LATIN SMALL LETTER I
      ,"i", // Folded result

       "Ĳ"  // U+0132: LATIN CAPITAL LIGATURE IJ
      ,"IJ", // Folded result

       "⒤"  // U+24A4: PARENTHESIZED LATIN SMALL LETTER I
      ,"(i)", // Folded result

       "ĳ"  // U+0133: LATIN SMALL LIGATURE IJ
      ,"ij", // Folded result

       "Ĵ"  // U+0134: LATIN CAPITAL LETTER J WITH CIRCUMFLEX
       + "Ɉ"  // U+0248: LATIN CAPITAL LETTER J WITH STROKE
       + "ᴊ"  // U+1D0A: LATIN LETTER SMALL CAPITAL J
       + "Ⓙ"  // U+24BF: CIRCLED LATIN CAPITAL LETTER J
       + "Ｊ"  // U+FF2A: FULLWIDTH LATIN CAPITAL LETTER J
      ,"J", // Folded result

       "ĵ"  // U+0135: LATIN SMALL LETTER J WITH CIRCUMFLEX
       + "ǰ"  // U+01F0: LATIN SMALL LETTER J WITH CARON
       + "ȷ"  // U+0237: LATIN SMALL LETTER DOTLESS J
       + "ɉ"  // U+0249: LATIN SMALL LETTER J WITH STROKE
       + "ɟ"  // U+025F: LATIN SMALL LETTER DOTLESS J WITH STROKE
       + "ʄ"  // U+0284: LATIN SMALL LETTER DOTLESS J WITH STROKE AND HOOK
       + "ʝ"  // U+029D: LATIN SMALL LETTER J WITH CROSSED-TAIL
       + "ⓙ"  // U+24D9: CIRCLED LATIN SMALL LETTER J
       + "ⱼ"  // U+2C7C: LATIN SUBSCRIPT SMALL LETTER J
       + "ｊ"  // U+FF4A: FULLWIDTH LATIN SMALL LETTER J
      ,"j", // Folded result

       "⒥"  // U+24A5: PARENTHESIZED LATIN SMALL LETTER J
      ,"(j)", // Folded result

       "Ķ"  // U+0136: LATIN CAPITAL LETTER K WITH CEDILLA
       + "Ƙ"  // U+0198: LATIN CAPITAL LETTER K WITH HOOK
       + "Ǩ"  // U+01E8: LATIN CAPITAL LETTER K WITH CARON
       + "ᴋ"  // U+1D0B: LATIN LETTER SMALL CAPITAL K
       + "Ḱ"  // U+1E30: LATIN CAPITAL LETTER K WITH ACUTE
       + "Ḳ"  // U+1E32: LATIN CAPITAL LETTER K WITH DOT BELOW
       + "Ḵ"  // U+1E34: LATIN CAPITAL LETTER K WITH LINE BELOW
       + "Ⓚ"  // U+24C0: CIRCLED LATIN CAPITAL LETTER K
       + "Ⱪ"  // U+2C69: LATIN CAPITAL LETTER K WITH DESCENDER
       + "Ꝁ"  // U+A740: LATIN CAPITAL LETTER K WITH STROKE
       + "Ꝃ"  // U+A742: LATIN CAPITAL LETTER K WITH DIAGONAL STROKE
       + "Ꝅ"  // U+A744: LATIN CAPITAL LETTER K WITH STROKE AND DIAGONAL STROKE
       + "Ｋ"  // U+FF2B: FULLWIDTH LATIN CAPITAL LETTER K
      ,"K", // Folded result

       "ķ"  // U+0137: LATIN SMALL LETTER K WITH CEDILLA
       + "ƙ"  // U+0199: LATIN SMALL LETTER K WITH HOOK
       + "ǩ"  // U+01E9: LATIN SMALL LETTER K WITH CARON
       + "ʞ"  // U+029E: LATIN SMALL LETTER TURNED K
       + "ᶄ"  // U+1D84: LATIN SMALL LETTER K WITH PALATAL HOOK
       + "ḱ"  // U+1E31: LATIN SMALL LETTER K WITH ACUTE
       + "ḳ"  // U+1E33: LATIN SMALL LETTER K WITH DOT BELOW
       + "ḵ"  // U+1E35: LATIN SMALL LETTER K WITH LINE BELOW
       + "ⓚ"  // U+24DA: CIRCLED LATIN SMALL LETTER K
       + "ⱪ"  // U+2C6A: LATIN SMALL LETTER K WITH DESCENDER
       + "ꝁ"  // U+A741: LATIN SMALL LETTER K WITH STROKE
       + "ꝃ"  // U+A743: LATIN SMALL LETTER K WITH DIAGONAL STROKE
       + "ꝅ"  // U+A745: LATIN SMALL LETTER K WITH STROKE AND DIAGONAL STROKE
       + "ｋ"  // U+FF4B: FULLWIDTH LATIN SMALL LETTER K
      ,"k", // Folded result

       "⒦"  // U+24A6: PARENTHESIZED LATIN SMALL LETTER K
      ,"(k)", // Folded result

       "Ĺ"  // U+0139: LATIN CAPITAL LETTER L WITH ACUTE
       + "Ļ"  // U+013B: LATIN CAPITAL LETTER L WITH CEDILLA
       + "Ľ"  // U+013D: LATIN CAPITAL LETTER L WITH CARON
       + "Ŀ"  // U+013F: LATIN CAPITAL LETTER L WITH MIDDLE DOT
       + "Ł"  // U+0141: LATIN CAPITAL LETTER L WITH STROKE
       + "Ƚ"  // U+023D: LATIN CAPITAL LETTER L WITH BAR
       + "ʟ"  // U+029F: LATIN LETTER SMALL CAPITAL L
       + "ᴌ"  // U+1D0C: LATIN LETTER SMALL CAPITAL L WITH STROKE
       + "Ḷ"  // U+1E36: LATIN CAPITAL LETTER L WITH DOT BELOW
       + "Ḹ"  // U+1E38: LATIN CAPITAL LETTER L WITH DOT BELOW AND MACRON
       + "Ḻ"  // U+1E3A: LATIN CAPITAL LETTER L WITH LINE BELOW
       + "Ḽ"  // U+1E3C: LATIN CAPITAL LETTER L WITH CIRCUMFLEX BELOW
       + "Ⓛ"  // U+24C1: CIRCLED LATIN CAPITAL LETTER L
       + "Ⱡ"  // U+2C60: LATIN CAPITAL LETTER L WITH DOUBLE BAR
       + "Ɫ"  // U+2C62: LATIN CAPITAL LETTER L WITH MIDDLE TILDE
       + "Ꝇ"  // U+A746: LATIN CAPITAL LETTER BROKEN L
       + "Ꝉ"  // U+A748: LATIN CAPITAL LETTER L WITH HIGH STROKE
       + "Ꞁ"  // U+A780: LATIN CAPITAL LETTER TURNED L
       + "Ｌ"  // U+FF2C: FULLWIDTH LATIN CAPITAL LETTER L
      ,"L", // Folded result

       "ĺ"  // U+013A: LATIN SMALL LETTER L WITH ACUTE
       + "ļ"  // U+013C: LATIN SMALL LETTER L WITH CEDILLA
       + "ľ"  // U+013E: LATIN SMALL LETTER L WITH CARON
       + "ŀ"  // U+0140: LATIN SMALL LETTER L WITH MIDDLE DOT
       + "ł"  // U+0142: LATIN SMALL LETTER L WITH STROKE
       + "ƚ"  // U+019A: LATIN SMALL LETTER L WITH BAR
       + "ȴ"  // U+0234: LATIN SMALL LETTER L WITH CURL
       + "ɫ"  // U+026B: LATIN SMALL LETTER L WITH MIDDLE TILDE
       + "ɬ"  // U+026C: LATIN SMALL LETTER L WITH BELT
       + "ɭ"  // U+026D: LATIN SMALL LETTER L WITH RETROFLEX HOOK
       + "ᶅ"  // U+1D85: LATIN SMALL LETTER L WITH PALATAL HOOK
       + "ḷ"  // U+1E37: LATIN SMALL LETTER L WITH DOT BELOW
       + "ḹ"  // U+1E39: LATIN SMALL LETTER L WITH DOT BELOW AND MACRON
       + "ḻ"  // U+1E3B: LATIN SMALL LETTER L WITH LINE BELOW
       + "ḽ"  // U+1E3D: LATIN SMALL LETTER L WITH CIRCUMFLEX BELOW
       + "ⓛ"  // U+24DB: CIRCLED LATIN SMALL LETTER L
       + "ⱡ"  // U+2C61: LATIN SMALL LETTER L WITH DOUBLE BAR
       + "ꝇ"  // U+A747: LATIN SMALL LETTER BROKEN L
       + "ꝉ"  // U+A749: LATIN SMALL LETTER L WITH HIGH STROKE
       + "ꞁ"  // U+A781: LATIN SMALL LETTER TURNED L
       + "ｌ"  // U+FF4C: FULLWIDTH LATIN SMALL LETTER L
      ,"l", // Folded result

       "Ǉ"  // U+01C7: LATIN CAPITAL LETTER LJ
      ,"LJ", // Folded result

       "Ỻ"  // U+1EFA: LATIN CAPITAL LETTER MIDDLE-WELSH LL
      ,"LL", // Folded result

       "ǈ"  // U+01C8: LATIN CAPITAL LETTER L WITH SMALL LETTER J
      ,"Lj", // Folded result

       "⒧"  // U+24A7: PARENTHESIZED LATIN SMALL LETTER L
      ,"(l)", // Folded result

       "ǉ"  // U+01C9: LATIN SMALL LETTER LJ
      ,"lj", // Folded result

       "ỻ"  // U+1EFB: LATIN SMALL LETTER MIDDLE-WELSH LL
      ,"ll", // Folded result

       "ʪ"  // U+02AA: LATIN SMALL LETTER LS DIGRAPH
      ,"ls", // Folded result

       "ʫ"  // U+02AB: LATIN SMALL LETTER LZ DIGRAPH
      ,"lz", // Folded result

       "Ɯ"  // U+019C: LATIN CAPITAL LETTER TURNED M
       + "ᴍ"  // U+1D0D: LATIN LETTER SMALL CAPITAL M
       + "Ḿ"  // U+1E3E: LATIN CAPITAL LETTER M WITH ACUTE
       + "Ṁ"  // U+1E40: LATIN CAPITAL LETTER M WITH DOT ABOVE
       + "Ṃ"  // U+1E42: LATIN CAPITAL LETTER M WITH DOT BELOW
       + "Ⓜ"  // U+24C2: CIRCLED LATIN CAPITAL LETTER M
       + "Ɱ"  // U+2C6E: LATIN CAPITAL LETTER M WITH HOOK
       + "ꟽ"  // U+A7FD: LATIN EPIGRAPHIC LETTER INVERTED M
       + "ꟿ"  // U+A7FF: LATIN EPIGRAPHIC LETTER ARCHAIC M
       + "Ｍ"  // U+FF2D: FULLWIDTH LATIN CAPITAL LETTER M
      ,"M", // Folded result

       "ɯ"  // U+026F: LATIN SMALL LETTER TURNED M
       + "ɰ"  // U+0270: LATIN SMALL LETTER TURNED M WITH LONG LEG
       + "ɱ"  // U+0271: LATIN SMALL LETTER M WITH HOOK
       + "ᵯ"  // U+1D6F: LATIN SMALL LETTER M WITH MIDDLE TILDE
       + "ᶆ"  // U+1D86: LATIN SMALL LETTER M WITH PALATAL HOOK
       + "ḿ"  // U+1E3F: LATIN SMALL LETTER M WITH ACUTE
       + "ṁ"  // U+1E41: LATIN SMALL LETTER M WITH DOT ABOVE
       + "ṃ"  // U+1E43: LATIN SMALL LETTER M WITH DOT BELOW
       + "ⓜ"  // U+24DC: CIRCLED LATIN SMALL LETTER M
       + "ｍ"  // U+FF4D: FULLWIDTH LATIN SMALL LETTER M
      ,"m", // Folded result

       "⒨"  // U+24A8: PARENTHESIZED LATIN SMALL LETTER M
      ,"(m)", // Folded result

       "Ñ"  // U+00D1: LATIN CAPITAL LETTER N WITH TILDE
       + "Ń"  // U+0143: LATIN CAPITAL LETTER N WITH ACUTE
       + "Ņ"  // U+0145: LATIN CAPITAL LETTER N WITH CEDILLA
       + "Ň"  // U+0147: LATIN CAPITAL LETTER N WITH CARON
       + "Ŋ"  // U+014A: LATIN CAPITAL LETTER ENG
       + "Ɲ"  // U+019D: LATIN CAPITAL LETTER N WITH LEFT HOOK
       + "Ǹ"  // U+01F8: LATIN CAPITAL LETTER N WITH GRAVE
       + "Ƞ"  // U+0220: LATIN CAPITAL LETTER N WITH LONG RIGHT LEG
       + "ɴ"  // U+0274: LATIN LETTER SMALL CAPITAL N
       + "ᴎ"  // U+1D0E: LATIN LETTER SMALL CAPITAL REVERSED N
       + "Ṅ"  // U+1E44: LATIN CAPITAL LETTER N WITH DOT ABOVE
       + "Ṇ"  // U+1E46: LATIN CAPITAL LETTER N WITH DOT BELOW
       + "Ṉ"  // U+1E48: LATIN CAPITAL LETTER N WITH LINE BELOW
       + "Ṋ"  // U+1E4A: LATIN CAPITAL LETTER N WITH CIRCUMFLEX BELOW
       + "Ⓝ"  // U+24C3: CIRCLED LATIN CAPITAL LETTER N
       + "Ｎ"  // U+FF2E: FULLWIDTH LATIN CAPITAL LETTER N
      ,"N", // Folded result

       "ñ"  // U+00F1: LATIN SMALL LETTER N WITH TILDE
       + "ń"  // U+0144: LATIN SMALL LETTER N WITH ACUTE
       + "ņ"  // U+0146: LATIN SMALL LETTER N WITH CEDILLA
       + "ň"  // U+0148: LATIN SMALL LETTER N WITH CARON
       + "ŉ"  // U+0149: LATIN SMALL LETTER N PRECEDED BY APOSTROPHE
       + "ŋ"  // U+014B: LATIN SMALL LETTER ENG
       + "ƞ"  // U+019E: LATIN SMALL LETTER N WITH LONG RIGHT LEG
       + "ǹ"  // U+01F9: LATIN SMALL LETTER N WITH GRAVE
       + "ȵ"  // U+0235: LATIN SMALL LETTER N WITH CURL
       + "ɲ"  // U+0272: LATIN SMALL LETTER N WITH LEFT HOOK
       + "ɳ"  // U+0273: LATIN SMALL LETTER N WITH RETROFLEX HOOK
       + "ᵰ"  // U+1D70: LATIN SMALL LETTER N WITH MIDDLE TILDE
       + "ᶇ"  // U+1D87: LATIN SMALL LETTER N WITH PALATAL HOOK
       + "ṅ"  // U+1E45: LATIN SMALL LETTER N WITH DOT ABOVE
       + "ṇ"  // U+1E47: LATIN SMALL LETTER N WITH DOT BELOW
       + "ṉ"  // U+1E49: LATIN SMALL LETTER N WITH LINE BELOW
       + "ṋ"  // U+1E4B: LATIN SMALL LETTER N WITH CIRCUMFLEX BELOW
       + "ⁿ"  // U+207F: SUPERSCRIPT LATIN SMALL LETTER N
       + "ⓝ"  // U+24DD: CIRCLED LATIN SMALL LETTER N
       + "ｎ"  // U+FF4E: FULLWIDTH LATIN SMALL LETTER N
      ,"n", // Folded result

       "Ǌ"  // U+01CA: LATIN CAPITAL LETTER NJ
      ,"NJ", // Folded result

       "ǋ"  // U+01CB: LATIN CAPITAL LETTER N WITH SMALL LETTER J
      ,"Nj", // Folded result

       "⒩"  // U+24A9: PARENTHESIZED LATIN SMALL LETTER N
      ,"(n)", // Folded result

       "ǌ"  // U+01CC: LATIN SMALL LETTER NJ
      ,"nj", // Folded result

       "Ò"  // U+00D2: LATIN CAPITAL LETTER O WITH GRAVE
       + "Ó"  // U+00D3: LATIN CAPITAL LETTER O WITH ACUTE
       + "Ô"  // U+00D4: LATIN CAPITAL LETTER O WITH CIRCUMFLEX
       + "Õ"  // U+00D5: LATIN CAPITAL LETTER O WITH TILDE
       + "Ö"  // U+00D6: LATIN CAPITAL LETTER O WITH DIAERESIS
       + "Ø"  // U+00D8: LATIN CAPITAL LETTER O WITH STROKE
       + "Ō"  // U+014C: LATIN CAPITAL LETTER O WITH MACRON
       + "Ŏ"  // U+014E: LATIN CAPITAL LETTER O WITH BREVE
       + "Ő"  // U+0150: LATIN CAPITAL LETTER O WITH DOUBLE ACUTE
       + "Ɔ"  // U+0186: LATIN CAPITAL LETTER OPEN O
       + "Ɵ"  // U+019F: LATIN CAPITAL LETTER O WITH MIDDLE TILDE
       + "Ơ"  // U+01A0: LATIN CAPITAL LETTER O WITH HORN
       + "Ǒ"  // U+01D1: LATIN CAPITAL LETTER O WITH CARON
       + "Ǫ"  // U+01EA: LATIN CAPITAL LETTER O WITH OGONEK
       + "Ǭ"  // U+01EC: LATIN CAPITAL LETTER O WITH OGONEK AND MACRON
       + "Ǿ"  // U+01FE: LATIN CAPITAL LETTER O WITH STROKE AND ACUTE
       + "Ȍ"  // U+020C: LATIN CAPITAL LETTER O WITH DOUBLE GRAVE
       + "Ȏ"  // U+020E: LATIN CAPITAL LETTER O WITH INVERTED BREVE
       + "Ȫ"  // U+022A: LATIN CAPITAL LETTER O WITH DIAERESIS AND MACRON
       + "Ȭ"  // U+022C: LATIN CAPITAL LETTER O WITH TILDE AND MACRON
       + "Ȯ"  // U+022E: LATIN CAPITAL LETTER O WITH DOT ABOVE
       + "Ȱ"  // U+0230: LATIN CAPITAL LETTER O WITH DOT ABOVE AND MACRON
       + "ᴏ"  // U+1D0F: LATIN LETTER SMALL CAPITAL O
       + "ᴐ"  // U+1D10: LATIN LETTER SMALL CAPITAL OPEN O
       + "Ṍ"  // U+1E4C: LATIN CAPITAL LETTER O WITH TILDE AND ACUTE
       + "Ṏ"  // U+1E4E: LATIN CAPITAL LETTER O WITH TILDE AND DIAERESIS
       + "Ṑ"  // U+1E50: LATIN CAPITAL LETTER O WITH MACRON AND GRAVE
       + "Ṓ"  // U+1E52: LATIN CAPITAL LETTER O WITH MACRON AND ACUTE
       + "Ọ"  // U+1ECC: LATIN CAPITAL LETTER O WITH DOT BELOW
       + "Ỏ"  // U+1ECE: LATIN CAPITAL LETTER O WITH HOOK ABOVE
       + "Ố"  // U+1ED0: LATIN CAPITAL LETTER O WITH CIRCUMFLEX AND ACUTE
       + "Ồ"  // U+1ED2: LATIN CAPITAL LETTER O WITH CIRCUMFLEX AND GRAVE
       + "Ổ"  // U+1ED4: LATIN CAPITAL LETTER O WITH CIRCUMFLEX AND HOOK ABOVE
       + "Ỗ"  // U+1ED6: LATIN CAPITAL LETTER O WITH CIRCUMFLEX AND TILDE
       + "Ộ"  // U+1ED8: LATIN CAPITAL LETTER O WITH CIRCUMFLEX AND DOT BELOW
       + "Ớ"  // U+1EDA: LATIN CAPITAL LETTER O WITH HORN AND ACUTE
       + "Ờ"  // U+1EDC: LATIN CAPITAL LETTER O WITH HORN AND GRAVE
       + "Ở"  // U+1EDE: LATIN CAPITAL LETTER O WITH HORN AND HOOK ABOVE
       + "Ỡ"  // U+1EE0: LATIN CAPITAL LETTER O WITH HORN AND TILDE
       + "Ợ"  // U+1EE2: LATIN CAPITAL LETTER O WITH HORN AND DOT BELOW
       + "Ⓞ"  // U+24C4: CIRCLED LATIN CAPITAL LETTER O
       + "Ꝋ"  // U+A74A: LATIN CAPITAL LETTER O WITH LONG STROKE OVERLAY
       + "Ꝍ"  // U+A74C: LATIN CAPITAL LETTER O WITH LOOP
       + "Ｏ"  // U+FF2F: FULLWIDTH LATIN CAPITAL LETTER O
      ,"O", // Folded result

       "ò"  // U+00F2: LATIN SMALL LETTER O WITH GRAVE
       + "ó"  // U+00F3: LATIN SMALL LETTER O WITH ACUTE
       + "ô"  // U+00F4: LATIN SMALL LETTER O WITH CIRCUMFLEX
       + "õ"  // U+00F5: LATIN SMALL LETTER O WITH TILDE
       + "ö"  // U+00F6: LATIN SMALL LETTER O WITH DIAERESIS
       + "ø"  // U+00F8: LATIN SMALL LETTER O WITH STROKE
       + "ō"  // U+014D: LATIN SMALL LETTER O WITH MACRON
       + "ŏ"  // U+014F: LATIN SMALL LETTER O WITH BREVE
       + "ő"  // U+0151: LATIN SMALL LETTER O WITH DOUBLE ACUTE
       + "ơ"  // U+01A1: LATIN SMALL LETTER O WITH HORN
       + "ǒ"  // U+01D2: LATIN SMALL LETTER O WITH CARON
       + "ǫ"  // U+01EB: LATIN SMALL LETTER O WITH OGONEK
       + "ǭ"  // U+01ED: LATIN SMALL LETTER O WITH OGONEK AND MACRON
       + "ǿ"  // U+01FF: LATIN SMALL LETTER O WITH STROKE AND ACUTE
       + "ȍ"  // U+020D: LATIN SMALL LETTER O WITH DOUBLE GRAVE
       + "ȏ"  // U+020F: LATIN SMALL LETTER O WITH INVERTED BREVE
       + "ȫ"  // U+022B: LATIN SMALL LETTER O WITH DIAERESIS AND MACRON
       + "ȭ"  // U+022D: LATIN SMALL LETTER O WITH TILDE AND MACRON
       + "ȯ"  // U+022F: LATIN SMALL LETTER O WITH DOT ABOVE
       + "ȱ"  // U+0231: LATIN SMALL LETTER O WITH DOT ABOVE AND MACRON
       + "ɔ"  // U+0254: LATIN SMALL LETTER OPEN O
       + "ɵ"  // U+0275: LATIN SMALL LETTER BARRED O
       + "ᴖ"  // U+1D16: LATIN SMALL LETTER TOP HALF O
       + "ᴗ"  // U+1D17: LATIN SMALL LETTER BOTTOM HALF O
       + "ᶗ"  // U+1D97: LATIN SMALL LETTER OPEN O WITH RETROFLEX HOOK
       + "ṍ"  // U+1E4D: LATIN SMALL LETTER O WITH TILDE AND ACUTE
       + "ṏ"  // U+1E4F: LATIN SMALL LETTER O WITH TILDE AND DIAERESIS
       + "ṑ"  // U+1E51: LATIN SMALL LETTER O WITH MACRON AND GRAVE
       + "ṓ"  // U+1E53: LATIN SMALL LETTER O WITH MACRON AND ACUTE
       + "ọ"  // U+1ECD: LATIN SMALL LETTER O WITH DOT BELOW
       + "ỏ"  // U+1ECF: LATIN SMALL LETTER O WITH HOOK ABOVE
       + "ố"  // U+1ED1: LATIN SMALL LETTER O WITH CIRCUMFLEX AND ACUTE
       + "ồ"  // U+1ED3: LATIN SMALL LETTER O WITH CIRCUMFLEX AND GRAVE
       + "ổ"  // U+1ED5: LATIN SMALL LETTER O WITH CIRCUMFLEX AND HOOK ABOVE
       + "ỗ"  // U+1ED7: LATIN SMALL LETTER O WITH CIRCUMFLEX AND TILDE
       + "ộ"  // U+1ED9: LATIN SMALL LETTER O WITH CIRCUMFLEX AND DOT BELOW
       + "ớ"  // U+1EDB: LATIN SMALL LETTER O WITH HORN AND ACUTE
       + "ờ"  // U+1EDD: LATIN SMALL LETTER O WITH HORN AND GRAVE
       + "ở"  // U+1EDF: LATIN SMALL LETTER O WITH HORN AND HOOK ABOVE
       + "ỡ"  // U+1EE1: LATIN SMALL LETTER O WITH HORN AND TILDE
       + "ợ"  // U+1EE3: LATIN SMALL LETTER O WITH HORN AND DOT BELOW
       + "ₒ"  // U+2092: LATIN SUBSCRIPT SMALL LETTER O
       + "ⓞ"  // U+24DE: CIRCLED LATIN SMALL LETTER O
       + "ⱺ"  // U+2C7A: LATIN SMALL LETTER O WITH LOW RING INSIDE
       + "ꝋ"  // U+A74B: LATIN SMALL LETTER O WITH LONG STROKE OVERLAY
       + "ꝍ"  // U+A74D: LATIN SMALL LETTER O WITH LOOP
       + "ｏ"  // U+FF4F: FULLWIDTH LATIN SMALL LETTER O
      ,"o", // Folded result

       "Œ"  // U+0152: LATIN CAPITAL LIGATURE OE
       + "ɶ"  // U+0276: LATIN LETTER SMALL CAPITAL OE
      ,"OE", // Folded result

       "Ꝏ"  // U+A74E: LATIN CAPITAL LETTER OO
      ,"OO", // Folded result

       "Ȣ"  // U+0222: LATIN CAPITAL LETTER OU
       + "ᴕ"  // U+1D15: LATIN LETTER SMALL CAPITAL OU
      ,"OU", // Folded result

       "⒪"  // U+24AA: PARENTHESIZED LATIN SMALL LETTER O
      ,"(o)", // Folded result

       "œ"  // U+0153: LATIN SMALL LIGATURE OE
       + "ᴔ"  // U+1D14: LATIN SMALL LETTER TURNED OE
      ,"oe", // Folded result

       "ꝏ"  // U+A74F: LATIN SMALL LETTER OO
      ,"oo", // Folded result

       "ȣ"  // U+0223: LATIN SMALL LETTER OU
      ,"ou", // Folded result

       "Ƥ"  // U+01A4: LATIN CAPITAL LETTER P WITH HOOK
       + "ᴘ"  // U+1D18: LATIN LETTER SMALL CAPITAL P
       + "Ṕ"  // U+1E54: LATIN CAPITAL LETTER P WITH ACUTE
       + "Ṗ"  // U+1E56: LATIN CAPITAL LETTER P WITH DOT ABOVE
       + "Ⓟ"  // U+24C5: CIRCLED LATIN CAPITAL LETTER P
       + "Ᵽ"  // U+2C63: LATIN CAPITAL LETTER P WITH STROKE
       + "Ꝑ"  // U+A750: LATIN CAPITAL LETTER P WITH STROKE THROUGH DESCENDER
       + "Ꝓ"  // U+A752: LATIN CAPITAL LETTER P WITH FLOURISH
       + "Ꝕ"  // U+A754: LATIN CAPITAL LETTER P WITH SQUIRREL TAIL
       + "Ｐ"  // U+FF30: FULLWIDTH LATIN CAPITAL LETTER P
      ,"P", // Folded result

       "ƥ"  // U+01A5: LATIN SMALL LETTER P WITH HOOK
       + "ᵱ"  // U+1D71: LATIN SMALL LETTER P WITH MIDDLE TILDE
       + "ᵽ"  // U+1D7D: LATIN SMALL LETTER P WITH STROKE
       + "ᶈ"  // U+1D88: LATIN SMALL LETTER P WITH PALATAL HOOK
       + "ṕ"  // U+1E55: LATIN SMALL LETTER P WITH ACUTE
       + "ṗ"  // U+1E57: LATIN SMALL LETTER P WITH DOT ABOVE
       + "ⓟ"  // U+24DF: CIRCLED LATIN SMALL LETTER P
       + "ꝑ"  // U+A751: LATIN SMALL LETTER P WITH STROKE THROUGH DESCENDER
       + "ꝓ"  // U+A753: LATIN SMALL LETTER P WITH FLOURISH
       + "ꝕ"  // U+A755: LATIN SMALL LETTER P WITH SQUIRREL TAIL
       + "ꟼ"  // U+A7FC: LATIN EPIGRAPHIC LETTER REVERSED P
       + "ｐ"  // U+FF50: FULLWIDTH LATIN SMALL LETTER P
      ,"p", // Folded result

       "⒫"  // U+24AB: PARENTHESIZED LATIN SMALL LETTER P
      ,"(p)", // Folded result

       "Ɋ"  // U+024A: LATIN CAPITAL LETTER SMALL Q WITH HOOK TAIL
       + "Ⓠ"  // U+24C6: CIRCLED LATIN CAPITAL LETTER Q
       + "Ꝗ"  // U+A756: LATIN CAPITAL LETTER Q WITH STROKE THROUGH DESCENDER
       + "Ꝙ"  // U+A758: LATIN CAPITAL LETTER Q WITH DIAGONAL STROKE
       + "Ｑ"  // U+FF31: FULLWIDTH LATIN CAPITAL LETTER Q
      ,"Q", // Folded result

       "ĸ"  // U+0138: LATIN SMALL LETTER KRA
       + "ɋ"  // U+024B: LATIN SMALL LETTER Q WITH HOOK TAIL
       + "ʠ"  // U+02A0: LATIN SMALL LETTER Q WITH HOOK
       + "ⓠ"  // U+24E0: CIRCLED LATIN SMALL LETTER Q
       + "ꝗ"  // U+A757: LATIN SMALL LETTER Q WITH STROKE THROUGH DESCENDER
       + "ꝙ"  // U+A759: LATIN SMALL LETTER Q WITH DIAGONAL STROKE
       + "ｑ"  // U+FF51: FULLWIDTH LATIN SMALL LETTER Q
      ,"q", // Folded result

       "⒬"  // U+24AC: PARENTHESIZED LATIN SMALL LETTER Q
      ,"(q)", // Folded result

       "ȹ"  // U+0239: LATIN SMALL LETTER QP DIGRAPH
      ,"qp", // Folded result

       "Ŕ"  // U+0154: LATIN CAPITAL LETTER R WITH ACUTE
       + "Ŗ"  // U+0156: LATIN CAPITAL LETTER R WITH CEDILLA
       + "Ř"  // U+0158: LATIN CAPITAL LETTER R WITH CARON
       + "Ȑ"  // U+0210: LATIN CAPITAL LETTER R WITH DOUBLE GRAVE
       + "Ȓ"  // U+0212: LATIN CAPITAL LETTER R WITH INVERTED BREVE
       + "Ɍ"  // U+024C: LATIN CAPITAL LETTER R WITH STROKE
       + "ʀ"  // U+0280: LATIN LETTER SMALL CAPITAL R
       + "ʁ"  // U+0281: LATIN LETTER SMALL CAPITAL INVERTED R
       + "ᴙ"  // U+1D19: LATIN LETTER SMALL CAPITAL REVERSED R
       + "ᴚ"  // U+1D1A: LATIN LETTER SMALL CAPITAL TURNED R
       + "Ṙ"  // U+1E58: LATIN CAPITAL LETTER R WITH DOT ABOVE
       + "Ṛ"  // U+1E5A: LATIN CAPITAL LETTER R WITH DOT BELOW
       + "Ṝ"  // U+1E5C: LATIN CAPITAL LETTER R WITH DOT BELOW AND MACRON
       + "Ṟ"  // U+1E5E: LATIN CAPITAL LETTER R WITH LINE BELOW
       + "Ⓡ"  // U+24C7: CIRCLED LATIN CAPITAL LETTER R
       + "Ɽ"  // U+2C64: LATIN CAPITAL LETTER R WITH TAIL
       + "Ꝛ"  // U+A75A: LATIN CAPITAL LETTER R ROTUNDA
       + "Ꞃ"  // U+A782: LATIN CAPITAL LETTER INSULAR R
       + "Ｒ"  // U+FF32: FULLWIDTH LATIN CAPITAL LETTER R
      ,"R", // Folded result

       "ŕ"  // U+0155: LATIN SMALL LETTER R WITH ACUTE
       + "ŗ"  // U+0157: LATIN SMALL LETTER R WITH CEDILLA
       + "ř"  // U+0159: LATIN SMALL LETTER R WITH CARON
       + "ȑ"  // U+0211: LATIN SMALL LETTER R WITH DOUBLE GRAVE
       + "ȓ"  // U+0213: LATIN SMALL LETTER R WITH INVERTED BREVE
       + "ɍ"  // U+024D: LATIN SMALL LETTER R WITH STROKE
       + "ɼ"  // U+027C: LATIN SMALL LETTER R WITH LONG LEG
       + "ɽ"  // U+027D: LATIN SMALL LETTER R WITH TAIL
       + "ɾ"  // U+027E: LATIN SMALL LETTER R WITH FISHHOOK
       + "ɿ"  // U+027F: LATIN SMALL LETTER REVERSED R WITH FISHHOOK
       + "ᵣ"  // U+1D63: LATIN SUBSCRIPT SMALL LETTER R
       + "ᵲ"  // U+1D72: LATIN SMALL LETTER R WITH MIDDLE TILDE
       + "ᵳ"  // U+1D73: LATIN SMALL LETTER R WITH FISHHOOK AND MIDDLE TILDE
       + "ᶉ"  // U+1D89: LATIN SMALL LETTER R WITH PALATAL HOOK
       + "ṙ"  // U+1E59: LATIN SMALL LETTER R WITH DOT ABOVE
       + "ṛ"  // U+1E5B: LATIN SMALL LETTER R WITH DOT BELOW
       + "ṝ"  // U+1E5D: LATIN SMALL LETTER R WITH DOT BELOW AND MACRON
       + "ṟ"  // U+1E5F: LATIN SMALL LETTER R WITH LINE BELOW
       + "ⓡ"  // U+24E1: CIRCLED LATIN SMALL LETTER R
       + "ꝛ"  // U+A75B: LATIN SMALL LETTER R ROTUNDA
       + "ꞃ"  // U+A783: LATIN SMALL LETTER INSULAR R
       + "ｒ"  // U+FF52: FULLWIDTH LATIN SMALL LETTER R
      ,"r", // Folded result

       "⒭"  // U+24AD: PARENTHESIZED LATIN SMALL LETTER R
      ,"(r)", // Folded result

       "Ś"  // U+015A: LATIN CAPITAL LETTER S WITH ACUTE
       + "Ŝ"  // U+015C: LATIN CAPITAL LETTER S WITH CIRCUMFLEX
       + "Ş"  // U+015E: LATIN CAPITAL LETTER S WITH CEDILLA
       + "Š"  // U+0160: LATIN CAPITAL LETTER S WITH CARON
       + "Ș"  // U+0218: LATIN CAPITAL LETTER S WITH COMMA BELOW
       + "Ṡ"  // U+1E60: LATIN CAPITAL LETTER S WITH DOT ABOVE
       + "Ṣ"  // U+1E62: LATIN CAPITAL LETTER S WITH DOT BELOW
       + "Ṥ"  // U+1E64: LATIN CAPITAL LETTER S WITH ACUTE AND DOT ABOVE
       + "Ṧ"  // U+1E66: LATIN CAPITAL LETTER S WITH CARON AND DOT ABOVE
       + "Ṩ"  // U+1E68: LATIN CAPITAL LETTER S WITH DOT BELOW AND DOT ABOVE
       + "Ⓢ"  // U+24C8: CIRCLED LATIN CAPITAL LETTER S
       + "ꜱ"  // U+A731: LATIN LETTER SMALL CAPITAL S
       + "ꞅ"  // U+A785: LATIN SMALL LETTER INSULAR S
       + "Ｓ"  // U+FF33: FULLWIDTH LATIN CAPITAL LETTER S
      ,"S", // Folded result

       "ś"  // U+015B: LATIN SMALL LETTER S WITH ACUTE
       + "ŝ"  // U+015D: LATIN SMALL LETTER S WITH CIRCUMFLEX
       + "ş"  // U+015F: LATIN SMALL LETTER S WITH CEDILLA
       + "š"  // U+0161: LATIN SMALL LETTER S WITH CARON
       + "ſ"  // U+017F: LATIN SMALL LETTER LONG S
       + "ș"  // U+0219: LATIN SMALL LETTER S WITH COMMA BELOW
       + "ȿ"  // U+023F: LATIN SMALL LETTER S WITH SWASH TAIL
       + "ʂ"  // U+0282: LATIN SMALL LETTER S WITH HOOK
       + "ᵴ"  // U+1D74: LATIN SMALL LETTER S WITH MIDDLE TILDE
       + "ᶊ"  // U+1D8A: LATIN SMALL LETTER S WITH PALATAL HOOK
       + "ṡ"  // U+1E61: LATIN SMALL LETTER S WITH DOT ABOVE
       + "ṣ"  // U+1E63: LATIN SMALL LETTER S WITH DOT BELOW
       + "ṥ"  // U+1E65: LATIN SMALL LETTER S WITH ACUTE AND DOT ABOVE
       + "ṧ"  // U+1E67: LATIN SMALL LETTER S WITH CARON AND DOT ABOVE
       + "ṩ"  // U+1E69: LATIN SMALL LETTER S WITH DOT BELOW AND DOT ABOVE
       + "ẜ"  // U+1E9C: LATIN SMALL LETTER LONG S WITH DIAGONAL STROKE
       + "ẝ"  // U+1E9D: LATIN SMALL LETTER LONG S WITH HIGH STROKE
       + "ⓢ"  // U+24E2: CIRCLED LATIN SMALL LETTER S
       + "Ꞅ"  // U+A784: LATIN CAPITAL LETTER INSULAR S
       + "ｓ"  // U+FF53: FULLWIDTH LATIN SMALL LETTER S
      ,"s", // Folded result

       "ẞ"  // U+1E9E: LATIN CAPITAL LETTER SHARP S
      ,"SS", // Folded result

       "⒮"  // U+24AE: PARENTHESIZED LATIN SMALL LETTER S
      ,"(s)", // Folded result

       "ß"  // U+00DF: LATIN SMALL LETTER SHARP S
      ,"ss", // Folded result

       "ﬆ"  // U+FB06: LATIN SMALL LIGATURE ST
      ,"st", // Folded result

       "Ţ"  // U+0162: LATIN CAPITAL LETTER T WITH CEDILLA
       + "Ť"  // U+0164: LATIN CAPITAL LETTER T WITH CARON
       + "Ŧ"  // U+0166: LATIN CAPITAL LETTER T WITH STROKE
       + "Ƭ"  // U+01AC: LATIN CAPITAL LETTER T WITH HOOK
       + "Ʈ"  // U+01AE: LATIN CAPITAL LETTER T WITH RETROFLEX HOOK
       + "Ț"  // U+021A: LATIN CAPITAL LETTER T WITH COMMA BELOW
       + "Ⱦ"  // U+023E: LATIN CAPITAL LETTER T WITH DIAGONAL STROKE
       + "ᴛ"  // U+1D1B: LATIN LETTER SMALL CAPITAL T
       + "Ṫ"  // U+1E6A: LATIN CAPITAL LETTER T WITH DOT ABOVE
       + "Ṭ"  // U+1E6C: LATIN CAPITAL LETTER T WITH DOT BELOW
       + "Ṯ"  // U+1E6E: LATIN CAPITAL LETTER T WITH LINE BELOW
       + "Ṱ"  // U+1E70: LATIN CAPITAL LETTER T WITH CIRCUMFLEX BELOW
       + "Ⓣ"  // U+24C9: CIRCLED LATIN CAPITAL LETTER T
       + "Ꞇ"  // U+A786: LATIN CAPITAL LETTER INSULAR T
       + "Ｔ"  // U+FF34: FULLWIDTH LATIN CAPITAL LETTER T
      ,"T", // Folded result

       "ţ"  // U+0163: LATIN SMALL LETTER T WITH CEDILLA
       + "ť"  // U+0165: LATIN SMALL LETTER T WITH CARON
       + "ŧ"  // U+0167: LATIN SMALL LETTER T WITH STROKE
       + "ƫ"  // U+01AB: LATIN SMALL LETTER T WITH PALATAL HOOK
       + "ƭ"  // U+01AD: LATIN SMALL LETTER T WITH HOOK
       + "ț"  // U+021B: LATIN SMALL LETTER T WITH COMMA BELOW
       + "ȶ"  // U+0236: LATIN SMALL LETTER T WITH CURL
       + "ʇ"  // U+0287: LATIN SMALL LETTER TURNED T
       + "ʈ"  // U+0288: LATIN SMALL LETTER T WITH RETROFLEX HOOK
       + "ᵵ"  // U+1D75: LATIN SMALL LETTER T WITH MIDDLE TILDE
       + "ṫ"  // U+1E6B: LATIN SMALL LETTER T WITH DOT ABOVE
       + "ṭ"  // U+1E6D: LATIN SMALL LETTER T WITH DOT BELOW
       + "ṯ"  // U+1E6F: LATIN SMALL LETTER T WITH LINE BELOW
       + "ṱ"  // U+1E71: LATIN SMALL LETTER T WITH CIRCUMFLEX BELOW
       + "ẗ"  // U+1E97: LATIN SMALL LETTER T WITH DIAERESIS
       + "ⓣ"  // U+24E3: CIRCLED LATIN SMALL LETTER T
       + "ⱦ"  // U+2C66: LATIN SMALL LETTER T WITH DIAGONAL STROKE
       + "ｔ"  // U+FF54: FULLWIDTH LATIN SMALL LETTER T
      ,"t", // Folded result

       "Þ"  // U+00DE: LATIN CAPITAL LETTER THORN
       + "Ꝧ"  // U+A766: LATIN CAPITAL LETTER THORN WITH STROKE THROUGH DESCENDER
      ,"TH", // Folded result

       "Ꜩ"  // U+A728: LATIN CAPITAL LETTER TZ
      ,"TZ", // Folded result

       "⒯"  // U+24AF: PARENTHESIZED LATIN SMALL LETTER T
      ,"(t)", // Folded result

       "ʨ"  // U+02A8: LATIN SMALL LETTER TC DIGRAPH WITH CURL
      ,"tc", // Folded result

       "þ"  // U+00FE: LATIN SMALL LETTER THORN
       + "ᵺ"  // U+1D7A: LATIN SMALL LETTER TH WITH STRIKETHROUGH
       + "ꝧ"  // U+A767: LATIN SMALL LETTER THORN WITH STROKE THROUGH DESCENDER
      ,"th", // Folded result

       "ʦ"  // U+02A6: LATIN SMALL LETTER TS DIGRAPH
      ,"ts", // Folded result

       "ꜩ"  // U+A729: LATIN SMALL LETTER TZ
      ,"tz", // Folded result

       "Ù"  // U+00D9: LATIN CAPITAL LETTER U WITH GRAVE
       + "Ú"  // U+00DA: LATIN CAPITAL LETTER U WITH ACUTE
       + "Û"  // U+00DB: LATIN CAPITAL LETTER U WITH CIRCUMFLEX
       + "Ü"  // U+00DC: LATIN CAPITAL LETTER U WITH DIAERESIS
       + "Ũ"  // U+0168: LATIN CAPITAL LETTER U WITH TILDE
       + "Ū"  // U+016A: LATIN CAPITAL LETTER U WITH MACRON
       + "Ŭ"  // U+016C: LATIN CAPITAL LETTER U WITH BREVE
       + "Ů"  // U+016E: LATIN CAPITAL LETTER U WITH RING ABOVE
       + "Ű"  // U+0170: LATIN CAPITAL LETTER U WITH DOUBLE ACUTE
       + "Ų"  // U+0172: LATIN CAPITAL LETTER U WITH OGONEK
       + "Ư"  // U+01AF: LATIN CAPITAL LETTER U WITH HORN
       + "Ǔ"  // U+01D3: LATIN CAPITAL LETTER U WITH CARON
       + "Ǖ"  // U+01D5: LATIN CAPITAL LETTER U WITH DIAERESIS AND MACRON
       + "Ǘ"  // U+01D7: LATIN CAPITAL LETTER U WITH DIAERESIS AND ACUTE
       + "Ǚ"  // U+01D9: LATIN CAPITAL LETTER U WITH DIAERESIS AND CARON
       + "Ǜ"  // U+01DB: LATIN CAPITAL LETTER U WITH DIAERESIS AND GRAVE
       + "Ȕ"  // U+0214: LATIN CAPITAL LETTER U WITH DOUBLE GRAVE
       + "Ȗ"  // U+0216: LATIN CAPITAL LETTER U WITH INVERTED BREVE
       + "Ʉ"  // U+0244: LATIN CAPITAL LETTER U BAR
       + "ᴜ"  // U+1D1C: LATIN LETTER SMALL CAPITAL U
       + "ᵾ"  // U+1D7E: LATIN SMALL CAPITAL LETTER U WITH STROKE
       + "Ṳ"  // U+1E72: LATIN CAPITAL LETTER U WITH DIAERESIS BELOW
       + "Ṵ"  // U+1E74: LATIN CAPITAL LETTER U WITH TILDE BELOW
       + "Ṷ"  // U+1E76: LATIN CAPITAL LETTER U WITH CIRCUMFLEX BELOW
       + "Ṹ"  // U+1E78: LATIN CAPITAL LETTER U WITH TILDE AND ACUTE
       + "Ṻ"  // U+1E7A: LATIN CAPITAL LETTER U WITH MACRON AND DIAERESIS
       + "Ụ"  // U+1EE4: LATIN CAPITAL LETTER U WITH DOT BELOW
       + "Ủ"  // U+1EE6: LATIN CAPITAL LETTER U WITH HOOK ABOVE
       + "Ứ"  // U+1EE8: LATIN CAPITAL LETTER U WITH HORN AND ACUTE
       + "Ừ"  // U+1EEA: LATIN CAPITAL LETTER U WITH HORN AND GRAVE
       + "Ử"  // U+1EEC: LATIN CAPITAL LETTER U WITH HORN AND HOOK ABOVE
       + "Ữ"  // U+1EEE: LATIN CAPITAL LETTER U WITH HORN AND TILDE
       + "Ự"  // U+1EF0: LATIN CAPITAL LETTER U WITH HORN AND DOT BELOW
       + "Ⓤ"  // U+24CA: CIRCLED LATIN CAPITAL LETTER U
       + "Ｕ"  // U+FF35: FULLWIDTH LATIN CAPITAL LETTER U
      ,"U", // Folded result

       "ù"  // U+00F9: LATIN SMALL LETTER U WITH GRAVE
       + "ú"  // U+00FA: LATIN SMALL LETTER U WITH ACUTE
       + "û"  // U+00FB: LATIN SMALL LETTER U WITH CIRCUMFLEX
       + "ü"  // U+00FC: LATIN SMALL LETTER U WITH DIAERESIS
       + "ũ"  // U+0169: LATIN SMALL LETTER U WITH TILDE
       + "ū"  // U+016B: LATIN SMALL LETTER U WITH MACRON
       + "ŭ"  // U+016D: LATIN SMALL LETTER U WITH BREVE
       + "ů"  // U+016F: LATIN SMALL LETTER U WITH RING ABOVE
       + "ű"  // U+0171: LATIN SMALL LETTER U WITH DOUBLE ACUTE
       + "ų"  // U+0173: LATIN SMALL LETTER U WITH OGONEK
       + "ư"  // U+01B0: LATIN SMALL LETTER U WITH HORN
       + "ǔ"  // U+01D4: LATIN SMALL LETTER U WITH CARON
       + "ǖ"  // U+01D6: LATIN SMALL LETTER U WITH DIAERESIS AND MACRON
       + "ǘ"  // U+01D8: LATIN SMALL LETTER U WITH DIAERESIS AND ACUTE
       + "ǚ"  // U+01DA: LATIN SMALL LETTER U WITH DIAERESIS AND CARON
       + "ǜ"  // U+01DC: LATIN SMALL LETTER U WITH DIAERESIS AND GRAVE
       + "ȕ"  // U+0215: LATIN SMALL LETTER U WITH DOUBLE GRAVE
       + "ȗ"  // U+0217: LATIN SMALL LETTER U WITH INVERTED BREVE
       + "ʉ"  // U+0289: LATIN SMALL LETTER U BAR
       + "ᵤ"  // U+1D64: LATIN SUBSCRIPT SMALL LETTER U
       + "ᶙ"  // U+1D99: LATIN SMALL LETTER U WITH RETROFLEX HOOK
       + "ṳ"  // U+1E73: LATIN SMALL LETTER U WITH DIAERESIS BELOW
       + "ṵ"  // U+1E75: LATIN SMALL LETTER U WITH TILDE BELOW
       + "ṷ"  // U+1E77: LATIN SMALL LETTER U WITH CIRCUMFLEX BELOW
       + "ṹ"  // U+1E79: LATIN SMALL LETTER U WITH TILDE AND ACUTE
       + "ṻ"  // U+1E7B: LATIN SMALL LETTER U WITH MACRON AND DIAERESIS
       + "ụ"  // U+1EE5: LATIN SMALL LETTER U WITH DOT BELOW
       + "ủ"  // U+1EE7: LATIN SMALL LETTER U WITH HOOK ABOVE
       + "ứ"  // U+1EE9: LATIN SMALL LETTER U WITH HORN AND ACUTE
       + "ừ"  // U+1EEB: LATIN SMALL LETTER U WITH HORN AND GRAVE
       + "ử"  // U+1EED: LATIN SMALL LETTER U WITH HORN AND HOOK ABOVE
       + "ữ"  // U+1EEF: LATIN SMALL LETTER U WITH HORN AND TILDE
       + "ự"  // U+1EF1: LATIN SMALL LETTER U WITH HORN AND DOT BELOW
       + "ⓤ"  // U+24E4: CIRCLED LATIN SMALL LETTER U
       + "ｕ"  // U+FF55: FULLWIDTH LATIN SMALL LETTER U
      ,"u", // Folded result

       "⒰"  // U+24B0: PARENTHESIZED LATIN SMALL LETTER U
      ,"(u)", // Folded result

       "ᵫ"  // U+1D6B: LATIN SMALL LETTER UE
      ,"ue", // Folded result

       "Ʋ"  // U+01B2: LATIN CAPITAL LETTER V WITH HOOK
       + "Ʌ"  // U+0245: LATIN CAPITAL LETTER TURNED V
       + "ᴠ"  // U+1D20: LATIN LETTER SMALL CAPITAL V
       + "Ṽ"  // U+1E7C: LATIN CAPITAL LETTER V WITH TILDE
       + "Ṿ"  // U+1E7E: LATIN CAPITAL LETTER V WITH DOT BELOW
       + "Ỽ"  // U+1EFC: LATIN CAPITAL LETTER MIDDLE-WELSH V
       + "Ⓥ"  // U+24CB: CIRCLED LATIN CAPITAL LETTER V
       + "Ꝟ"  // U+A75E: LATIN CAPITAL LETTER V WITH DIAGONAL STROKE
       + "Ꝩ"  // U+A768: LATIN CAPITAL LETTER VEND
       + "Ｖ"  // U+FF36: FULLWIDTH LATIN CAPITAL LETTER V
      ,"V", // Folded result

       "ʋ"  // U+028B: LATIN SMALL LETTER V WITH HOOK
       + "ʌ"  // U+028C: LATIN SMALL LETTER TURNED V
       + "ᵥ"  // U+1D65: LATIN SUBSCRIPT SMALL LETTER V
       + "ᶌ"  // U+1D8C: LATIN SMALL LETTER V WITH PALATAL HOOK
       + "ṽ"  // U+1E7D: LATIN SMALL LETTER V WITH TILDE
       + "ṿ"  // U+1E7F: LATIN SMALL LETTER V WITH DOT BELOW
       + "ⓥ"  // U+24E5: CIRCLED LATIN SMALL LETTER V
       + "ⱱ"  // U+2C71: LATIN SMALL LETTER V WITH RIGHT HOOK
       + "ⱴ"  // U+2C74: LATIN SMALL LETTER V WITH CURL
       + "ꝟ"  // U+A75F: LATIN SMALL LETTER V WITH DIAGONAL STROKE
       + "ｖ"  // U+FF56: FULLWIDTH LATIN SMALL LETTER V
      ,"v", // Folded result

       "Ꝡ"  // U+A760: LATIN CAPITAL LETTER VY
      ,"VY", // Folded result

       "⒱"  // U+24B1: PARENTHESIZED LATIN SMALL LETTER V
      ,"(v)", // Folded result

       "ꝡ"  // U+A761: LATIN SMALL LETTER VY
      ,"vy", // Folded result

       "Ŵ"  // U+0174: LATIN CAPITAL LETTER W WITH CIRCUMFLEX
       + "Ƿ"  // U+01F7: LATIN CAPITAL LETTER WYNN
       + "ᴡ"  // U+1D21: LATIN LETTER SMALL CAPITAL W
       + "Ẁ"  // U+1E80: LATIN CAPITAL LETTER W WITH GRAVE
       + "Ẃ"  // U+1E82: LATIN CAPITAL LETTER W WITH ACUTE
       + "Ẅ"  // U+1E84: LATIN CAPITAL LETTER W WITH DIAERESIS
       + "Ẇ"  // U+1E86: LATIN CAPITAL LETTER W WITH DOT ABOVE
       + "Ẉ"  // U+1E88: LATIN CAPITAL LETTER W WITH DOT BELOW
       + "Ⓦ"  // U+24CC: CIRCLED LATIN CAPITAL LETTER W
       + "Ⱳ"  // U+2C72: LATIN CAPITAL LETTER W WITH HOOK
       + "Ｗ"  // U+FF37: FULLWIDTH LATIN CAPITAL LETTER W
      ,"W", // Folded result

       "ŵ"  // U+0175: LATIN SMALL LETTER W WITH CIRCUMFLEX
       + "ƿ"  // U+01BF: LATIN LETTER WYNN
       + "ʍ"  // U+028D: LATIN SMALL LETTER TURNED W
       + "ẁ"  // U+1E81: LATIN SMALL LETTER W WITH GRAVE
       + "ẃ"  // U+1E83: LATIN SMALL LETTER W WITH ACUTE
       + "ẅ"  // U+1E85: LATIN SMALL LETTER W WITH DIAERESIS
       + "ẇ"  // U+1E87: LATIN SMALL LETTER W WITH DOT ABOVE
       + "ẉ"  // U+1E89: LATIN SMALL LETTER W WITH DOT BELOW
       + "ẘ"  // U+1E98: LATIN SMALL LETTER W WITH RING ABOVE
       + "ⓦ"  // U+24E6: CIRCLED LATIN SMALL LETTER W
       + "ⱳ"  // U+2C73: LATIN SMALL LETTER W WITH HOOK
       + "ｗ"  // U+FF57: FULLWIDTH LATIN SMALL LETTER W
      ,"w", // Folded result

       "⒲"  // U+24B2: PARENTHESIZED LATIN SMALL LETTER W
      ,"(w)", // Folded result

       "Ẋ"  // U+1E8A: LATIN CAPITAL LETTER X WITH DOT ABOVE
       + "Ẍ"  // U+1E8C: LATIN CAPITAL LETTER X WITH DIAERESIS
       + "Ⓧ"  // U+24CD: CIRCLED LATIN CAPITAL LETTER X
       + "Ｘ"  // U+FF38: FULLWIDTH LATIN CAPITAL LETTER X
      ,"X", // Folded result

       "ᶍ"  // U+1D8D: LATIN SMALL LETTER X WITH PALATAL HOOK
       + "ẋ"  // U+1E8B: LATIN SMALL LETTER X WITH DOT ABOVE
       + "ẍ"  // U+1E8D: LATIN SMALL LETTER X WITH DIAERESIS
       + "ₓ"  // U+2093: LATIN SUBSCRIPT SMALL LETTER X
       + "ⓧ"  // U+24E7: CIRCLED LATIN SMALL LETTER X
       + "ｘ"  // U+FF58: FULLWIDTH LATIN SMALL LETTER X
      ,"x", // Folded result

       "⒳"  // U+24B3: PARENTHESIZED LATIN SMALL LETTER X
      ,"(x)", // Folded result

       "Ý"  // U+00DD: LATIN CAPITAL LETTER Y WITH ACUTE
       + "Ŷ"  // U+0176: LATIN CAPITAL LETTER Y WITH CIRCUMFLEX
       + "Ÿ"  // U+0178: LATIN CAPITAL LETTER Y WITH DIAERESIS
       + "Ƴ"  // U+01B3: LATIN CAPITAL LETTER Y WITH HOOK
       + "Ȳ"  // U+0232: LATIN CAPITAL LETTER Y WITH MACRON
       + "Ɏ"  // U+024E: LATIN CAPITAL LETTER Y WITH STROKE
       + "ʏ"  // U+028F: LATIN LETTER SMALL CAPITAL Y
       + "Ẏ"  // U+1E8E: LATIN CAPITAL LETTER Y WITH DOT ABOVE
       + "Ỳ"  // U+1EF2: LATIN CAPITAL LETTER Y WITH GRAVE
       + "Ỵ"  // U+1EF4: LATIN CAPITAL LETTER Y WITH DOT BELOW
       + "Ỷ"  // U+1EF6: LATIN CAPITAL LETTER Y WITH HOOK ABOVE
       + "Ỹ"  // U+1EF8: LATIN CAPITAL LETTER Y WITH TILDE
       + "Ỿ"  // U+1EFE: LATIN CAPITAL LETTER Y WITH LOOP
       + "Ⓨ"  // U+24CE: CIRCLED LATIN CAPITAL LETTER Y
       + "Ｙ"  // U+FF39: FULLWIDTH LATIN CAPITAL LETTER Y
      ,"Y", // Folded result

       "ý"  // U+00FD: LATIN SMALL LETTER Y WITH ACUTE
       + "ÿ"  // U+00FF: LATIN SMALL LETTER Y WITH DIAERESIS
       + "ŷ"  // U+0177: LATIN SMALL LETTER Y WITH CIRCUMFLEX
       + "ƴ"  // U+01B4: LATIN SMALL LETTER Y WITH HOOK
       + "ȳ"  // U+0233: LATIN SMALL LETTER Y WITH MACRON
       + "ɏ"  // U+024F: LATIN SMALL LETTER Y WITH STROKE
       + "ʎ"  // U+028E: LATIN SMALL LETTER TURNED Y
       + "ẏ"  // U+1E8F: LATIN SMALL LETTER Y WITH DOT ABOVE
       + "ẙ"  // U+1E99: LATIN SMALL LETTER Y WITH RING ABOVE
       + "ỳ"  // U+1EF3: LATIN SMALL LETTER Y WITH GRAVE
       + "ỵ"  // U+1EF5: LATIN SMALL LETTER Y WITH DOT BELOW
       + "ỷ"  // U+1EF7: LATIN SMALL LETTER Y WITH HOOK ABOVE
       + "ỹ"  // U+1EF9: LATIN SMALL LETTER Y WITH TILDE
       + "ỿ"  // U+1EFF: LATIN SMALL LETTER Y WITH LOOP
       + "ⓨ"  // U+24E8: CIRCLED LATIN SMALL LETTER Y
       + "ｙ"  // U+FF59: FULLWIDTH LATIN SMALL LETTER Y
      ,"y", // Folded result

       "⒴"  // U+24B4: PARENTHESIZED LATIN SMALL LETTER Y
      ,"(y)", // Folded result

       "Ź"  // U+0179: LATIN CAPITAL LETTER Z WITH ACUTE
       + "Ż"  // U+017B: LATIN CAPITAL LETTER Z WITH DOT ABOVE
       + "Ž"  // U+017D: LATIN CAPITAL LETTER Z WITH CARON
       + "Ƶ"  // U+01B5: LATIN CAPITAL LETTER Z WITH STROKE
       + "Ȝ"  // U+021C: LATIN CAPITAL LETTER YOGH
       + "Ȥ"  // U+0224: LATIN CAPITAL LETTER Z WITH HOOK
       + "ᴢ"  // U+1D22: LATIN LETTER SMALL CAPITAL Z
       + "Ẑ"  // U+1E90: LATIN CAPITAL LETTER Z WITH CIRCUMFLEX
       + "Ẓ"  // U+1E92: LATIN CAPITAL LETTER Z WITH DOT BELOW
       + "Ẕ"  // U+1E94: LATIN CAPITAL LETTER Z WITH LINE BELOW
       + "Ⓩ"  // U+24CF: CIRCLED LATIN CAPITAL LETTER Z
       + "Ⱬ"  // U+2C6B: LATIN CAPITAL LETTER Z WITH DESCENDER
       + "Ꝣ"  // U+A762: LATIN CAPITAL LETTER VISIGOTHIC Z
       + "Ｚ"  // U+FF3A: FULLWIDTH LATIN CAPITAL LETTER Z
      ,"Z", // Folded result

       "ź"  // U+017A: LATIN SMALL LETTER Z WITH ACUTE
       + "ż"  // U+017C: LATIN SMALL LETTER Z WITH DOT ABOVE
       + "ž"  // U+017E: LATIN SMALL LETTER Z WITH CARON
       + "ƶ"  // U+01B6: LATIN SMALL LETTER Z WITH STROKE
       + "ȝ"  // U+021D: LATIN SMALL LETTER YOGH
       + "ȥ"  // U+0225: LATIN SMALL LETTER Z WITH HOOK
       + "ɀ"  // U+0240: LATIN SMALL LETTER Z WITH SWASH TAIL
       + "ʐ"  // U+0290: LATIN SMALL LETTER Z WITH RETROFLEX HOOK
       + "ʑ"  // U+0291: LATIN SMALL LETTER Z WITH CURL
       + "ᵶ"  // U+1D76: LATIN SMALL LETTER Z WITH MIDDLE TILDE
       + "ᶎ"  // U+1D8E: LATIN SMALL LETTER Z WITH PALATAL HOOK
       + "ẑ"  // U+1E91: LATIN SMALL LETTER Z WITH CIRCUMFLEX
       + "ẓ"  // U+1E93: LATIN SMALL LETTER Z WITH DOT BELOW
       + "ẕ"  // U+1E95: LATIN SMALL LETTER Z WITH LINE BELOW
       + "ⓩ"  // U+24E9: CIRCLED LATIN SMALL LETTER Z
       + "ⱬ"  // U+2C6C: LATIN SMALL LETTER Z WITH DESCENDER
       + "ꝣ"  // U+A763: LATIN SMALL LETTER VISIGOTHIC Z
       + "ｚ"  // U+FF5A: FULLWIDTH LATIN SMALL LETTER Z
      ,"z", // Folded result

       "⒵"  // U+24B5: PARENTHESIZED LATIN SMALL LETTER Z
      ,"(z)", // Folded result

       "⁰"  // U+2070: SUPERSCRIPT ZERO
       + "₀"  // U+2080: SUBSCRIPT ZERO
       + "⓪"  // U+24EA: CIRCLED DIGIT ZERO
       + "⓿"  // U+24FF: NEGATIVE CIRCLED DIGIT ZERO
       + "０"  // U+FF10: FULLWIDTH DIGIT ZERO
      ,"0", // Folded result

       "¹"  // U+00B9: SUPERSCRIPT ONE
       + "₁"  // U+2081: SUBSCRIPT ONE
       + "①"  // U+2460: CIRCLED DIGIT ONE
       + "⓵"  // U+24F5: DOUBLE CIRCLED DIGIT ONE
       + "❶"  // U+2776: DINGBAT NEGATIVE CIRCLED DIGIT ONE
       + "➀"  // U+2780: DINGBAT CIRCLED SANS-SERIF DIGIT ONE
       + "➊"  // U+278A: DINGBAT NEGATIVE CIRCLED SANS-SERIF DIGIT ONE
       + "１"  // U+FF11: FULLWIDTH DIGIT ONE
      ,"1", // Folded result

       "⒈"  // U+2488: DIGIT ONE FULL STOP
      ,"1.", // Folded result

       "⑴"  // U+2474: PARENTHESIZED DIGIT ONE
      ,"(1)", // Folded result

       "²"  // U+00B2: SUPERSCRIPT TWO
       + "₂"  // U+2082: SUBSCRIPT TWO
       + "②"  // U+2461: CIRCLED DIGIT TWO
       + "⓶"  // U+24F6: DOUBLE CIRCLED DIGIT TWO
       + "❷"  // U+2777: DINGBAT NEGATIVE CIRCLED DIGIT TWO
       + "➁"  // U+2781: DINGBAT CIRCLED SANS-SERIF DIGIT TWO
       + "➋"  // U+278B: DINGBAT NEGATIVE CIRCLED SANS-SERIF DIGIT TWO
       + "２"  // U+FF12: FULLWIDTH DIGIT TWO
      ,"2", // Folded result

       "⒉"  // U+2489: DIGIT TWO FULL STOP
      ,"2.", // Folded result

       "⑵"  // U+2475: PARENTHESIZED DIGIT TWO
      ,"(2)", // Folded result

       "³"  // U+00B3: SUPERSCRIPT THREE
       + "₃"  // U+2083: SUBSCRIPT THREE
       + "③"  // U+2462: CIRCLED DIGIT THREE
       + "⓷"  // U+24F7: DOUBLE CIRCLED DIGIT THREE
       + "❸"  // U+2778: DINGBAT NEGATIVE CIRCLED DIGIT THREE
       + "➂"  // U+2782: DINGBAT CIRCLED SANS-SERIF DIGIT THREE
       + "➌"  // U+278C: DINGBAT NEGATIVE CIRCLED SANS-SERIF DIGIT THREE
       + "３"  // U+FF13: FULLWIDTH DIGIT THREE
      ,"3", // Folded result

       "⒊"  // U+248A: DIGIT THREE FULL STOP
      ,"3.", // Folded result

       "⑶"  // U+2476: PARENTHESIZED DIGIT THREE
      ,"(3)", // Folded result

       "⁴"  // U+2074: SUPERSCRIPT FOUR
       + "₄"  // U+2084: SUBSCRIPT FOUR
       + "④"  // U+2463: CIRCLED DIGIT FOUR
       + "⓸"  // U+24F8: DOUBLE CIRCLED DIGIT FOUR
       + "❹"  // U+2779: DINGBAT NEGATIVE CIRCLED DIGIT FOUR
       + "➃"  // U+2783: DINGBAT CIRCLED SANS-SERIF DIGIT FOUR
       + "➍"  // U+278D: DINGBAT NEGATIVE CIRCLED SANS-SERIF DIGIT FOUR
       + "４"  // U+FF14: FULLWIDTH DIGIT FOUR
      ,"4", // Folded result

       "⒋"  // U+248B: DIGIT FOUR FULL STOP
      ,"4.", // Folded result

       "⑷"  // U+2477: PARENTHESIZED DIGIT FOUR
      ,"(4)", // Folded result

       "⁵"  // U+2075: SUPERSCRIPT FIVE
       + "₅"  // U+2085: SUBSCRIPT FIVE
       + "⑤"  // U+2464: CIRCLED DIGIT FIVE
       + "⓹"  // U+24F9: DOUBLE CIRCLED DIGIT FIVE
       + "❺"  // U+277A: DINGBAT NEGATIVE CIRCLED DIGIT FIVE
       + "➄"  // U+2784: DINGBAT CIRCLED SANS-SERIF DIGIT FIVE
       + "➎"  // U+278E: DINGBAT NEGATIVE CIRCLED SANS-SERIF DIGIT FIVE
       + "５"  // U+FF15: FULLWIDTH DIGIT FIVE
      ,"5", // Folded result

       "⒌"  // U+248C: DIGIT FIVE FULL STOP
      ,"5.", // Folded result

       "⑸"  // U+2478: PARENTHESIZED DIGIT FIVE
      ,"(5)", // Folded result

       "⁶"  // U+2076: SUPERSCRIPT SIX
       + "₆"  // U+2086: SUBSCRIPT SIX
       + "⑥"  // U+2465: CIRCLED DIGIT SIX
       + "⓺"  // U+24FA: DOUBLE CIRCLED DIGIT SIX
       + "❻"  // U+277B: DINGBAT NEGATIVE CIRCLED DIGIT SIX
       + "➅"  // U+2785: DINGBAT CIRCLED SANS-SERIF DIGIT SIX
       + "➏"  // U+278F: DINGBAT NEGATIVE CIRCLED SANS-SERIF DIGIT SIX
       + "６"  // U+FF16: FULLWIDTH DIGIT SIX
      ,"6", // Folded result

       "⒍"  // U+248D: DIGIT SIX FULL STOP
      ,"6.", // Folded result

       "⑹"  // U+2479: PARENTHESIZED DIGIT SIX
      ,"(6)", // Folded result

       "⁷"  // U+2077: SUPERSCRIPT SEVEN
       + "₇"  // U+2087: SUBSCRIPT SEVEN
       + "⑦"  // U+2466: CIRCLED DIGIT SEVEN
       + "⓻"  // U+24FB: DOUBLE CIRCLED DIGIT SEVEN
       + "❼"  // U+277C: DINGBAT NEGATIVE CIRCLED DIGIT SEVEN
       + "➆"  // U+2786: DINGBAT CIRCLED SANS-SERIF DIGIT SEVEN
       + "➐"  // U+2790: DINGBAT NEGATIVE CIRCLED SANS-SERIF DIGIT SEVEN
       + "７"  // U+FF17: FULLWIDTH DIGIT SEVEN
      ,"7", // Folded result

       "⒎"  // U+248E: DIGIT SEVEN FULL STOP
      ,"7.", // Folded result

       "⑺"  // U+247A: PARENTHESIZED DIGIT SEVEN
      ,"(7)", // Folded result

       "⁸"  // U+2078: SUPERSCRIPT EIGHT
       + "₈"  // U+2088: SUBSCRIPT EIGHT
       + "⑧"  // U+2467: CIRCLED DIGIT EIGHT
       + "⓼"  // U+24FC: DOUBLE CIRCLED DIGIT EIGHT
       + "❽"  // U+277D: DINGBAT NEGATIVE CIRCLED DIGIT EIGHT
       + "➇"  // U+2787: DINGBAT CIRCLED SANS-SERIF DIGIT EIGHT
       + "➑"  // U+2791: DINGBAT NEGATIVE CIRCLED SANS-SERIF DIGIT EIGHT
       + "８"  // U+FF18: FULLWIDTH DIGIT EIGHT
      ,"8", // Folded result

       "⒏"  // U+248F: DIGIT EIGHT FULL STOP
      ,"8.", // Folded result

       "⑻"  // U+247B: PARENTHESIZED DIGIT EIGHT
      ,"(8)", // Folded result

       "⁹"  // U+2079: SUPERSCRIPT NINE
       + "₉"  // U+2089: SUBSCRIPT NINE
       + "⑨"  // U+2468: CIRCLED DIGIT NINE
       + "⓽"  // U+24FD: DOUBLE CIRCLED DIGIT NINE
       + "❾"  // U+277E: DINGBAT NEGATIVE CIRCLED DIGIT NINE
       + "➈"  // U+2788: DINGBAT CIRCLED SANS-SERIF DIGIT NINE
       + "➒"  // U+2792: DINGBAT NEGATIVE CIRCLED SANS-SERIF DIGIT NINE
       + "９"  // U+FF19: FULLWIDTH DIGIT NINE
      ,"9", // Folded result

       "⒐"  // U+2490: DIGIT NINE FULL STOP
      ,"9.", // Folded result

       "⑼"  // U+247C: PARENTHESIZED DIGIT NINE
      ,"(9)", // Folded result

       "⑩"  // U+2469: CIRCLED NUMBER TEN
       + "⓾"  // U+24FE: DOUBLE CIRCLED NUMBER TEN
       + "❿"  // U+277F: DINGBAT NEGATIVE CIRCLED NUMBER TEN
       + "➉"  // U+2789: DINGBAT CIRCLED SANS-SERIF NUMBER TEN
       + "➓"  // U+2793: DINGBAT NEGATIVE CIRCLED SANS-SERIF NUMBER TEN
      ,"10", // Folded result

       "⒑"  // U+2491: NUMBER TEN FULL STOP
      ,"10.", // Folded result

       "⑽"  // U+247D: PARENTHESIZED NUMBER TEN
      ,"(10)", // Folded result

       "⑪"  // U+246A: CIRCLED NUMBER ELEVEN
       + "⓫"  // U+24EB: NEGATIVE CIRCLED NUMBER ELEVEN
      ,"11", // Folded result

       "⒒"  // U+2492: NUMBER ELEVEN FULL STOP
      ,"11.", // Folded result

       "⑾"  // U+247E: PARENTHESIZED NUMBER ELEVEN
      ,"(11)", // Folded result

       "⑫"  // U+246B: CIRCLED NUMBER TWELVE
       + "⓬"  // U+24EC: NEGATIVE CIRCLED NUMBER TWELVE
      ,"12", // Folded result

       "⒓"  // U+2493: NUMBER TWELVE FULL STOP
      ,"12.", // Folded result

       "⑿"  // U+247F: PARENTHESIZED NUMBER TWELVE
      ,"(12)", // Folded result

       "⑬"  // U+246C: CIRCLED NUMBER THIRTEEN
       + "⓭"  // U+24ED: NEGATIVE CIRCLED NUMBER THIRTEEN
      ,"13", // Folded result

       "⒔"  // U+2494: NUMBER THIRTEEN FULL STOP
      ,"13.", // Folded result

       "⒀"  // U+2480: PARENTHESIZED NUMBER THIRTEEN
      ,"(13)", // Folded result

       "⑭"  // U+246D: CIRCLED NUMBER FOURTEEN
       + "⓮"  // U+24EE: NEGATIVE CIRCLED NUMBER FOURTEEN
      ,"14", // Folded result

       "⒕"  // U+2495: NUMBER FOURTEEN FULL STOP
      ,"14.", // Folded result

       "⒁"  // U+2481: PARENTHESIZED NUMBER FOURTEEN
      ,"(14)", // Folded result

       "⑮"  // U+246E: CIRCLED NUMBER FIFTEEN
       + "⓯"  // U+24EF: NEGATIVE CIRCLED NUMBER FIFTEEN
      ,"15", // Folded result

       "⒖"  // U+2496: NUMBER FIFTEEN FULL STOP
      ,"15.", // Folded result

       "⒂"  // U+2482: PARENTHESIZED NUMBER FIFTEEN
      ,"(15)", // Folded result

       "⑯"  // U+246F: CIRCLED NUMBER SIXTEEN
       + "⓰"  // U+24F0: NEGATIVE CIRCLED NUMBER SIXTEEN
      ,"16", // Folded result

       "⒗"  // U+2497: NUMBER SIXTEEN FULL STOP
      ,"16.", // Folded result

       "⒃"  // U+2483: PARENTHESIZED NUMBER SIXTEEN
      ,"(16)", // Folded result

       "⑰"  // U+2470: CIRCLED NUMBER SEVENTEEN
       + "⓱"  // U+24F1: NEGATIVE CIRCLED NUMBER SEVENTEEN
      ,"17", // Folded result

       "⒘"  // U+2498: NUMBER SEVENTEEN FULL STOP
      ,"17.", // Folded result

       "⒄"  // U+2484: PARENTHESIZED NUMBER SEVENTEEN
      ,"(17)", // Folded result

       "⑱"  // U+2471: CIRCLED NUMBER EIGHTEEN
       + "⓲"  // U+24F2: NEGATIVE CIRCLED NUMBER EIGHTEEN
      ,"18", // Folded result

       "⒙"  // U+2499: NUMBER EIGHTEEN FULL STOP
      ,"18.", // Folded result

       "⒅"  // U+2485: PARENTHESIZED NUMBER EIGHTEEN
      ,"(18)", // Folded result

       "⑲"  // U+2472: CIRCLED NUMBER NINETEEN
       + "⓳"  // U+24F3: NEGATIVE CIRCLED NUMBER NINETEEN
      ,"19", // Folded result

       "⒚"  // U+249A: NUMBER NINETEEN FULL STOP
      ,"19.", // Folded result

       "⒆"  // U+2486: PARENTHESIZED NUMBER NINETEEN
      ,"(19)", // Folded result

       "⑳"  // U+2473: CIRCLED NUMBER TWENTY
       + "⓴"  // U+24F4: NEGATIVE CIRCLED NUMBER TWENTY
      ,"20", // Folded result

       "⒛"  // U+249B: NUMBER TWENTY FULL STOP
      ,"20.", // Folded result

       "⒇"  // U+2487: PARENTHESIZED NUMBER TWENTY
      ,"(20)", // Folded result

       "«"  // U+00AB: LEFT-POINTING DOUBLE ANGLE QUOTATION MARK
       + "»"  // U+00BB: RIGHT-POINTING DOUBLE ANGLE QUOTATION MARK
       + "“"  // U+201C: LEFT DOUBLE QUOTATION MARK
       + "”"  // U+201D: RIGHT DOUBLE QUOTATION MARK
       + "„"  // U+201E: DOUBLE LOW-9 QUOTATION MARK
       + "″"  // U+2033: DOUBLE PRIME
       + "‶"  // U+2036: REVERSED DOUBLE PRIME
       + "❝"  // U+275D: HEAVY DOUBLE TURNED COMMA QUOTATION MARK ORNAMENT
       + "❞"  // U+275E: HEAVY DOUBLE COMMA QUOTATION MARK ORNAMENT
       + "❮"  // U+276E: HEAVY LEFT-POINTING ANGLE QUOTATION MARK ORNAMENT
       + "❯"  // U+276F: HEAVY RIGHT-POINTING ANGLE QUOTATION MARK ORNAMENT
       + "＂"  // U+FF02: FULLWIDTH QUOTATION MARK
      ,"\"", // Folded result

       "‘"  // U+2018: LEFT SINGLE QUOTATION MARK
       + "’"  // U+2019: RIGHT SINGLE QUOTATION MARK
       + "‚"  // U+201A: SINGLE LOW-9 QUOTATION MARK
       + "‛"  // U+201B: SINGLE HIGH-REVERSED-9 QUOTATION MARK
       + "′"  // U+2032: PRIME
       + "‵"  // U+2035: REVERSED PRIME
       + "‹"  // U+2039: SINGLE LEFT-POINTING ANGLE QUOTATION MARK
       + "›"  // U+203A: SINGLE RIGHT-POINTING ANGLE QUOTATION MARK
       + "❛"  // U+275B: HEAVY SINGLE TURNED COMMA QUOTATION MARK ORNAMENT
       + "❜"  // U+275C: HEAVY SINGLE COMMA QUOTATION MARK ORNAMENT
       + "＇"  // U+FF07: FULLWIDTH APOSTROPHE
      ,"'", // Folded result

       "‐"  // U+2010: HYPHEN
       + "‑"  // U+2011: NON-BREAKING HYPHEN
       + "‒"  // U+2012: FIGURE DASH
       + "–"  // U+2013: EN DASH
       + "—"  // U+2014: EM DASH
       + "⁻"  // U+207B: SUPERSCRIPT MINUS
       + "₋"  // U+208B: SUBSCRIPT MINUS
       + "－"  // U+FF0D: FULLWIDTH HYPHEN-MINUS
      ,"-", // Folded result

       "⁅"  // U+2045: LEFT SQUARE BRACKET WITH QUILL
       + "❲"  // U+2772: LIGHT LEFT TORTOISE SHELL BRACKET ORNAMENT
       + "［"  // U+FF3B: FULLWIDTH LEFT SQUARE BRACKET
      ,"[", // Folded result

       "⁆"  // U+2046: RIGHT SQUARE BRACKET WITH QUILL
       + "❳"  // U+2773: LIGHT RIGHT TORTOISE SHELL BRACKET ORNAMENT
       + "］"  // U+FF3D: FULLWIDTH RIGHT SQUARE BRACKET
      ,"]", // Folded result

       "⁽"  // U+207D: SUPERSCRIPT LEFT PARENTHESIS
       + "₍"  // U+208D: SUBSCRIPT LEFT PARENTHESIS
       + "❨"  // U+2768: MEDIUM LEFT PARENTHESIS ORNAMENT
       + "❪"  // U+276A: MEDIUM FLATTENED LEFT PARENTHESIS ORNAMENT
       + "（"  // U+FF08: FULLWIDTH LEFT PARENTHESIS
      ,"(", // Folded result

       "⸨"  // U+2E28: LEFT DOUBLE PARENTHESIS
      ,"((", // Folded result

       "⁾"  // U+207E: SUPERSCRIPT RIGHT PARENTHESIS
       + "₎"  // U+208E: SUBSCRIPT RIGHT PARENTHESIS
       + "❩"  // U+2769: MEDIUM RIGHT PARENTHESIS ORNAMENT
       + "❫"  // U+276B: MEDIUM FLATTENED RIGHT PARENTHESIS ORNAMENT
       + "）"  // U+FF09: FULLWIDTH RIGHT PARENTHESIS
      ,")", // Folded result

       "⸩"  // U+2E29: RIGHT DOUBLE PARENTHESIS
      ,"))", // Folded result

       "❬"  // U+276C: MEDIUM LEFT-POINTING ANGLE BRACKET ORNAMENT
       + "❰"  // U+2770: HEAVY LEFT-POINTING ANGLE BRACKET ORNAMENT
       + "＜"  // U+FF1C: FULLWIDTH LESS-THAN SIGN
      ,"<", // Folded result

       "❭"  // U+276D: MEDIUM RIGHT-POINTING ANGLE BRACKET ORNAMENT
       + "❱"  // U+2771: HEAVY RIGHT-POINTING ANGLE BRACKET ORNAMENT
       + "＞"  // U+FF1E: FULLWIDTH GREATER-THAN SIGN
      ,">", // Folded result

       "❴"  // U+2774: MEDIUM LEFT CURLY BRACKET ORNAMENT
       + "｛"  // U+FF5B: FULLWIDTH LEFT CURLY BRACKET
      ,"{", // Folded result

       "❵"  // U+2775: MEDIUM RIGHT CURLY BRACKET ORNAMENT
       + "｝"  // U+FF5D: FULLWIDTH RIGHT CURLY BRACKET
      ,"}", // Folded result

       "⁺"  // U+207A: SUPERSCRIPT PLUS SIGN
       + "₊"  // U+208A: SUBSCRIPT PLUS SIGN
       + "＋"  // U+FF0B: FULLWIDTH PLUS SIGN
      ,"+", // Folded result

       "⁼"  // U+207C: SUPERSCRIPT EQUALS SIGN
       + "₌"  // U+208C: SUBSCRIPT EQUALS SIGN
       + "＝"  // U+FF1D: FULLWIDTH EQUALS SIGN
      ,"=", // Folded result

       "！"  // U+FF01: FULLWIDTH EXCLAMATION MARK
      ,"!", // Folded result

       "‼"  // U+203C: DOUBLE EXCLAMATION MARK
      ,"!!", // Folded result

       "⁉"  // U+2049: EXCLAMATION QUESTION MARK
      ,"!?", // Folded result

       "＃"  // U+FF03: FULLWIDTH NUMBER SIGN
      ,"#", // Folded result

       "＄"  // U+FF04: FULLWIDTH DOLLAR SIGN
      ,"$", // Folded result

       "⁒"  // U+2052: COMMERCIAL MINUS SIGN
       + "％"  // U+FF05: FULLWIDTH PERCENT SIGN
      ,"%", // Folded result

       "＆"  // U+FF06: FULLWIDTH AMPERSAND
      ,"&", // Folded result

       "⁎"  // U+204E: LOW ASTERISK
       + "＊"  // U+FF0A: FULLWIDTH ASTERISK
      ,"*", // Folded result

       "，"  // U+FF0C: FULLWIDTH COMMA
      ,",", // Folded result

       "．"  // U+FF0E: FULLWIDTH FULL STOP
      ,".", // Folded result

       "⁄"  // U+2044: FRACTION SLASH
       + "／"  // U+FF0F: FULLWIDTH SOLIDUS
      ,"/", // Folded result

       "："  // U+FF1A: FULLWIDTH COLON
      ,":", // Folded result

       "⁏"  // U+204F: REVERSED SEMICOLON
       + "；"  // U+FF1B: FULLWIDTH SEMICOLON
      ,";", // Folded result

       "？"  // U+FF1F: FULLWIDTH QUESTION MARK
      ,"?", // Folded result

       "⁇"  // U+2047: DOUBLE QUESTION MARK
      ,"??", // Folded result

       "⁈"  // U+2048: QUESTION EXCLAMATION MARK
      ,"?!", // Folded result

       "＠"  // U+FF20: FULLWIDTH COMMERCIAL AT
      ,"@", // Folded result

       "＼"  // U+FF3C: FULLWIDTH REVERSE SOLIDUS
      ,"\\", // Folded result

       "‸"  // U+2038: CARET
       + "＾"  // U+FF3E: FULLWIDTH CIRCUMFLEX ACCENT
      ,"^", // Folded result

       "＿"  // U+FF3F: FULLWIDTH LOW LINE
      ,"_", // Folded result

       "⁓"  // U+2053: SWUNG DASH
       + "～"  // U+FF5E: FULLWIDTH TILDE
      ,"~", // Folded result
    };

    // Construct input text and expected output tokens
    List<String> expectedUnfoldedTokens = new ArrayList<>();
    List<String> expectedFoldedTokens = new ArrayList<>();
    StringBuilder inputText = new StringBuilder();
    for (int n = 0 ; n < foldings.length ; n += 2) {
      if (n > 0) {
        inputText.append(' ');  // Space between tokens
      }
      inputText.append(foldings[n]);

      // Construct the expected output tokens: both the unfolded and folded string,
      // with the folded duplicated as many times as the number of characters in
      // the input text.
      StringBuilder expected = new StringBuilder();
      int numChars = foldings[n].length();
      for (int m = 0 ; m < numChars; ++m) {
        expected.append(foldings[n + 1]);
      }
      expectedUnfoldedTokens.add(foldings[n]);
      expectedFoldedTokens.add(expected.toString());
    }

    TokenStream stream = whitespaceMockTokenizer(inputText.toString());
    ASCIIFoldingFilter filter = new ASCIIFoldingFilter(stream, random().nextBoolean());
    CharTermAttribute termAtt = filter.getAttribute(CharTermAttribute.class);
    Iterator<String> unfoldedIter = expectedUnfoldedTokens.iterator();
    Iterator<String> foldedIter = expectedFoldedTokens.iterator();
    filter.reset();
    while (foldedIter.hasNext()) {
      assertNextTerms(unfoldedIter.next(), foldedIter.next(), filter, termAtt);
    }
    assertFalse(filter.incrementToken());
  }
  
  /** blast some random strings through the analyzer */
  public void testRandomStrings() throws Exception {
    Analyzer a = new Analyzer() {

      @Override
      protected TokenStreamComponents createComponents(String fieldName) {
        Tokenizer tokenizer = new MockTokenizer(MockTokenizer.WHITESPACE, false);
        return new TokenStreamComponents(tokenizer,
          new ASCIIFoldingFilter(tokenizer, random().nextBoolean()));
      } 
    };
    checkRandomData(random(), a, 200 * RANDOM_MULTIPLIER);
    a.close();
  }
  
  public void testEmptyTerm() throws IOException {
    Analyzer a = new Analyzer() {
      @Override
      protected TokenStreamComponents createComponents(String fieldName) {
        Tokenizer tokenizer = new KeywordTokenizer();
        return new TokenStreamComponents(tokenizer,
          new ASCIIFoldingFilter(tokenizer, random().nextBoolean()));
      }
    };
    checkOneTerm(a, "", "");
    a.close();
  }
}

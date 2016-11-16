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
package org.apache.lucene.analysis.ja;


import java.io.IOException;
import java.math.BigDecimal;

import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.KeywordAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionLengthAttribute;

/**
 * A {@link TokenFilter} that normalizes Japanese numbers (kansūji) to regular Arabic
 * decimal numbers in half-width characters.
 * <p>
 * Japanese numbers are often written using a combination of kanji and Arabic numbers with
 * various kinds punctuation. For example, ３．２千 means 3200. This filter does this kind
 * of normalization and allows a search for 3200 to match ３．２千 in text, but can also be
 * used to make range facets based on the normalized numbers and so on.
 * <p>
 * Notice that this analyzer uses a token composition scheme and relies on punctuation
 * tokens being found in the token stream. Please make sure your {@link JapaneseTokenizer}
 * has {@code discardPunctuation} set to false. In case punctuation characters, such as ．
 * (U+FF0E FULLWIDTH FULL STOP), is removed from the token stream, this filter would find
 * input tokens tokens ３ and ２千 and give outputs 3 and 2000 instead of 3200, which is
 * likely not the intended result. If you want to remove punctuation characters from your
 * index that are not part of normalized numbers, add a
 * {@link org.apache.lucene.analysis.StopFilter} with the punctuation you wish to
 * remove after {@link JapaneseNumberFilter} in your analyzer chain.
 * <p>
 * Below are some examples of normalizations this filter supports. The input is untokenized
 * text and the result is the single term attribute emitted for the input.
 * <ul>
 * <li>〇〇七 becomes 7</li>
 * <li>一〇〇〇 becomes 1000</li>
 * <li>三千2百２十三 becomes 3223</li>
 * <li>兆六百万五千一 becomes 1000006005001</li>
 * <li>３．２千 becomes 3200</li>
 * <li>１．２万３４５．６７ becomes 12345.67</li>
 * <li>4,647.100 becomes 4647.1</li>
 * <li>15,7 becomes 157 (be aware of this weakness)</li>
 * </ul>
 * <p>
 * Tokens preceded by a token with {@link PositionIncrementAttribute} of zero are left
 * left untouched and emitted as-is.
 * <p>
 * This filter does not use any part-of-speech information for its normalization and
 * the motivation for this is to also support n-grammed token streams in the future.
 * <p>
 * This filter may in some cases normalize tokens that are not numbers in their context.
 * For example, is 田中京一 is a name and means Tanaka Kyōichi, but 京一 (Kyōichi) out of
 * context can strictly speaking also represent the number 10000000000000001. This filter
 * respects the {@link KeywordAttribute}, which can be used to prevent specific
 * normalizations from happening.
 * <p>
 * Also notice that token attributes such as
 * {@link org.apache.lucene.analysis.ja.tokenattributes.PartOfSpeechAttribute},
 * {@link org.apache.lucene.analysis.ja.tokenattributes.ReadingAttribute},
 * {@link org.apache.lucene.analysis.ja.tokenattributes.InflectionAttribute} and
 * {@link org.apache.lucene.analysis.ja.tokenattributes.BaseFormAttribute} are left
 * unchanged and will inherit the values of the last token used to compose the normalized
 * number and can be wrong. Hence, for １０万 (10000), we will have
 * {@link org.apache.lucene.analysis.ja.tokenattributes.ReadingAttribute}
 * set to マン. This is a known issue and is subject to a future improvement.
 * <p>
 * Japanese formal numbers (daiji), accounting numbers and decimal fractions are currently
 * not supported.
 */
public class JapaneseNumberFilter extends TokenFilter {

  private final CharTermAttribute termAttr = addAttribute(CharTermAttribute.class);
  private final OffsetAttribute offsetAttr = addAttribute(OffsetAttribute.class);
  private final KeywordAttribute keywordAttr = addAttribute(KeywordAttribute.class);
  private final PositionIncrementAttribute posIncrAttr = addAttribute(PositionIncrementAttribute.class);
  private final PositionLengthAttribute posLengthAttr = addAttribute(PositionLengthAttribute.class);

  private static char NO_NUMERAL = Character.MAX_VALUE;

  private static char[] numerals;

  private static char[] exponents;

  private State state;

  private StringBuilder numeral;

  private int fallThroughTokens;
  
  private boolean exhausted = false;

  static {
    numerals = new char[0x10000];
    for (int i = 0; i < numerals.length; i++) {
      numerals[i] = NO_NUMERAL;
    }
    numerals['〇'] = 0; // 〇 U+3007 0
    numerals['一'] = 1; // 一 U+4E00 1
    numerals['二'] = 2; // 二 U+4E8C 2
    numerals['三'] = 3; // 三 U+4E09 3
    numerals['四'] = 4; // 四 U+56DB 4
    numerals['五'] = 5; // 五 U+4E94 5
    numerals['六'] = 6; // 六 U+516D 6
    numerals['七'] = 7; // 七 U+4E03 7
    numerals['八'] = 8; // 八 U+516B 8
    numerals['九'] = 9; // 九 U+4E5D 9

    exponents = new char[0x10000];
    for (int i = 0; i < exponents.length; i++) {
      exponents[i] = 0;
    }
    exponents['十'] = 1;  // 十 U+5341 10
    exponents['百'] = 2;  // 百 U+767E 100
    exponents['千'] = 3;  // 千 U+5343 1,000
    exponents['万'] = 4;  // 万 U+4E07 10,000
    exponents['億'] = 8;  // 億 U+5104 100,000,000
    exponents['兆'] = 12; // 兆 U+5146 1,000,000,000,000
    exponents['京'] = 16; // 京 U+4EAC 10,000,000,000,000,000
    exponents['垓'] = 20; // 垓 U+5793 100,000,000,000,000,000,000
  }

  public JapaneseNumberFilter(TokenStream input) {
    super(input);
  }

  @Override
  public final boolean incrementToken() throws IOException {

    // Emit previously captured token we read past earlier
    if (state != null) {
      restoreState(state);
      state = null;
      return true;
    }

    if (exhausted) {
      return false;
    }
    
    if (!input.incrementToken()) {
      exhausted = true;
      return false;
    }

    if (keywordAttr.isKeyword()) {
      return true;
    }

    if (fallThroughTokens > 0) {
      fallThroughTokens--;
      return true;
    }

    if (posIncrAttr.getPositionIncrement() == 0) {
      fallThroughTokens = posLengthAttr.getPositionLength() - 1;
      return true;
    }

    boolean moreTokens = true;
    boolean composedNumberToken = false;
    int startOffset = 0;
    int endOffset = 0;
    State preCompositionState = captureState();
    String term = termAttr.toString();
    boolean numeralTerm = isNumeral(term);
    
    while (moreTokens && numeralTerm) {

      if (!composedNumberToken) {
        startOffset = offsetAttr.startOffset();
        composedNumberToken = true;
      }

      endOffset = offsetAttr.endOffset();
      moreTokens = input.incrementToken();
      if (moreTokens == false) {
        exhausted = true;
      }

      if (posIncrAttr.getPositionIncrement() == 0) {
        // This token is a stacked/synonym token, capture number of tokens "under" this token,
        // except the first token, which we will emit below after restoring state
        fallThroughTokens = posLengthAttr.getPositionLength() - 1;
        state = captureState();
        restoreState(preCompositionState);
        return moreTokens;
      }

      numeral.append(term);

      if (moreTokens) {
        term = termAttr.toString();
        numeralTerm = isNumeral(term) || isNumeralPunctuation(term);
      }
    }

    if (composedNumberToken) {
      if (moreTokens) {
        // We have read past all numerals and there are still tokens left, so
        // capture the state of this token and emit it on our next incrementToken()
        state = captureState();
      }

      String normalizedNumber = normalizeNumber(numeral.toString());

      termAttr.setEmpty();
      termAttr.append(normalizedNumber);
      offsetAttr.setOffset(startOffset, endOffset);

      numeral = new StringBuilder();
      return true;
    }
    return moreTokens;
  }

  @Override
  public void reset() throws IOException {
    super.reset();
    fallThroughTokens = 0;
    numeral = new StringBuilder();
    state = null;
    exhausted = false;
  }

  /**
   * Normalizes a Japanese number
   *
   * @param number number or normalize
   * @return normalized number, or number to normalize on error (no op)
   */
  public String normalizeNumber(String number) {
    try {
      BigDecimal normalizedNumber = parseNumber(new NumberBuffer(number));
      if (normalizedNumber == null) {
        return number;
      }
      return normalizedNumber.stripTrailingZeros().toPlainString();
    } catch (NumberFormatException | ArithmeticException e) {
      // Return the source number in case of error, i.e. malformed input
      return number;
    }
  }

  /**
   * Parses a Japanese number
   *
   * @param buffer buffer to parse
   * @return parsed number, or null on error or end of input
   */
  private BigDecimal parseNumber(NumberBuffer buffer) {
    BigDecimal sum = BigDecimal.ZERO;
    BigDecimal result = parseLargePair(buffer);

    if (result == null) {
      return null;
    }

    while (result != null) {
      sum = sum.add(result);
      result = parseLargePair(buffer);
    }

    return sum;
  }

  /**
   * Parses a pair of large numbers, i.e. large kanji factor is 10,000（万）or larger
   *
   * @param buffer buffer to parse
   * @return parsed pair, or null on error or end of input
   */
  private BigDecimal parseLargePair(NumberBuffer buffer) {
    BigDecimal first = parseMediumNumber(buffer);
    BigDecimal second = parseLargeKanjiNumeral(buffer);

    if (first == null && second == null) {
      return null;
    }

    if (second == null) {
      // If there's no second factor, we return the first one
      // This can happen if we our number is smaller than 10,000 (万)
      return first;
    }

    if (first == null) {
      // If there's no first factor, just return the second one,
      // which is the same as multiplying by 1, i.e. with 万
      return second;
    }

    return first.multiply(second);
  }

  /**
   * Parses a "medium sized" number, typically less than 10,000（万）, but might be larger
   * due to a larger factor from {link parseBasicNumber}.
   *
   * @param buffer buffer to parse
   * @return parsed number, or null on error or end of input
   */
  private BigDecimal parseMediumNumber(NumberBuffer buffer) {
    BigDecimal sum = BigDecimal.ZERO;
    BigDecimal result = parseMediumPair(buffer);

    if (result == null) {
      return null;
    }

    while (result != null) {
      sum = sum.add(result);
      result = parseMediumPair(buffer);
    }

    return sum;
  }

  /**
   * Parses a pair of "medium sized" numbers, i.e. large kanji factor is at most 1,000（千）
   *
   * @param buffer buffer to parse
   * @return parsed pair, or null on error or end of input
   */
  private BigDecimal parseMediumPair(NumberBuffer buffer) {

    BigDecimal first = parseBasicNumber(buffer);
    BigDecimal second = parseMediumKanjiNumeral(buffer);

    if (first == null && second == null) {
      return null;
    }

    if (second == null) {
      // If there's no second factor, we return the first one
      // This can happen if we just have a plain number such as 五
      return first;
    }

    if (first == null) {
      // If there's no first factor, just return the second one,
      // which is the same as multiplying by 1, i.e. with 千
      return second;
    }

    // Return factors multiplied
    return first.multiply(second);
  }

  /**
   * Parse a basic number, which is a sequence of Arabic numbers or a sequence or 0-9 kanji numerals (〇 to 九).
   *
   * @param buffer buffer to parse
   * @return parsed number, or null on error or end of input
   */
  private BigDecimal parseBasicNumber(NumberBuffer buffer) {
    StringBuilder builder = new StringBuilder();
    int i = buffer.position();

    while (i < buffer.length()) {
      char c = buffer.charAt(i);

      if (isArabicNumeral(c)) {
        // Arabic numerals; 0 to 9 or ０ to ９ (full-width)
        builder.append(arabicNumeralValue(c));
      } else if (isKanjiNumeral(c)) {
        // Kanji numerals; 〇, 一, 二, 三, 四, 五, 六, 七, 八, or 九
        builder.append(kanjiNumeralValue(c));
      } else if (isDecimalPoint(c)) {
        builder.append(".");
      } else if (isThousandSeparator(c)) {
        // Just skip and move to the next character
      } else {
        // We don't have an Arabic nor kanji numeral, nor separation or punctuation, so we'll stop.
        break;
      }

      i++;
      buffer.advance();
    }

    if (builder.length() == 0) {
      // We didn't build anything, so we don't have a number
      return null;
    }

    return new BigDecimal(builder.toString());
  }

  /**
   * Parse large kanji numerals (ten thousands or larger)
   *
   * @param buffer buffer to parse
   * @return parsed number, or null on error or end of input
   */
  public BigDecimal parseLargeKanjiNumeral(NumberBuffer buffer) {
    int i = buffer.position();

    if (i >= buffer.length()) {
      return null;
    }

    char c = buffer.charAt(i);
    int power = exponents[c];

    if (power > 3) {
      buffer.advance();
      return BigDecimal.TEN.pow(power);
    }

    return null;
  }

  /**
   * Parse medium kanji numerals (tens, hundreds or thousands)
   *
   * @param buffer buffer to parse
   * @return parsed number or null on error
   */
  public BigDecimal parseMediumKanjiNumeral(NumberBuffer buffer) {
    int i = buffer.position();

    if (i >= buffer.length()) {
      return null;
    }

    char c = buffer.charAt(i);
    int power = exponents[c];

    if (1 <= power && power <= 3) {
      buffer.advance();
      return BigDecimal.TEN.pow(power);
    }

    return null;
  }

  /**
   * Numeral predicate
   *
   * @param input string to test
   * @return true if and only if input is a numeral
   */
  public boolean isNumeral(String input) {
    for (int i = 0; i < input.length(); i++) {
      if (!isNumeral(input.charAt(i))) {
        return false;
      }
    }
    return true;
  }

  /**
   * Numeral predicate
   *
   * @param c character to test
   * @return true if and only if c is a numeral
   */
  public boolean isNumeral(char c) {
    return isArabicNumeral(c) || isKanjiNumeral(c) || exponents[c] > 0;
  }

  /**
   * Numeral punctuation predicate
   *
   * @param input string to test
   * @return true if and only if c is a numeral punctuation string
   */
  public boolean isNumeralPunctuation(String input) {
    for (int i = 0; i < input.length(); i++) {
      if (!isNumeralPunctuation(input.charAt(i))) {
        return false;
      }
    }
    return true;
  }

  /**
   * Numeral punctuation predicate
   *
   * @param c character to test
   * @return true if and only if c is a numeral punctuation character
   */
  public boolean isNumeralPunctuation(char c) {
    return isDecimalPoint(c) || isThousandSeparator(c);
  }

  /**
   * Arabic numeral predicate. Both half-width and full-width characters are supported
   *
   * @param c character to test
   * @return true if and only if c is an Arabic numeral
   */
  public boolean isArabicNumeral(char c) {
    return isHalfWidthArabicNumeral(c) || isFullWidthArabicNumeral(c);
  }

  /**
   * Arabic half-width numeral predicate
   *
   * @param c character to test
   * @return true if and only if c is a half-width Arabic numeral
   */
  private boolean isHalfWidthArabicNumeral(char c) {
    // 0 U+0030 - 9 U+0039
    return '0' <= c && c <= '9';
  }

  /**
   * Arabic full-width numeral predicate
   *
   * @param c character to test
   * @return true if and only if c is a full-width Arabic numeral
   */
  private boolean isFullWidthArabicNumeral(char c) {
    // ０ U+FF10 - ９ U+FF19
    return '０' <= c && c <= '９';
  }

  /**
   * Returns the numeric value for the specified character Arabic numeral.
   * Behavior is undefined if a non-Arabic numeral is provided
   *
   * @param c arabic numeral character
   * @return numeral value
   */
  private int arabicNumeralValue(char c) {
    int offset;
    if (isHalfWidthArabicNumeral(c)) {
      offset = '0';
    } else {
      offset = '０';
    }
    return c - offset;
  }

  /**
   * Kanji numeral predicate that tests if the provided character is one of 〇, 一, 二, 三, 四, 五, 六, 七, 八, or 九.
   * Larger number kanji gives a false value.
   *
   * @param c character to test
   * @return true if and only is character is one of 〇, 一, 二, 三, 四, 五, 六, 七, 八, or 九 (0 to 9)
   */
  private boolean isKanjiNumeral(char c) {
    return numerals[c] != NO_NUMERAL;
  }

  /**
   * Returns the value for the provided kanji numeral. Only numeric values for the characters where
   * {link isKanjiNumeral} return true are supported - behavior is undefined for other characters.
   *
   * @param c kanji numeral character
   * @return numeral value
   * @see #isKanjiNumeral(char)
   */
  private int kanjiNumeralValue(char c) {
    return numerals[c];
  }

  /**
   * Decimal point predicate
   *
   * @param c character to test
   * @return true if and only if c is a decimal point
   */
  private boolean isDecimalPoint(char c) {
    return c == '.'   // U+002E FULL STOP 
        || c == '．'; // U+FF0E FULLWIDTH FULL STOP
  }

  /**
   * Thousand separator predicate
   *
   * @param c character to test
   * @return true if and only if c is a thousand separator predicate
   */
  private boolean isThousandSeparator(char c) {
    return c == ','   // U+002C COMMA
        || c == '，'; // U+FF0C FULLWIDTH COMMA
  }

  /**
   * Buffer that holds a Japanese number string and a position index used as a parsed-to marker
   */
  public static class NumberBuffer {

    private int position;

    private String string;

    public NumberBuffer(String string) {
      this.string = string;
      this.position = 0;
    }

    public char charAt(int index) {
      return string.charAt(index);
    }

    public int length() {
      return string.length();
    }

    public void advance() {
      position++;
    }

    public int position() {
      return position;
    }
  }
}

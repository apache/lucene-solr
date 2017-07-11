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
package org.apache.solr.spelling;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PayloadAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.TypeAttribute;


/**
 * Converts the query string to a Collection of Lucene tokens using a regular expression.
 * Boolean operators AND, OR, NOT are skipped. 
 * 
 * Each term is checked to determine if it is optional, required or prohibited.  Required
 * terms output a {@link Token} with the {@link QueryConverter#REQUIRED_TERM_FLAG} set.
 * Prohibited terms output a {@link Token} with the {@link QueryConverter#PROHIBITED_TERM_FLAG} 
 * set. If the query uses the plus (+) and minus (-) to denote required and prohibited, this
 * determination will be accurate.  In the case boolean AND/OR/NOTs are used, this
 * converter makes an uninformed guess as to whether the term would likely behave as if it
 * is Required or Prohibited and sets the flags accordingly.  These flags are used downstream
 * to generate collations for {@link WordBreakSolrSpellChecker}, in cases where an original 
 * term is split up into multiple Tokens.
 * 
 * @since solr 1.3
 **/
public class SpellingQueryConverter extends QueryConverter  {

  /*
  * The following builds up a regular expression that matches productions
  * of the syntax for NMTOKEN as per the W3C XML Recommendation - with one
  * important exception (see below).
  *
  * http://www.w3.org/TR/2008/REC-xml-20081126/ - version used as reference
  *
  * http://www.w3.org/TR/REC-xml/#NT-Nmtoken
  *
  * An NMTOKEN is a series of one or more NAMECHAR characters, which is an
  * extension of the NAMESTARTCHAR character class.
  *
  * The EXCEPTION referred to above concerns the colon, which is legal in an
  * NMTOKEN, but cannot currently be used as a valid field name within Solr,
  * as it is used to delimit the field name from the query string.
  */

  final static String[] NAMESTARTCHAR_PARTS = {
          "A-Z_a-z", "\\xc0-\\xd6", "\\xd8-\\xf6", "\\xf8-\\u02ff",
          "\\u0370-\\u037d", "\\u037f-\\u1fff",
          "\\u200c-\\u200d", "\\u2070-\\u218f",
          "\\u2c00-\\u2fef", "\\u2001-\\ud7ff",
          "\\uf900-\\ufdcf", "\\ufdf0-\\ufffd"
  };
  final static String[] ADDITIONAL_NAMECHAR_PARTS = {
          "\\-.0-9\\xb7", "\\u0300-\\u036f", "\\u203f-\\u2040"
  };
  final static String SURROGATE_PAIR = "\\p{Cs}{2}";
  final static String NMTOKEN;

  static {
    StringBuilder sb = new StringBuilder();
    for (String part : NAMESTARTCHAR_PARTS)
      sb.append(part);
    for (String part : ADDITIONAL_NAMECHAR_PARTS)
      sb.append(part);
    NMTOKEN = "([" + sb.toString() + "]|" + SURROGATE_PAIR + ")+";
  }

  final static String PATTERN = "(?:(?!(" + NMTOKEN + ":|[\\^.]\\d+)))[^^.:(\\s][\\p{L}_\\-0-9]+";
  // previous version: Pattern.compile("(?:(?!(\\w+:|\\d+)))\\w+");
  protected Pattern QUERY_REGEX = Pattern.compile(PATTERN);
  
  /**
   * Converts the original query string to a collection of Lucene Tokens.
   * @param original the original query string
   * @return a Collection of Lucene Tokens
   */
  @Override
  public Collection<Token> convert(String original) {
    if (original == null) { // this can happen with q.alt = and no query
      return Collections.emptyList();
    }
    boolean mightContainRangeQuery = (original.indexOf('[') != -1 || original.indexOf('{') != -1)
        && (original.indexOf(']') != -1 || original.indexOf('}') != -1);
    Collection<Token> result = new ArrayList<>();
    Matcher matcher = QUERY_REGEX.matcher(original);
    String nextWord = null;
    int nextStartIndex = 0;
    String lastBooleanOp = null;
    while (nextWord!=null || matcher.find()) {
      String word = null;
      int startIndex = 0;
      if(nextWord != null) {
        word = nextWord;
        startIndex = nextStartIndex;
        nextWord = null;
      } else {
        word = matcher.group(0);
        startIndex = matcher.start();
      }
      if(matcher.find()) {
        nextWord = matcher.group(0);
        nextStartIndex = matcher.start();
      }  
      if(mightContainRangeQuery && "TO".equals(word)) {
        continue;
      }
      if("AND".equals(word) || "OR".equals(word) || "NOT".equals(word)) {
        lastBooleanOp = word;        
        continue;
      }
      // treat "AND NOT" as "NOT"...
      if ("AND".equals(nextWord)
          && original.length() > nextStartIndex + 7
          && original.substring(nextStartIndex, nextStartIndex + 7).equals(
              "AND NOT")) {
        nextWord = "NOT";
      }
      
      int flagValue = 0;
      if (word.charAt(0) == '-'
          || (startIndex > 0 && original.charAt(startIndex - 1) == '-')) {
        flagValue = PROHIBITED_TERM_FLAG;
      } else if (word.charAt(0) == '+'
          || (startIndex > 0 && original.charAt(startIndex - 1) == '+')) {
        flagValue = REQUIRED_TERM_FLAG;
      //we don't know the default operator so just assume the first operator isn't new.
      } else if (nextWord != null
          && lastBooleanOp != null 
          && !nextWord.equals(lastBooleanOp)
          && ("AND".equals(nextWord) || "OR".equals(nextWord) || "NOT".equals(nextWord))) {
        flagValue = TERM_PRECEDES_NEW_BOOLEAN_OPERATOR_FLAG;
      //...unless the 1st boolean operator is a NOT, because only AND/OR can be default.
      } else if (nextWord != null
          && lastBooleanOp == null
          && !nextWord.equals(lastBooleanOp)
          && ("NOT".equals(nextWord))) {
        flagValue = TERM_PRECEDES_NEW_BOOLEAN_OPERATOR_FLAG;
      }
      try {
        analyze(result, word, startIndex, flagValue);
      } catch (IOException e) {
        // TODO: shouldn't we log something?
      }   
    }
    if(lastBooleanOp != null) {
      for(Token t : result) {
        int f = t.getFlags();
        t.setFlags(f |= QueryConverter.TERM_IN_BOOLEAN_QUERY_FLAG);
      }
    }
    return result;
  }
  
  protected void analyze(Collection<Token> result, String text, int offset, int flagsAttValue) throws IOException {
    TokenStream stream = analyzer.tokenStream("", text);
    // TODO: support custom attributes
    CharTermAttribute termAtt = stream.addAttribute(CharTermAttribute.class);
    TypeAttribute typeAtt = stream.addAttribute(TypeAttribute.class);
    PayloadAttribute payloadAtt = stream.addAttribute(PayloadAttribute.class);
    PositionIncrementAttribute posIncAtt = stream.addAttribute(PositionIncrementAttribute.class);
    OffsetAttribute offsetAtt = stream.addAttribute(OffsetAttribute.class);
    stream.reset();
    while (stream.incrementToken()) {      
      Token token = new Token();
      token.copyBuffer(termAtt.buffer(), 0, termAtt.length());
      token.setOffset(offset + offsetAtt.startOffset(), 
                      offset + offsetAtt.endOffset());
      token.setFlags(flagsAttValue); //overwriting any flags already set...
      token.setType(typeAtt.type());
      token.setPayload(payloadAtt.getPayload());
      token.setPositionIncrement(posIncAtt.getPositionIncrement());
      result.add(token);
    }
    stream.end();
    stream.close();
  }
}


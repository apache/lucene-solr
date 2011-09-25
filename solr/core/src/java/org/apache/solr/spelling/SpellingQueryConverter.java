/**
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
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.FlagsAttribute;
import org.apache.lucene.analysis.tokenattributes.PayloadAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.TypeAttribute;


/**
 * Converts the query string to a Collection of Lucene tokens using a regular expression.
 * Boolean operators AND and OR are skipped.
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

  final static String PATTERN = "(?:(?!(" + NMTOKEN + ":|\\d+)))[\\p{L}_\\-0-9]+";
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
    Collection<Token> result = new ArrayList<Token>();
    //TODO: Extract the words using a simple regex, but not query stuff, and then analyze them to produce the token stream
    Matcher matcher = QUERY_REGEX.matcher(original);
    TokenStream stream;
    while (matcher.find()) {
      String word = matcher.group(0);
      if (word.equals("AND") == false && word.equals("OR") == false) {
        try {
          stream = analyzer.reusableTokenStream("", new StringReader(word));
          // TODO: support custom attributes
          CharTermAttribute termAtt = stream.addAttribute(CharTermAttribute.class);
          FlagsAttribute flagsAtt = stream.addAttribute(FlagsAttribute.class);
          TypeAttribute typeAtt = stream.addAttribute(TypeAttribute.class);
          PayloadAttribute payloadAtt = stream.addAttribute(PayloadAttribute.class);
          PositionIncrementAttribute posIncAtt = stream.addAttribute(PositionIncrementAttribute.class);
          stream.reset();
          while (stream.incrementToken()) {
            Token token = new Token();
            token.copyBuffer(termAtt.buffer(), 0, termAtt.length());
            token.setStartOffset(matcher.start());
            token.setEndOffset(matcher.end());
            token.setFlags(flagsAtt.getFlags());
            token.setType(typeAtt.type());
            token.setPayload(payloadAtt.getPayload());
            token.setPositionIncrement(posIncAtt.getPositionIncrement());
            result.add(token);
          }
          stream.end();
          stream.close();
        } catch (IOException e) {
        }
      }
    }
    return result;
  }

}


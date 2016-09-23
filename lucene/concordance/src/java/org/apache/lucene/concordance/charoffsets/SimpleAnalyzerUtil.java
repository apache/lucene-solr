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

package org.apache.lucene.concordance.charoffsets;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;


/**
 * Simple util class for Analyzers
 */
public class SimpleAnalyzerUtil {
  private final static String DEFAULT_FIELD = "FIELD";

  /**
   *
   * @param s string to analyze
   * @param field field to analyze
   * @param analyzer analyzer to use
   * @return list of analyzed terms
   * @throws IOException if there's an IOException during analysis
   */
  public static List<String> getTermStrings(String s, String field, Analyzer analyzer)
      throws IOException {
    List<String> terms = new ArrayList<>();
    return getTermStrings(s, field, analyzer, terms);
  }

  /**
   * allows reuse of terms, this method calls terms.clear() before adding new
   * terms
   *
   * @param s        string to analyze
   * @param field    to use in analysis
   * @param analyzer analyzer
   * @param terms    list for reuse
   * @return list of strings
   * @throws IOException if there's an IOException during analysis
   */
  public static List<String> getTermStrings(String s, String field, Analyzer analyzer,
                                            List<String> terms) throws IOException {
    if (terms == null) {
      terms = new ArrayList<>();
    }
    terms.clear();
    TokenStream stream = analyzer.tokenStream(field, s);
    stream.reset();
    CharTermAttribute termAtt = stream
        .getAttribute(org.apache.lucene.analysis.tokenattributes.CharTermAttribute.class);

    while (stream.incrementToken()) {
      terms.add(termAtt.toString());
    }
    stream.end();
    stream.close();

    return terms;
  }

  /**
   * This calculates a substring from an array of StorableFields.
   * <p>
   * This attempts to do the best job possible, and at worst will
   * return an empty string.  If the start or end is within a gap,
   * or before 0 or after the total number of characters, this will
   * gracefully (blithely?) handle those cases.
   *
   * @param start            character offset to start
   * @param end              character offset to end
   * @param fieldValues      array of Strings to process
   * @param offsetGap        offsetGap as typically returned by Analyzer's .getOffsetGap()
   * @param interFieldJoiner string to use to mark that a substring goes beyond a single
   *                         field entry
   * @return substring, potentially empty, never null.
   */
  public static String substringFromMultiValuedFields(int start,
                                                      int end, String[] fieldValues, int offsetGap, String interFieldJoiner) {
    start = (start < 0) ? 0 : start;
    end = (end < 0) ? 0 : end;

    if (start > end) {
      start = end;
    }

    int charBase = 0;
    StringBuilder sb = new StringBuilder();
    int lastFieldIndex = 0;
    int localStart = 0;
    boolean foundStart = false;
    //get start
    for (int fieldIndex = 0; fieldIndex < fieldValues.length; fieldIndex++) {
      String fString = fieldValues[fieldIndex];
      if (start < charBase + fString.length()) {
        localStart = start - charBase;
        lastFieldIndex = fieldIndex;
        foundStart = true;
        break;
      }
      charBase += fString.length() + offsetGap;
    }
    if (foundStart == false) {
      return "";
    }
    //if start occurred in a gap, reset localStart to 0
    if (localStart < 0) {
      sb.append(interFieldJoiner);
      localStart = 0;
    }
    //now append and look for end
    for (int fieldIndex = lastFieldIndex; fieldIndex < fieldValues.length; fieldIndex++) {
      String fString = fieldValues[fieldIndex];

      if (end <= charBase + fString.length()) {
        int localEnd = end - charBase;
        //must be in gap
        if (charBase > end) {
          return sb.toString();
        }
        if (fieldIndex != lastFieldIndex) {
          sb.append(interFieldJoiner);
        }
        sb.append(fString.substring(localStart, localEnd));
        break;
      } else {
        if (fieldIndex != lastFieldIndex) {
          sb.append(interFieldJoiner);
        }
        sb.append(fString.substring(localStart));
        localStart = 0;
      }
      charBase += fString.length() + offsetGap;
    }
    return sb.toString();
  }
}

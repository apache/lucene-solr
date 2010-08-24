package org.apache.lucene.search;

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

import java.io.IOException;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;

/**
 * Subclass of FilteredTermEnum for enumerating all terms that match the
 * specified wildcard filter term.
 * <p>
 * Term enumerations are always ordered by Term.compareTo().  Each term in
 * the enumeration is greater than all that precede it.
 */
public class WildcardTermEnum extends FilteredTermEnum {
  final Term searchTerm;
  final String field;
  final String text;
  final String pre;
  final int preLen;
  boolean endEnum = false;

  /**
   * Creates a new <code>WildcardTermEnum</code>.
   * <p>
   * After calling the constructor the enumeration is already pointing to the first 
   * valid term if such a term exists.
   */
  public WildcardTermEnum(IndexReader reader, Term term) throws IOException {
    super();
    searchTerm = term;
    field = searchTerm.field();
    final String searchTermText = searchTerm.text();

    final int sidx = searchTermText.indexOf(WILDCARD_STRING);
    final int cidx = searchTermText.indexOf(WILDCARD_CHAR);
    int idx = sidx;
    if (idx == -1) {
      idx = cidx;
    }
    else if (cidx >= 0) {
      idx = Math.min(idx, cidx);
    }
    pre = idx != -1?searchTerm.text().substring(0,idx): "";

    preLen = pre.length();
    text = searchTermText.substring(preLen);
    setEnum(reader.terms(new Term(searchTerm.field(), pre)));
  }

  @Override
  protected final boolean termCompare(Term term) {
    if (field == term.field()) {
      String searchText = term.text();
      if (searchText.startsWith(pre)) {
        return wildcardEquals(text, 0, searchText, preLen);
      }
    }
    endEnum = true;
    return false;
  }

  @Override
  public float difference() {
    return 1.0f;
  }

  @Override
  public final boolean endEnum() {
    return endEnum;
  }

  /********************************************
   * String equality with support for wildcards
   ********************************************/

  public static final char WILDCARD_STRING = '*';
  public static final char WILDCARD_CHAR = '?';

  /**
   * Determines if a word matches a wildcard pattern.
   * <small>Work released by Granta Design Ltd after originally being done on
   * company time.</small>
   */
  public static final boolean wildcardEquals(String pattern, int patternIdx,
    String string, int stringIdx)
  {
    int p = patternIdx;
    
    for (int s = stringIdx; ; ++p, ++s)
      {
        // End of string yet?
        boolean sEnd = (s >= string.length());
        // End of pattern yet?
        boolean pEnd = (p >= pattern.length());

        // If we're looking at the end of the string...
        if (sEnd)
        {
          // Assume the only thing left on the pattern is/are wildcards
          boolean justWildcardsLeft = true;

          // Current wildcard position
          int wildcardSearchPos = p;
          // While we haven't found the end of the pattern,
          // and haven't encountered any non-wildcard characters
          while (wildcardSearchPos < pattern.length() && justWildcardsLeft)
          {
            // Check the character at the current position
            char wildchar = pattern.charAt(wildcardSearchPos);
            
            // If it's not a wildcard character, then there is more
            // pattern information after this/these wildcards.
            if (wildchar != WILDCARD_CHAR && wildchar != WILDCARD_STRING)
            {
              justWildcardsLeft = false;
            }
            else
            {
              // to prevent "cat" matches "ca??"
              if (wildchar == WILDCARD_CHAR) {
                return false;
              }
              
              // Look at the next character
              wildcardSearchPos++;
            }
          }

          // This was a prefix wildcard search, and we've matched, so
          // return true.
          if (justWildcardsLeft)
          {
            return true;
          }
        }

        // If we've gone past the end of the string, or the pattern,
        // return false.
        if (sEnd || pEnd)
        {
          break;
        }

        // Match a single character, so continue.
        if (pattern.charAt(p) == WILDCARD_CHAR)
        {
          continue;
        }

        //
        if (pattern.charAt(p) == WILDCARD_STRING)
        {
          // Look at the character beyond the '*' characters.
          while (p < pattern.length() && pattern.charAt(p) == WILDCARD_STRING)
            ++p;
          // Examine the string, starting at the last character.
          for (int i = string.length(); i >= s; --i)
          {
            if (wildcardEquals(pattern, p, string, i))
            {
              return true;
            }
          }
          break;
        }
        if (pattern.charAt(p) != string.charAt(s))
        {
          break;
        }
      }
      return false;
  }
}

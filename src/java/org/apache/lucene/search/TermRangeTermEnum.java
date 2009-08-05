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
import java.text.Collator;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.util.StringHelper;

/**
 * Subclass of FilteredTermEnum for enumerating all terms that match the
 * specified range parameters.
 * <p>
 * Term enumerations are always ordered by Term.compareTo().  Each term in
 * the enumeration is greater than all that precede it.
 * @since 2.9
 */
public class TermRangeTermEnum extends FilteredTermEnum {

  private Collator collator = null;
  private boolean endEnum = false;
  private String field;
  private String upperTermText;
  private String lowerTermText;
  private boolean includeLower;
  private boolean includeUpper;

  /**
   * Enumerates all terms greater/equal than <code>lowerTerm</code>
   * but less/equal than <code>upperTerm</code>. 
   * 
   * If an endpoint is null, it is said to be "open". Either or both 
   * endpoints may be open.  Open endpoints may not be exclusive 
   * (you can't select all but the first or last term without 
   * explicitly specifying the term to exclude.)
   * 
   * @param reader
   * @param field
   *          An interned field that holds both lower and upper terms.
   * @param lowerTermText
   *          The term text at the lower end of the range
   * @param upperTermText
   *          The term text at the upper end of the range
   * @param includeLower
   *          If true, the <code>lowerTerm</code> is included in the range.
   * @param includeUpper
   *          If true, the <code>upperTerm</code> is included in the range.
   * @param collator
   *          The collator to use to collate index Terms, to determine their
   *          membership in the range bounded by <code>lowerTerm</code> and
   *          <code>upperTerm</code>.
   * 
   * @throws IOException
   */
  public TermRangeTermEnum(IndexReader reader, String field, String lowerTermText, String upperTermText, 
    boolean includeLower, boolean includeUpper, Collator collator) throws IOException {
    this.collator = collator;
    this.upperTermText = upperTermText;
    this.lowerTermText = lowerTermText;
    this.includeLower = includeLower;
    this.includeUpper = includeUpper;
    this.field = StringHelper.intern(field);
    
    // do a little bit of normalization...
    // open ended range queries should always be inclusive.
    if (this.lowerTermText == null) {
      this.lowerTermText = "";
      this.includeLower = true;
    }
    
    if (this.upperTermText == null) {
      this.includeUpper = true;
    }

    String startTermText = collator == null ? this.lowerTermText : "";
    setEnum(reader.terms(new Term(this.field, startTermText)));
  }

  public float difference() {
    return 1.0f;
  }

  protected boolean endEnum() {
    return endEnum;
  }

  protected boolean termCompare(Term term) {
    if (collator == null) {
      // Use Unicode code point ordering
      boolean checkLower = false;
      if (!includeLower) // make adjustments to set to exclusive
        checkLower = true;
      if (term != null && term.field() == field) { // interned comparison
        if (!checkLower || null==lowerTermText || term.text().compareTo(lowerTermText) > 0) {
          checkLower = false;
          if (upperTermText != null) {
            int compare = upperTermText.compareTo(term.text());
            /*
             * if beyond the upper term, or is exclusive and this is equal to
             * the upper term, break out
             */
            if ((compare < 0) ||
                (!includeUpper && compare==0)) {
              endEnum = true;
              return false;
            }
          }
          return true;
        }
      } else {
        // break
        endEnum = true;
        return false;
      }
      return false;
    } else {
      if (term != null && term.field() == field) { // interned comparison
        if ((lowerTermText == null
            || (includeLower
                ? collator.compare(term.text(), lowerTermText) >= 0
                : collator.compare(term.text(), lowerTermText) > 0))
           && (upperTermText == null
               || (includeUpper
                   ? collator.compare(term.text(), upperTermText) <= 0
                   : collator.compare(term.text(), upperTermText) < 0))) {
          return true;
        }
        return false;
      }
      endEnum = true;
      return false;
    }
  }
}

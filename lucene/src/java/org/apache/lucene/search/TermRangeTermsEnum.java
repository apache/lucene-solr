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
import java.util.Comparator;

import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.BytesRef;

/**
 * Subclass of FilteredTermEnum for enumerating all terms that match the
 * specified range parameters.
 * <p>Term enumerations are always ordered by
 * {@link #getComparator}.  Each term in the enumeration is
 * greater than all that precede it.</p>
 */
public class TermRangeTermsEnum extends FilteredTermsEnum {

  private Collator collator;
  private String upperTermText;
  private String lowerTermText;
  private boolean includeLower;
  private boolean includeUpper;
  final private BytesRef lowerBytesRef;
  final private BytesRef upperBytesRef;
  private final Comparator<BytesRef> termComp;

  /**
   * Enumerates all terms greater/equal than <code>lowerTerm</code>
   * but less/equal than <code>upperTerm</code>. 
   * 
   * If an endpoint is null, it is said to be "open". Either or both 
   * endpoints may be open.  Open endpoints may not be exclusive 
   * (you can't select all but the first or last term without 
   * explicitly specifying the term to exclude.)
   * 
   * @param tenum
   *          TermsEnum to filter
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
  public TermRangeTermsEnum(TermsEnum tenum, String lowerTermText, String upperTermText, 
    boolean includeLower, boolean includeUpper, Collator collator) throws IOException {
    super(tenum);
    this.collator = collator;
    this.upperTermText = upperTermText;
    this.lowerTermText = lowerTermText;
    this.includeLower = includeLower;
    this.includeUpper = includeUpper;

    // do a little bit of normalization...
    // open ended range queries should always be inclusive.
    if (this.lowerTermText == null) {
      this.lowerTermText = "";
      this.includeLower = true;
    }
    lowerBytesRef = new BytesRef(this.lowerTermText);

    if (this.upperTermText == null) {
      this.includeUpper = true;
      upperBytesRef = null;
    } else {
      upperBytesRef = new BytesRef(upperTermText);
    }

    BytesRef startBytesRef = (collator == null) ? lowerBytesRef : new BytesRef("");
    setInitialSeekTerm(startBytesRef);
    termComp = getComparator();
  }

  @Override
  protected AcceptStatus accept(BytesRef term) {
    if (collator == null) {
      if (!this.includeLower && term.equals(lowerBytesRef))
        return AcceptStatus.NO;
      // Use this field's default sort ordering
      if (upperBytesRef != null) {
        final int cmp = termComp.compare(upperBytesRef, term);
        /*
         * if beyond the upper term, or is exclusive and this is equal to
         * the upper term, break out
         */
        if ((cmp < 0) ||
            (!includeUpper && cmp==0)) {
          return AcceptStatus.END;
        }
      }
      return AcceptStatus.YES;
    } else {
      if ((includeLower
           ? collator.compare(term.utf8ToString(), lowerTermText) >= 0
           : collator.compare(term.utf8ToString(), lowerTermText) > 0)
          && (upperTermText == null
              || (includeUpper
                  ? collator.compare(term.utf8ToString(), upperTermText) <= 0
                  : collator.compare(term.utf8ToString(), upperTermText) < 0))) {
        return AcceptStatus.YES;
      }
      return AcceptStatus.NO;
    }
  }
}

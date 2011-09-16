package org.apache.lucene.sandbox.queries;

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

import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.index.FilteredTermsEnum;
import org.apache.lucene.util.BytesRef;

/**
 * Subclass of FilteredTermEnum for enumerating all terms that match the
 * specified range parameters.
 * <p>Term enumerations are always ordered by
 * {@link #getComparator}.  Each term in the enumeration is
 * greater than all that precede it.</p>
 * @deprecated Index collation keys with CollationKeyAnalyzer or ICUCollationKeyAnalyzer instead.
 *  This class will be removed in Lucene 5.0
 */
@Deprecated
public class SlowCollatedTermRangeTermsEnum extends FilteredTermsEnum {
  private Collator collator;
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
   * @param tenum
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
  public SlowCollatedTermRangeTermsEnum(TermsEnum tenum, String lowerTermText, String upperTermText, 
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

    // TODO: optimize
    BytesRef startBytesRef = new BytesRef("");
    setInitialSeekTerm(startBytesRef);
  }

  @Override
  protected AcceptStatus accept(BytesRef term) {
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

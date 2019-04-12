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

package org.apache.lucene.search.join;

import java.io.IOException;

import org.apache.lucene.index.FilteredTermsEnum;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefHash;

/**
 * A filtered TermsEnum that uses a BytesRefHash as a filter
 * @lucene.internal
 */
public class SeekingTermSetTermsEnum extends FilteredTermsEnum {

  private final BytesRefHash terms;
  private final int[] ords;
  private final int lastElement;

  private final BytesRef lastTerm;
  private final BytesRef spare = new BytesRef();

  private BytesRef seekTerm;
  private int upto = 0;

  /**
   * Constructor
   */
  public SeekingTermSetTermsEnum(TermsEnum tenum, BytesRefHash terms, int[] ords) {
    super(tenum);
    this.terms = terms;
    this.ords = ords;
    lastElement = terms.size() - 1;
    lastTerm = terms.get(ords[lastElement], new BytesRef());
    seekTerm = terms.get(ords[upto], spare);
  }

  @Override
  protected BytesRef nextSeekTerm(BytesRef currentTerm) throws IOException {
    BytesRef temp = seekTerm;
    seekTerm = null;
    return temp;
  }

  @Override
  protected AcceptStatus accept(BytesRef term) throws IOException {
    if (term.compareTo(lastTerm) > 0) {
      return AcceptStatus.END;
    }

    BytesRef currentTerm = terms.get(ords[upto], spare);
    if (term.compareTo(currentTerm) == 0) {
      if (upto == lastElement) {
        return AcceptStatus.YES;
      } else {
        seekTerm = terms.get(ords[++upto], spare);
        return AcceptStatus.YES_AND_SEEK;
      }
    } else {
      if (upto == lastElement) {
        return AcceptStatus.NO;
      } else { // Our current term doesn't match the the given term.
        int cmp;
        do { // We maybe are behind the given term by more than one step. Keep incrementing till we're the same or higher.
          if (upto == lastElement) {
            return AcceptStatus.NO;
          }
          // typically the terms dict is a superset of query's terms so it's unusual that we have to skip many of
          // our terms so we don't do a binary search here
          seekTerm = terms.get(ords[++upto], spare);
        } while ((cmp = seekTerm.compareTo(term)) < 0);
        if (cmp == 0) {
          if (upto == lastElement) {
            return AcceptStatus.YES;
          }
          seekTerm = terms.get(ords[++upto], spare);
          return AcceptStatus.YES_AND_SEEK;
        } else {
          return AcceptStatus.NO_AND_SEEK;
        }
      }
    }
  }

}

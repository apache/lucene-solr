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
package org.apache.lucene.search.uhighlight;

import java.io.IOException;

import org.apache.lucene.index.Fields;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.automaton.CompiledAutomaton;

/**
 * This class provides a filtered view on a leaf reader.
 * The view is filtered to only iterate over the Terms specified.
 * Certain methods may be unimplemented or cause large operations on the underlying reader
 * and be slow.
 */
public final class TermVectorFilteredLeafReader extends FilterLeafReader {
  private final Terms filterTerms;

  /**
   * <p>Construct a FilterLeafReader based on the specified base reader.
   * <p>Note that base reader is closed if this FilterLeafReader is closed.</p>
   *
   * @param in          specified base reader.
   * @param filterTerms to filter by
   */
  public TermVectorFilteredLeafReader(LeafReader in, Terms filterTerms) {
    super(in);
    this.filterTerms = filterTerms;
  }

  @Override
  public Fields fields() throws IOException {

    return new TermVectorFilteredFields(in.fields(), filterTerms);
  }

  private static final class TermVectorFilteredFields extends FilterLeafReader.FilterFields {

    private final Terms filterTerms;

    /**
     * Creates a new FilterFields.
     *
     * @param in          the underlying Fields instance.
     * @param filterTerms
     */
    public TermVectorFilteredFields(Fields in, Terms filterTerms) {
      super(in);
      this.filterTerms = filterTerms;
    }

    @Override
    public Terms terms(String field) throws IOException {
      return new TermsFilteredTerms(in.terms(field), filterTerms);
    }
  }

  private static final class TermsFilteredTerms extends FilterLeafReader.FilterTerms {

    private final Terms filterTerms;

    /**
     * Creates a new FilterTerms
     *
     * @param in          the underlying Terms instance.
     * @param filterTerms
     */
    public TermsFilteredTerms(Terms in, Terms filterTerms) {
      super(in);
      this.filterTerms = filterTerms;
    }

    //TODO delegate size() ?
    //todo delegate getMin, getMax delegate to filterTerms

    @Override
    public TermsEnum iterator() throws IOException {
      return new TermVectorFilteredTermsEnum(in.iterator(), filterTerms.iterator());
    }

    @Override
    public TermsEnum intersect(CompiledAutomaton compiled, BytesRef startTerm) throws IOException {
      return new TermVectorFilteredTermsEnum(in.iterator(), filterTerms.intersect(compiled, startTerm));
    }
  }

  private static final class TermVectorFilteredTermsEnum extends FilterLeafReader.FilterTermsEnum {

    //TODO: track the last term state from the term state method and do some potential optimizations
    private final TermsEnum fullTermsEnum;

    /**
     * Creates a new FilterTermsEnum
     *
     * @param fullTermsEnum the underlying TermsEnum instance.
     */
    public TermVectorFilteredTermsEnum(TermsEnum fullTermsEnum, TermsEnum filteredTermsEnum) {
      super(filteredTermsEnum);
      this.fullTermsEnum = fullTermsEnum;
    }

    //TODO delegate docFreq & ttf (moveToCurrentTerm() then call on full?

    @Override
    public PostingsEnum postings(PostingsEnum reuse, int flags) throws IOException {
      moveToCurrentTerm();
      return fullTermsEnum.postings(reuse, flags);
    }

    void moveToCurrentTerm() throws IOException {
      BytesRef currentTerm = in.term();
      boolean termInBothTermsEnum = fullTermsEnum.seekExact(currentTerm);

      if (!termInBothTermsEnum) {
        throw new IllegalStateException("Term vector term " + currentTerm + " does not appear in full index.");
      }
    }

  }
}

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

import org.apache.lucene.index.IndexedFields;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.IndexedField;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.automaton.CompiledAutomaton;

/**
 * A filtered LeafReader that only includes the terms that are also in a provided set of terms.
 * Certain methods may be unimplemented or cause large operations on the underlying reader
 * and be slow.
 *
 * @lucene.internal
 */
final class TermVectorFilteredLeafReader extends FilterLeafReader {
  // NOTE: super ("in") is baseLeafReader

  private final IndexedField filterTerms;

  /**
   * <p>Construct a FilterLeafReader based on the specified base reader.
   * <p>Note that base reader is closed if this FilterLeafReader is closed.</p>
   *
   * @param baseLeafReader full/original reader.
   * @param filterTerms set of terms to filter by -- probably from a TermVector or MemoryIndex.
   */
  TermVectorFilteredLeafReader(LeafReader baseLeafReader, IndexedField filterTerms) {
    super(baseLeafReader);
    this.filterTerms = filterTerms;
  }

  @Override
  public IndexedFields fields() throws IOException {
    return new TermVectorFilteredFields(in.fields(), filterTerms);
  }

  private static final class TermVectorFilteredFields extends FilterLeafReader.FilterFields {
    // NOTE: super ("in") is baseFields

    private final IndexedField filterTerms;

    TermVectorFilteredFields(IndexedFields baseFields, IndexedField filterTerms) {
      super(baseFields);
      this.filterTerms = filterTerms;
    }

    @Override
    public IndexedField indexedField(String field) throws IOException {
      return new TermsFilteredTerms(in.indexedField(field), filterTerms);
    }
  }

  private static final class TermsFilteredTerms extends FilterLeafReader.FilterField {
    // NOTE: super ("in") is the baseTerms

    private final IndexedField filterTerms;

    TermsFilteredTerms(IndexedField baseTerms, IndexedField filterTerms) {
      super(baseTerms);
      this.filterTerms = filterTerms;
    }

    //TODO delegate size() ?

    //TODO delegate getMin, getMax to filterTerms

    @Override
    public TermsEnum getTermsEnum() throws IOException {
      return new TermVectorFilteredTermsEnum(in.getTermsEnum(), filterTerms.getTermsEnum());
    }

    @Override
    public TermsEnum intersect(CompiledAutomaton compiled, BytesRef startTerm) throws IOException {
      return new TermVectorFilteredTermsEnum(in.getTermsEnum(), filterTerms.intersect(compiled, startTerm));
    }
  }

  private static final class TermVectorFilteredTermsEnum extends FilterLeafReader.FilterTermsEnum {
    // NOTE: super ("in") is the filteredTermsEnum. This is different than wrappers above because we
    //    navigate the terms using the filter.

    //TODO: track the last term state from the term state method and do some potential optimizations
    private final TermsEnum baseTermsEnum;

    TermVectorFilteredTermsEnum(TermsEnum baseTermsEnum, TermsEnum filteredTermsEnum) {
      super(filteredTermsEnum); // note this is reversed from constructors above
      this.baseTermsEnum = baseTermsEnum;
    }

    //TODO delegate docFreq & ttf (moveToCurrentTerm() then call on full?

    @Override
    public PostingsEnum postings(PostingsEnum reuse, int flags) throws IOException {
      moveToCurrentTerm();
      return baseTermsEnum.postings(reuse, flags);
    }

    void moveToCurrentTerm() throws IOException {
      BytesRef currentTerm = in.term(); // from filteredTermsEnum
      boolean termInBothTermsEnum = baseTermsEnum.seekExact(currentTerm);

      if (!termInBothTermsEnum) {
        throw new IllegalStateException("Term vector term " + currentTerm + " does not appear in full index.");
      }
    }

  }
}

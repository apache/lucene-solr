package org.apache.lucene.document;

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

import java.io.IOException;

import org.apache.lucene.document.FieldTypes;
import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.BitsFilteredDocIdSet;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Filter;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;

/** A filter that matches docs that indexed the specified field name.  This only works
 *  if {@link FieldTypes#enableExistsFilters} is true (the default). */
final class FieldExistsFilter extends Filter {

  private final String fieldString;
  private final BytesRef field;

  /**
   * @param term The term documents need to have in order to be a match for this filter.
   */
  public FieldExistsFilter(String field) {
    if (field == null) {
      throw new IllegalArgumentException("field must not be null");
    }
    this.fieldString = field;
    this.field = new BytesRef(field);
  }

  @Override
  public DocIdSet getDocIdSet(LeafReaderContext context, final Bits acceptDocs) throws IOException {

    int maxDoc = context.reader().maxDoc();

    Terms terms = context.reader().terms(fieldString);
    if (terms != null && terms.getDocCount() == maxDoc) {
      // All docs have the field
      return BitsFilteredDocIdSet.wrap(DocIdSet.full(maxDoc), acceptDocs);
    }

    terms = context.reader().terms(FieldTypes.FIELD_NAMES_FIELD);
    if (terms == null) {
      return null;
    }

    final TermsEnum termsEnum = terms.iterator(null);
    if (!termsEnum.seekExact(field)) {
      return null;
    }

    // The Terms.getDocCount() above should have kicked in:
    assert termsEnum.docFreq() < maxDoc;

    return new DocIdSet() {
      @Override
      public DocIdSetIterator iterator() throws IOException {
        return termsEnum.docs(acceptDocs, null, DocsEnum.FLAG_NONE);
      }

      @Override
      public long ramBytesUsed() {
        return 0L;
      }
    };
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    FieldExistsFilter that = (FieldExistsFilter) o;

    return field.equals(that.field);
  }

  @Override
  public int hashCode() {
    return field.hashCode();
  }

  @Override
  public String toString() {
    return "FieldExistsFilter(field=" + fieldString + ")";
  }
}

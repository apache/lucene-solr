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
import java.util.Objects;

import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.AttributeSource;
import org.apache.lucene.util.BytesRefHash;
import org.apache.lucene.util.RamUsageEstimator;

/**
 * A query that has an array of terms from a specific field. This query will match documents have one or more terms in
 * the specified field that match with the terms specified in the array.
 *
 * @lucene.experimental
 */
class TermsQuery extends MultiTermQuery implements Accountable {
  private static final long BASE_RAM_BYTES = RamUsageEstimator.shallowSizeOfInstance(TermsQuery.class);

  private final BytesRefHash terms;
  private final int[] ords;

  // These fields are used for equals() and hashcode() only
  private final String fromField;
  private final Query fromQuery;
  // id of the context rather than the context itself in order not to hold references to index readers
  private final Object indexReaderContextId;

  private final long ramBytesUsed; // cache

  /**
   * @param toField               The field that should contain terms that are specified in the next parameter.
   * @param terms                 The terms that matching documents should have. The terms must be sorted by natural order.
   * @param indexReaderContextId  Refers to the top level index reader used to create the set of terms in the previous parameter.
   */
  TermsQuery(String toField, BytesRefHash terms, String fromField, Query fromQuery, Object indexReaderContextId) {
    super(toField);
    this.terms = terms;
    ords = terms.sort();
    this.fromField = fromField;
    this.fromQuery = fromQuery;
    this.indexReaderContextId = indexReaderContextId;

    this.ramBytesUsed = BASE_RAM_BYTES +
        RamUsageEstimator.sizeOfObject(field) +
        RamUsageEstimator.sizeOfObject(fromField) +
        RamUsageEstimator.sizeOfObject(fromQuery, RamUsageEstimator.QUERY_DEFAULT_RAM_BYTES_USED) +
        RamUsageEstimator.sizeOfObject(ords) +
        RamUsageEstimator.sizeOfObject(terms);
  }

  @Override
  public void visit(QueryVisitor visitor) {
    visitor.visitLeaf(this);
  }

  @Override
  protected TermsEnum getTermsEnum(Terms terms, AttributeSource atts) throws IOException {
    if (this.terms.size() == 0) {
      return TermsEnum.EMPTY;
    }

    return new SeekingTermSetTermsEnum(terms.iterator(), this.terms, ords);
  }

  @Override
  public String toString(String string) {
    return "TermsQuery{" +
        "field=" + field +
        "fromQuery=" + fromQuery.toString(field) +
        '}';
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    } if (!super.equals(obj)) {
      return false;
    } if (getClass() != obj.getClass()) {
      return false;
    }

    TermsQuery other = (TermsQuery) obj;
    return Objects.equals(field, other.field) &&
        Objects.equals(fromField, other.fromField) &&
        Objects.equals(fromQuery, other.fromQuery) &&
        Objects.equals(indexReaderContextId, other.indexReaderContextId);
  }

  @Override
  public int hashCode() {
    return classHash() + Objects.hash(field, fromField, fromQuery, indexReaderContextId);
  }

  @Override
  public long ramBytesUsed() {
    return ramBytesUsed;
  }
}

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
package org.apache.lucene.search;


import java.util.Objects;

import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermsEnum; // javadocs
import org.apache.lucene.util.BytesRef;
/**
 * Contains statistics for a specific term
 * <p>
 * This class holds statistics for this term across all documents for scoring purposes:
 * <ul>
 *   <li> {@link #docFreq}: number of documents this term occurs in.
 *   <li> {@link #totalTermFreq}: number of tokens for this term.
 * </ul>
 * <p>
 * The following conditions are always true:
 * <ul>
 *   <li> All statistics are positive integers: never zero or negative.
 *   <li> {@code docFreq} &lt;= {@code totalTermFreq}
 *   <li> {@code docFreq} &lt;= {@code sumDocFreq} of the collection
 *   <li> {@code totalTermFreq} &lt;= {@code sumTotalTermFreq} of the collection
 * </ul>
 * <p>
 * Values may include statistics on deleted documents that have not yet been merged away.
 * <p>
 * Be careful when performing calculations on these values because they are represented
 * as 64-bit integer values, you may need to cast to {@code double} for your use.
 * @lucene.experimental
 */
// TODO: actually add missing cross-checks to guarantee TermStatistics is in bounds of CollectionStatistics,
// otherwise many similarity functions will implode.
public class TermStatistics {
  private final BytesRef term;
  private final long docFreq;
  private final long totalTermFreq;
  
  /**
   * Creates statistics instance for a term.
   * @param term Term bytes
   * @param docFreq number of documents containing the term in the collection.
   * @param totalTermFreq number of occurrences of the term in the collection.
   * @throws NullPointerException if {@code term} is {@code null}.
   * @throws IllegalArgumentException if {@code docFreq} is negative or zero.
   * @throws IllegalArgumentException if {@code totalTermFreq} is less than {@code docFreq}.
   */
  public TermStatistics(BytesRef term, long docFreq, long totalTermFreq) {
    Objects.requireNonNull(term);
    if (docFreq <= 0) {
      throw new IllegalArgumentException("docFreq must be positive, docFreq: " + docFreq);
    }
    if (totalTermFreq <= 0) {
      throw new IllegalArgumentException("totalTermFreq must be positive, totalTermFreq: " + totalTermFreq);
    }
    if (totalTermFreq < docFreq) {
      throw new IllegalArgumentException("totalTermFreq must be at least docFreq, totalTermFreq: " + totalTermFreq + ", docFreq: " + docFreq);
    }
    this.term = term;
    this.docFreq = docFreq;
    this.totalTermFreq = totalTermFreq;
  }
  
  /**
   * The term text.
   * <p>
   * This value is never {@code null}.
   * @return term's text, not {@code null}
   */
  public final BytesRef term() {
    return term;
  }
  
  /**
   * The number of documents this term occurs in.
   * <p>
   * This is the document-frequency for the term: the count of documents
   * where the term appears at least one time.
   * <p>
   * This value is always a positive number, and never
   * exceeds {@link #totalTermFreq}. It also cannot exceed {@link CollectionStatistics#sumDocFreq()}.
   * @return document frequency, in the range [1 .. {@link #totalTermFreq()}]
   * @see TermsEnum#docFreq()
   */
  public final long docFreq() {
    return docFreq;
  }
  
  /**
   * The total number of occurrences of this term.
   * <p>
   * This is the token count for the term: the number of times it appears in the field across all documents.
   * <p>
   * This value is always a positive number, always at least {@link #docFreq()},
   * and never exceeds {@link CollectionStatistics#sumTotalTermFreq()}.
   * @return number of occurrences, in the range [{@link #docFreq()} .. {@link CollectionStatistics#sumTotalTermFreq()}]
   * @see TermsEnum#totalTermFreq()
   */
  public final long totalTermFreq() {
    return totalTermFreq;
  }
  
  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("term=");
    sb.append('"');
    sb.append(Term.toString(term()));
    sb.append('"');
    sb.append(",docFreq=");
    sb.append(docFreq());
    sb.append(",totalTermFreq=");
    sb.append(totalTermFreq());
    return sb.toString();
  }
}

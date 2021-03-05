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
import org.apache.lucene.index.IndexReader; // javadocs
import org.apache.lucene.index.Terms; // javadocs

/**
 * Contains statistics for a collection (field).
 *
 * <p>This class holds statistics across all documents for scoring purposes:
 *
 * <ul>
 *   <li>{@link #maxDoc()}: number of documents.
 *   <li>{@link #docCount()}: number of documents that contain this field.
 *   <li>{@link #sumDocFreq()}: number of postings-list entries.
 *   <li>{@link #sumTotalTermFreq()}: number of tokens.
 * </ul>
 *
 * <p>The following conditions are always true:
 *
 * <ul>
 *   <li>All statistics are positive integers: never zero or negative.
 *   <li>{@code docCount} &lt;= {@code maxDoc}
 *   <li>{@code docCount} &lt;= {@code sumDocFreq} &lt;= {@code sumTotalTermFreq}
 * </ul>
 *
 * <p>Values may include statistics on deleted documents that have not yet been merged away.
 *
 * <p>Be careful when performing calculations on these values because they are represented as 64-bit
 * integer values, you may need to cast to {@code double} for your use.
 *
 * @lucene.experimental
 */
public class CollectionStatistics {
  private final String field;
  private final long maxDoc;
  private final long docCount;
  private final long sumTotalTermFreq;
  private final long sumDocFreq;

  /**
   * Creates statistics instance for a collection (field).
   *
   * @param field Field's name
   * @param maxDoc total number of documents.
   * @param docCount number of documents containing the field.
   * @param sumTotalTermFreq number of tokens in the field.
   * @param sumDocFreq number of postings list entries for the field.
   * @throws IllegalArgumentException if {@code maxDoc} is negative or zero.
   * @throws IllegalArgumentException if {@code docCount} is negative or zero.
   * @throws IllegalArgumentException if {@code docCount} is more than {@code maxDoc}.
   * @throws IllegalArgumentException if {@code sumDocFreq} is less than {@code docCount}.
   * @throws IllegalArgumentException if {@code sumTotalTermFreq} is less than {@code sumDocFreq}.
   */
  public CollectionStatistics(
      String field, long maxDoc, long docCount, long sumTotalTermFreq, long sumDocFreq) {
    Objects.requireNonNull(field);
    if (maxDoc <= 0) {
      throw new IllegalArgumentException("maxDoc must be positive, maxDoc: " + maxDoc);
    }
    if (docCount <= 0) {
      throw new IllegalArgumentException("docCount must be positive, docCount: " + docCount);
    }
    if (docCount > maxDoc) {
      throw new IllegalArgumentException(
          "docCount must not exceed maxDoc, docCount: " + docCount + ", maxDoc: " + maxDoc);
    }
    if (sumDocFreq <= 0) {
      throw new IllegalArgumentException("sumDocFreq must be positive, sumDocFreq: " + sumDocFreq);
    }
    if (sumDocFreq < docCount) {
      throw new IllegalArgumentException(
          "sumDocFreq must be at least docCount, sumDocFreq: "
              + sumDocFreq
              + ", docCount: "
              + docCount);
    }
    if (sumTotalTermFreq <= 0) {
      throw new IllegalArgumentException(
          "sumTotalTermFreq must be positive, sumTotalTermFreq: " + sumTotalTermFreq);
    }
    if (sumTotalTermFreq < sumDocFreq) {
      throw new IllegalArgumentException(
          "sumTotalTermFreq must be at least sumDocFreq, sumTotalTermFreq: "
              + sumTotalTermFreq
              + ", sumDocFreq: "
              + sumDocFreq);
    }
    this.field = field;
    this.maxDoc = maxDoc;
    this.docCount = docCount;
    this.sumTotalTermFreq = sumTotalTermFreq;
    this.sumDocFreq = sumDocFreq;
  }

  /**
   * The field's name.
   *
   * <p>This value is never {@code null}.
   *
   * @return field's name, not {@code null}
   */
  public final String field() {
    return field;
  }

  /**
   * The total number of documents, regardless of whether they all contain values for this field.
   *
   * <p>This value is always a positive number.
   *
   * @return total number of documents, in the range [1 .. {@link Long#MAX_VALUE}]
   * @see IndexReader#maxDoc()
   */
  public final long maxDoc() {
    return maxDoc;
  }

  /**
   * The total number of documents that have at least one term for this field.
   *
   * <p>This value is always a positive number, and never exceeds {@link #maxDoc()}.
   *
   * @return total number of documents containing this field, in the range [1 .. {@link #maxDoc()}]
   * @see Terms#getDocCount()
   */
  public final long docCount() {
    return docCount;
  }

  /**
   * The total number of tokens for this field. This is the "word count" for this field across all
   * documents. It is the sum of {@link TermStatistics#totalTermFreq()} across all terms. It is also
   * the sum of each document's field length across all documents.
   *
   * <p>This value is always a positive number, and always at least {@link #sumDocFreq()}.
   *
   * @return total number of tokens in the field, in the range [{@link #sumDocFreq()} .. {@link
   *     Long#MAX_VALUE}]
   * @see Terms#getSumTotalTermFreq()
   */
  public final long sumTotalTermFreq() {
    return sumTotalTermFreq;
  }

  /**
   * The total number of posting list entries for this field. This is the sum of term-document
   * pairs: the sum of {@link TermStatistics#docFreq()} across all terms. It is also the sum of each
   * document's unique term count for this field across all documents.
   *
   * <p>This value is always a positive number, always at least {@link #docCount()}, and never
   * exceeds {@link #sumTotalTermFreq()}.
   *
   * @return number of posting list entries, in the range [{@link #docCount()} .. {@link
   *     #sumTotalTermFreq()}]
   * @see Terms#getSumDocFreq()
   */
  public final long sumDocFreq() {
    return sumDocFreq;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("field=");
    sb.append('"');
    sb.append(field());
    sb.append('"');
    sb.append(",maxDoc=");
    sb.append(maxDoc());
    sb.append(",docCount=");
    sb.append(docCount());
    sb.append(",sumTotalTermFreq=");
    sb.append(sumTotalTermFreq());
    sb.append(",sumDocFreq=");
    sb.append(sumDocFreq);
    return sb.toString();
  }
}

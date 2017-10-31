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
 * @lucene.experimental
 */
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
    if (totalTermFreq != -1) {
      if (totalTermFreq < docFreq) {
        throw new IllegalArgumentException("totalTermFreq must be at least docFreq, totalTermFreq: " + totalTermFreq + ", docFreq: " + docFreq);
      }
    }
    this.term = term;
    this.docFreq = docFreq;
    this.totalTermFreq = totalTermFreq;
  }
  
  /** returns the term text */
  public final BytesRef term() {
    return term;
  }
  
  /** returns the number of documents this term occurs in 
   * @see TermsEnum#docFreq() */
  public final long docFreq() {
    return docFreq;
  }
  
  /** returns the total number of occurrences of this term
   * @see TermsEnum#totalTermFreq() */
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

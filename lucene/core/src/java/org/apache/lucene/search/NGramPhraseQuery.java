package org.apache.lucene.search;

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

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;

/**
 * This is a {@link PhraseQuery} which is optimized for n-gram phrase query.
 * For example, when you query "ABCD" on a 2-gram field, you may want to use
 * NGramPhraseQuery rather than {@link PhraseQuery}, because NGramPhraseQuery
 * will {@link #rewrite(IndexReader)} the query to "AB/0 CD/2", while {@link PhraseQuery}
 * will query "AB/0 BC/1 CD/2" (where term/position).
 *
 */
public class NGramPhraseQuery extends PhraseQuery {
  private final int n;
  
  /**
   * Constructor that takes gram size.
   * @param n n-gram size
   */
  public NGramPhraseQuery(int n){
    super();
    this.n = n;
  }

  @Override
  public Query rewrite(IndexReader reader) throws IOException {
    if(getSlop() != 0) return super.rewrite(reader);
    
    // check whether optimizable or not
    if(n < 2 || // non-overlap n-gram cannot be optimized
        getTerms().length < 3)  // too short to optimize
      return super.rewrite(reader);

    // check all posIncrement is 1
    // if not, cannot optimize
    int[] positions = getPositions();
    Term[] terms = getTerms();
    int prevPosition = positions[0];
    for(int i = 1; i < positions.length; i++){
      int pos = positions[i];
      if(prevPosition + 1 != pos) return super.rewrite(reader);
      prevPosition = pos;
    }

    // now create the new optimized phrase query for n-gram
    PhraseQuery optimized = new PhraseQuery();
    optimized.setBoost(getBoost());
    int pos = 0;
    final int lastPos = terms.length - 1;
    for(int i = 0; i < terms.length; i++){
      if(pos % n == 0 || pos >= lastPos){
        optimized.add(terms[i], positions[i]);
      }
      pos++;
    }
    
    return optimized;
  }

  /** Returns true iff <code>o</code> is equal to this. */
  @Override
  public boolean equals(Object o) {
    if (!(o instanceof NGramPhraseQuery))
      return false;
    NGramPhraseQuery other = (NGramPhraseQuery)o;
    if(this.n != other.n) return false;
    return super.equals(other);
  }

  /** Returns a hash code value for this object.*/
  @Override
  public int hashCode() {
    return Float.floatToIntBits(getBoost())
      ^ getSlop()
      ^ getTerms().hashCode()
      ^ getPositions().hashCode()
      ^ n;
  }
}

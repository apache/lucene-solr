package org.apache.lucene.index;

import org.apache.lucene.util.BytesRef;

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

/** Provides access to stored term vector of 
 *  a document field.  The vector consists of the name of the field, an array of the terms that occur in the field of the
 * {@link org.apache.lucene.document.Document} and a parallel array of frequencies.  Thus, getTermFrequencies()[5] corresponds with the
 * frequency of getTerms()[5], assuming there are at least 5 terms in the Document.
 */
public interface TermFreqVector {
  /**
   * The {@link org.apache.lucene.index.IndexableField} name. 
   * @return The name of the field this vector is associated with.
   * 
   */ 
  public String getField();
  
  /** 
   * @return The number of terms in the term vector.
   */
  public int size();

  /** 
   * @return An Array of term texts in ascending order.
   */
  public BytesRef[] getTerms();


  /** Array of term frequencies. Locations of the array correspond one to one
   *  to the terms in the array obtained from <code>getTerms</code>
   *  method. Each location in the array contains the number of times this
   *  term occurs in the document or the document field.
   */
  public int[] getTermFrequencies();
  

  /** Return an index in the term numbers array returned from
   *  <code>getTerms</code> at which the term with the specified
   *  <code>term</code> appears. If this term does not appear in the array,
   *  return -1.
   */
  public int indexOf(BytesRef term);


  /** Just like <code>indexOf(int)</code> but searches for a number of terms
   *  at the same time. Returns an array that has the same size as the number
   *  of terms searched for, each slot containing the result of searching for
   *  that term number.
   *
   *  @param terms array containing terms to look for
   *  @param start index in the array where the list of terms starts
   *  @param len the number of terms in the list
   */
  public int[] indexesOf(BytesRef[] terms, int start, int len);

}

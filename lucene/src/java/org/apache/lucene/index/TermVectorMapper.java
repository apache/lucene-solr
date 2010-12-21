package org.apache.lucene.index;

import org.apache.lucene.util.BytesRef;

/**
 * Copyright 2007 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


/**
 * The TermVectorMapper can be used to map Term Vectors into your own
 * structure instead of the parallel array structure used by
 * {@link org.apache.lucene.index.IndexReader#getTermFreqVector(int,String)}.
 * <p/>
 * It is up to the implementation to make sure it is thread-safe.
 *
 *
 **/
public abstract class TermVectorMapper {

  private boolean ignoringPositions;
  private boolean ignoringOffsets;


  protected TermVectorMapper() {
  }

  /**
   *
   * @param ignoringPositions true if this mapper should tell Lucene to ignore positions even if they are stored
   * @param ignoringOffsets similar to ignoringPositions
   */
  protected TermVectorMapper(boolean ignoringPositions, boolean ignoringOffsets) {
    this.ignoringPositions = ignoringPositions;
    this.ignoringOffsets = ignoringOffsets;
  }

  /**
   * Tell the mapper what to expect in regards to field, number of terms, offset and position storage.
   * This method will be called once before retrieving the vector for a field.
   *
   * This method will be called before {@link #map(BytesRef,int,TermVectorOffsetInfo[],int[])}.
   * @param field The field the vector is for
   * @param numTerms The number of terms that need to be mapped
   * @param storeOffsets true if the mapper should expect offset information
   * @param storePositions true if the mapper should expect positions info
   */
  public abstract void setExpectations(String field, int numTerms, boolean storeOffsets, boolean storePositions);
  /**
   * Map the Term Vector information into your own structure
   * @param term The term to add to the vector
   * @param frequency The frequency of the term in the document
   * @param offsets null if the offset is not specified, otherwise the offset into the field of the term
   * @param positions null if the position is not specified, otherwise the position in the field of the term
   */
  public abstract void map(BytesRef term, int frequency, TermVectorOffsetInfo [] offsets, int [] positions);

  /**
   * Indicate to Lucene that even if there are positions stored, this mapper is not interested in them and they
   * can be skipped over.  Derived classes should set this to true if they want to ignore positions.  The default
   * is false, meaning positions will be loaded if they are stored.
   * @return false
   */
  public boolean isIgnoringPositions()
  {
    return ignoringPositions;
  }

  /**
   *
   * @see #isIgnoringPositions() Same principal as {@link #isIgnoringPositions()}, but applied to offsets.  false by default.
   * @return false
   */
  public boolean isIgnoringOffsets()
  {
    return ignoringOffsets;
  }

  /**
   * Passes down the index of the document whose term vector is currently being mapped,
   * once for each top level call to a term vector reader.
   *<p/>
   * Default implementation IGNORES the document number.  Override if your implementation needs the document number.
   * <p/> 
   * NOTE: Document numbers are internal to Lucene and subject to change depending on indexing operations.
   *
   * @param documentNumber index of document currently being mapped
   */
  public void setDocumentNumber(int documentNumber) {
  }

}

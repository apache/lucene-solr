package org.apache.lucene.facet.enhancements.association;

import java.io.IOException;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;

import org.apache.lucene.facet.index.params.CategoryListParams;
import org.apache.lucene.facet.search.PayloadIntDecodingIterator;
import org.apache.lucene.util.collections.IntIterator;
import org.apache.lucene.util.collections.IntToIntMap;
import org.apache.lucene.util.encoding.SimpleIntDecoder;

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

/**
 * Allows easy iteration over the associations payload, decoding and breaking it
 * to (ordinal, value) pairs, stored in a hash.
 * 
 * @lucene.experimental
 */
public class AssociationsPayloadIterator {

  /**
   * Default Term for associations
   */
  public static final Term ASSOCIATION_POSTING_TERM = new Term(
      CategoryListParams.DEFAULT_TERM.field(),
      AssociationEnhancement.CATEGORY_LIST_TERM_TEXT);

  /**
   * Hash mapping to ordinals to the associated int value
   */
  private IntToIntMap ordinalToAssociationMap;

  /**
   * An inner payload decoder which actually goes through the posting and
   * decode the ints representing the ordinals and the values
   */
  private PayloadIntDecodingIterator associationPayloadIter;

  /**
   * Marking whether there are associations (at all) in the given index
   */
  private boolean hasAssociations = false;

  /**
   * The long-special-value returned for ordinals which have no associated int
   * value. It is not in the int range of values making it a valid mark.
   */
  public final static long NO_ASSOCIATION = Integer.MAX_VALUE + 1;

  /**
   * Construct a new association-iterator, initializing the inner payload
   * iterator, with the supplied term and checking whether there are any
   * associations within the given index
   * 
   * @param reader
   *            a reader containing the postings to be iterated
   * @param field
   *            the field containing the relevant associations list term
   */
  public AssociationsPayloadIterator(IndexReader reader, String field)
      throws IOException {
    // Initialize the payloadDecodingIterator
    associationPayloadIter = new PayloadIntDecodingIterator(
        reader,
        // TODO (Facet): should consolidate with AssociationListTokenizer which
        // uses AssociationEnhancement.getCatTermText()
        new Term(field, AssociationEnhancement.CATEGORY_LIST_TERM_TEXT),
        new SimpleIntDecoder());

    // Check whether there are any associations
    hasAssociations = associationPayloadIter.init();

    ordinalToAssociationMap = new IntToIntMap();
  }

  /**
   * Skipping to the next document, fetching its associations & populating the
   * map.
   * 
   * @param docId
   *            document id to be skipped to
   * @return true if the document contains associations and they were fetched
   *         correctly. false otherwise.
   * @throws IOException
   *             on error
   */
  public boolean setNextDoc(int docId) throws IOException {
    ordinalToAssociationMap.clear();
    boolean docContainsAssociations = false;
    try {
      docContainsAssociations = fetchAssociations(docId);
    } catch (IOException e) {
      IOException ioe = new IOException(
          "An Error occured while reading a document's associations payload (docId="
              + docId + ")");
      ioe.initCause(e);
      throw ioe;
    }

    return docContainsAssociations;
  }

  /**
   * Get int association value for the given ordinal. <br>
   * The return is either an int value casted as long if the ordinal has an
   * associated value. Otherwise the returned value would be
   * {@link #NO_ASSOCIATION} which is 'pure long' value (e.g not in the int
   * range of values)
   * 
   * @param ordinal
   *            for which the association value is requested
   * @return the associated int value (encapsulated in a long) if the ordinal
   *         had an associated value, or {@link #NO_ASSOCIATION} otherwise
   */
  public long getAssociation(int ordinal) {
    if (ordinalToAssociationMap.containsKey(ordinal)) {
      return ordinalToAssociationMap.get(ordinal);
    }

    return NO_ASSOCIATION;
  }

  /**
   * Get an iterator over the ordinals which has an association for the
   * document set by {@link #setNextDoc(int)}.
   */
  public IntIterator getAssociatedOrdinals() {
    return ordinalToAssociationMap.keyIterator();
  }

  /**
   * Skips to the given docId, getting the values in pairs of (ordinal, value)
   * and populating the map
   * 
   * @param docId
   *            document id owning the associations
   * @return true if associations were fetched successfully, false otherwise
   * @throws IOException
   *             on error
   */
  private boolean fetchAssociations(int docId) throws IOException {
    // No associations at all? don't bother trying to seek the docID in the
    // posting
    if (!hasAssociations) {
      return false;
    }

    // No associations for this document? well, nothing to decode than,
    // return false
    if (!associationPayloadIter.skipTo(docId)) {
      return false;
    }

    // loop over all the values decoded from the payload in pairs.
    for (;;) {
      // Get the ordinal
      long ordinal = associationPayloadIter.nextCategory();

      // if no ordinal - it's the end of data, break the loop
      if (ordinal > Integer.MAX_VALUE) {
        break;
      }

      // get the associated value
      long association = associationPayloadIter.nextCategory();
      // If we're at this step - it means we have an ordinal, do we have
      // an association for it?
      if (association > Integer.MAX_VALUE) {
        // No association!!! A Broken Pair!! PANIC!
        throw new IOException(
            "ERROR! Associations should come in pairs of (ordinal, value), yet this payload has an odd number of values! (docId="
                + docId + ")");
      }
      // Populate the map with the given ordinal and association pair
      ordinalToAssociationMap.put((int) ordinal, (int) association);
    }

    return true;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime
        * result
        + ((associationPayloadIter == null) ? 0
            : associationPayloadIter.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    
    if (obj == null) {
      return false;
    }
    
    if (getClass() != obj.getClass()) {
      return false;
    }
    
    AssociationsPayloadIterator other = (AssociationsPayloadIterator) obj;
    if (associationPayloadIter == null) {
      if (other.associationPayloadIter != null) {
        return false;
      }
    } else if (!associationPayloadIter.equals(other.associationPayloadIter)) {
      return false;
    }
    return true;
  }

}

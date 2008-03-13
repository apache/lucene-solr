package org.apache.lucene.store.instantiated;

import org.apache.lucene.index.TermFreqVector;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

/**
 * Copyright 2006 The Apache Software Foundation
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
 * Vector space view of a document in an {@link InstantiatedIndexReader}.
 *
 * @see org.apache.lucene.index.TermFreqVector
 */
public class InstantiatedTermFreqVector
    implements TermFreqVector, Serializable {

  private static final long serialVersionUID = 1l;

  private final List<InstantiatedTermDocumentInformation> termDocumentInformations;
  private final String field;
  private final String terms[];
  private final int termFrequencies[];

  public InstantiatedTermFreqVector(InstantiatedDocument document, String field) {
    this.field = field;
    termDocumentInformations = document.getVectorSpace().get(field);
    terms = new String[termDocumentInformations.size()];
    termFrequencies = new int[termDocumentInformations.size()];

    for (int i = 0; i < termDocumentInformations.size(); i++) {
      InstantiatedTermDocumentInformation termDocumentInformation = termDocumentInformations.get(i);
      terms[i] = termDocumentInformation.getTerm().text();
      termFrequencies[i] = termDocumentInformation.getTermPositions().length;
    }
  }

  /**
   * @return The number of the field this vector is associated with
   */
  public String getField() {
    return field;
  }

  public String toString() {
    StringBuffer sb = new StringBuffer();
    sb.append('{');
    sb.append(field).append(": ");
    if (terms != null) {
      for (int i = 0; i < terms.length; i++) {
        if (i > 0) sb.append(", ");
        sb.append(terms[i]).append('/').append(termFrequencies[i]);
      }
    }
    sb.append('}');

    return sb.toString();
  }

  public int size() {
    return terms == null ? 0 : terms.length;
  }

  public String[] getTerms() {
    return terms;
  }

  public int[] getTermFrequencies() {
    return termFrequencies;
  }

  public int indexOf(String termText) {
    if (terms == null)
      return -1;
    int res = Arrays.binarySearch(terms, termText);
    return res >= 0 ? res : -1;
  }

  public int[] indexesOf(String[] termNumbers, int start, int len) {
    // TODO: there must be a more efficient way of doing this.
    //       At least, we could advance the lower bound of the terms array
    //       as we find valid indices. Also, it might be possible to leverage
    //       this even more by starting in the middle of the termNumbers array
    //       and thus dividing the terms array maybe in half with each found index.
    int res[] = new int[len];

    for (int i = 0; i < len; i++) {
      res[i] = indexOf(termNumbers[start + i]);
    }
    return res;
  }

  public List<InstantiatedTermDocumentInformation> getTermDocumentInformations() {
    return termDocumentInformations;
  }

}

package org.apache.lucene.store.instantiated;

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

import org.apache.lucene.index.Term;

import java.io.Serializable;
import java.util.Comparator;
import java.util.Collections;
import java.util.Arrays;

/**
 * A term in the inverted index, coupled to the documents it occurs in.
 *
 * @see org.apache.lucene.index.Term
 */
public class InstantiatedTerm
    implements Serializable {

  private static final long serialVersionUID = 1l;

  public static final Comparator<InstantiatedTerm> comparator = new Comparator<InstantiatedTerm>() {
    public int compare(InstantiatedTerm instantiatedTerm, InstantiatedTerm instantiatedTerm1) {
      return instantiatedTerm.getTerm().compareTo(instantiatedTerm1.getTerm());
    }
  };

  public static final Comparator termComparator = new Comparator() {
    public int compare(Object o, Object o1) {
      return ((InstantiatedTerm)o).getTerm().compareTo((Term)o1);
    }
  };
  
  private Term term;

  /**
   * index of term in InstantiatedIndex
   * @see org.apache.lucene.store.instantiated.InstantiatedIndex#getOrderedTerms() */
  private int termIndex;

  /**
   * @return Term associated with this entry of the index object graph
   */
  public Term getTerm() {
    return term;
  }

  InstantiatedTerm(String field, String text) {
    this.term = new Term(field, text);
  }

//  this could speed up TermDocs.skipTo even more
//  private Map</** document number*/Integer, /** index in associatedDocuments */Integer> associatedDocumentIndexByDocumentNumber = new HashMap<Integer, Integer>();
//
//  public Map</** document number*/Integer, /** index in associatedDocuments */Integer> getAssociatedDocumentIndexByDocumentNumber() {
//    return associatedDocumentIndexByDocumentNumber;
//  }

  /** Ordered by document number */
  private InstantiatedTermDocumentInformation[] associatedDocuments;

  /**
   * Meta data per document in wich this term is occuring.
   * Ordered by document number.
   *
   * @return Meta data per document in wich this term is occuring.
   */
  public InstantiatedTermDocumentInformation[] getAssociatedDocuments() {
    return associatedDocuments;
  }


  /**
   * Meta data per document in wich this term is occuring.
   * Ordered by document number.
   *
   * @param associatedDocuments meta data per document in wich this term is occuring, ordered by document number
   */
  void setAssociatedDocuments(InstantiatedTermDocumentInformation[] associatedDocuments) {
    this.associatedDocuments = associatedDocuments;
  }

  /**
   * Finds index to the first beyond the current whose document number is
   * greater than or equal to <i>target</i>, -1 if there is no such element.
   *
   * @param target the document number to match
   * @return -1 if there is no such element
   */
  public int seekCeilingDocumentInformationIndex(int target) {
    return seekCeilingDocumentInformationIndex(target, 0, getAssociatedDocuments().length);
  }

  /**
   * Finds index to the first beyond the current whose document number is
   * greater than or equal to <i>target</i>, -1 if there is no such element.
   *
   * @param target the document number to match
   * @param startOffset associated documents index start offset
   * @return -1 if there is no such element
   */
  public int seekCeilingDocumentInformationIndex(int target, int startOffset) {
    return seekCeilingDocumentInformationIndex(target, startOffset, getAssociatedDocuments().length);
  }

  /**
   * Finds index to the first beyond the current whose document number is
   * greater than or equal to <i>target</i>, -1 if there is no such element.
   *
   * @param target the document number to match
   * @param startOffset associated documents index start offset
   * @param endPosition associated documents index end position
   * @return -1 if there is no such element
   */
  public int seekCeilingDocumentInformationIndex(int target, int startOffset, int endPosition) {

    int pos = binarySearchAssociatedDocuments(target, startOffset, endPosition - startOffset);

//    int pos = Arrays.binarySearch(getAssociatedDocuments(), target, InstantiatedTermDocumentInformation.doumentNumberIntegerComparator);

    if (pos < 0) {
      pos = -1 - pos;
    }
    if (getAssociatedDocuments().length <= pos) {
      return -1;
    } else {
      return pos;
    }
  }

  public int binarySearchAssociatedDocuments(int target) {
    return binarySearchAssociatedDocuments(target, 0);
  }

  public int binarySearchAssociatedDocuments(int target, int offset) {
    return binarySearchAssociatedDocuments(target, offset, associatedDocuments.length - offset);
  }

  /**
   * @param target value to search for in the array
   * @param offset index of the first valid value in the array
   * @param length number of valid values in the array
   * @return index of an occurrence of key in array, or -(insertionIndex + 1) if key is not contained in array (<i>insertionIndex</i> is then the index at which key could be inserted).
   */
  public int binarySearchAssociatedDocuments(int target, int offset, int length) {

    // implementation originally from http://ochafik.free.fr/blog/?p=106

    if (length == 0) {
      return -1 - offset;
    }
    int min = offset, max = offset + length - 1;
    int minVal = getAssociatedDocuments()[min].getDocument().getDocumentNumber();
    int maxVal = getAssociatedDocuments()[max].getDocument().getDocumentNumber();


    int nPreviousSteps = 0;

    for (; ;) {

      // be careful not to compute key - minVal, for there might be an integer overflow.
      if (target <= minVal) return target == minVal ? min : -1 - min;
      if (target >= maxVal) return target == maxVal ? max : -2 - max;

      assert min != max;

      int pivot;
      // A typical binarySearch algorithm uses pivot = (min + max) / 2.
      // The pivot we use here tries to be smarter and to choose a pivot close to the expectable location of the key.
      // This reduces dramatically the number of steps needed to get to the key.
      // However, it does not work well with a logaritmic distribution of values, for instance.
      // When the key is not found quickly the smart way, we switch to the standard pivot.
      if (nPreviousSteps > 2) {
        pivot = (min + max) >> 1;
        // stop increasing nPreviousSteps from now on
      } else {
        // NOTE: We cannot do the following operations in int precision, because there might be overflows.
        //       long operations are slower than float operations with the hardware this was tested on (intel core duo 2, JVM 1.6.0).
        //       Overall, using float proved to be the safest and fastest approach.
        pivot = min + (int) ((target - (float) minVal) / (maxVal - (float) minVal) * (max - min));
        nPreviousSteps++;
      }

      int pivotVal = getAssociatedDocuments()[pivot].getDocument().getDocumentNumber();

      // NOTE: do not store key - pivotVal because of overflows
      if (target > pivotVal) {
        min = pivot + 1;
        max--;
      } else if (target == pivotVal) {
        return pivot;
      } else {
        min++;
        max = pivot - 1;
      }
      maxVal = getAssociatedDocuments()[max].getDocument().getDocumentNumber();
      minVal = getAssociatedDocuments()[min].getDocument().getDocumentNumber();
    }
  }


  /**
   * Navigates to the view of this occurances of this term in a specific document. 
   *
   * This method is only used by InstantiatedIndex(IndexReader) and
   * should not be optimized for less CPU at the cost of more RAM.
   *
   * @param documentNumber the n:th document in the index
   * @return view of this term from specified document
   */
  public InstantiatedTermDocumentInformation getAssociatedDocument(int documentNumber) {
    int pos = binarySearchAssociatedDocuments(documentNumber);
    return pos < 0 ? null : getAssociatedDocuments()[pos];
  }

  public final String field() {
    return term.field();
  }

  public final String text() {
    return term.text();
  }

  public String toString() {
    return term.toString();
  }


  public int getTermIndex() {
    return termIndex;
  }

  public void setTermIndex(int termIndex) {
    this.termIndex = termIndex;
  }
}

package org.apache.lucene.store.instantiated;

import org.apache.lucene.index.TermVectorOffsetInfo;

import java.io.Serializable;
import java.util.Comparator;

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
 * There is one instance of this class per indexed term in a document
 * and it contains the meta data about each occurance of a term in a docment.
 *
 * It is the inner glue of the inverted index.
 *
 * <pre>
 * [Term]-- {0..*} | {0..*} --(field)[Document]
 *            &lt;&lt;ordered>>
 *                 |
 *    [TermDocumentInformation]
 *       +payloads
 *       +termPositions
 *       +termOffsets
 * </pre>
 * 
 */
public class InstantiatedTermDocumentInformation
    implements Serializable {

  private static final long serialVersionUID = 1l;

  public static final Comparator<InstantiatedTermDocumentInformation> termComparator = new Comparator<InstantiatedTermDocumentInformation>() {
    public int compare(InstantiatedTermDocumentInformation instantiatedTermDocumentInformation, InstantiatedTermDocumentInformation instantiatedTermDocumentInformation1) {
      return instantiatedTermDocumentInformation.getTerm().getTerm().compareTo(instantiatedTermDocumentInformation1.getTerm());
    }
  };

  public static final Comparator<InstantiatedTermDocumentInformation> documentNumberComparator = new Comparator<InstantiatedTermDocumentInformation>() {
    public int compare(InstantiatedTermDocumentInformation instantiatedTermDocumentInformation, InstantiatedTermDocumentInformation instantiatedTermDocumentInformation1) {
      return instantiatedTermDocumentInformation.getDocument().getDocumentNumber().compareTo(instantiatedTermDocumentInformation1.getDocument().getDocumentNumber());
    }
  };

  public static final Comparator doumentNumberIntegerComparator = new Comparator() {
    public int compare(Object o1, Object o2) {
      InstantiatedTermDocumentInformation di = (InstantiatedTermDocumentInformation) o1;
      Integer i = (Integer) o2;
      return di.getDocument().getDocumentNumber().compareTo(i);
    }
  };


  private byte[][] payloads;
  private int[] termPositions;
  private InstantiatedTerm term;
  private InstantiatedDocument document;
  private TermVectorOffsetInfo[] termOffsets;



  public InstantiatedTermDocumentInformation(InstantiatedTerm term, InstantiatedDocument document, int[] termPositions, byte[][] payloads) {
    this.term = term;
    this.document = document;
    this.termPositions = termPositions;
    this.payloads = payloads;
  }


// not quite sure why I wanted this.
//  /**
//   * [Term]--- {0..* ordered} ->[Info]
//   */
//  private int indexFromTerm;


//  public int getIndexFromTerm() {
//    return indexFromTerm;
//  }
//
//  void setIndexFromTerm(int indexFromTerm) {
//    this.indexFromTerm = indexFromTerm;
//  }


  public int[] getTermPositions() {
    return termPositions;
  }


  public byte[][] getPayloads() {
    return payloads;
  }

  public InstantiatedDocument getDocument() {
    return document;
  }



  public InstantiatedTerm getTerm() {
    return term;
  }


  void setTermPositions(int[] termPositions) {
    this.termPositions = termPositions;
  }


  void setTerm(InstantiatedTerm term) {
    this.term = term;
  }

  void setDocument(InstantiatedDocument document) {
    this.document = document;
  }

  public TermVectorOffsetInfo[] getTermOffsets() {
    return termOffsets;
  }

  void setTermOffsets(TermVectorOffsetInfo[] termOffsets) {
    this.termOffsets = termOffsets;
  }
}

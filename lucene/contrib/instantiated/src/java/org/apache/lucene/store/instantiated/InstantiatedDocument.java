package org.apache.lucene.store.instantiated;
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

import org.apache.lucene.document.Document;

import java.util.List;
import java.util.Map;

/**
 * A document in the instantiated index object graph, optionally coupled to the vector space view. 
 *
 * @see org.apache.lucene.document.Document
 */
public class InstantiatedDocument {

  private Document document;

  public InstantiatedDocument() {
    this.document = new Document();
  }


  public InstantiatedDocument(Document document) {
    this.document = document;
  }

  /** this is the unsafe index order document number. */
  private Integer documentNumber;

  /** this is the term vector space view */
  private Map<String /*field name*/, List<InstantiatedTermDocumentInformation>> vectorSpace;

  /**
   * @return position of document in the index.
   */
  public Integer getDocumentNumber() {
    return documentNumber;
  }

  void setDocumentNumber(Integer documentNumber) {
    this.documentNumber = documentNumber;
  }

  public Map</*field name*/ String, List<InstantiatedTermDocumentInformation>> getVectorSpace() {
    return vectorSpace;
  }

  public void setVectorSpace(Map</*field name*/ String, List<InstantiatedTermDocumentInformation>> vectorSpace) {
    this.vectorSpace = vectorSpace;
  }

  public Document getDocument() {
    return document;
  }

  @Override
  public String toString() {
    return document.toString();
  }
}

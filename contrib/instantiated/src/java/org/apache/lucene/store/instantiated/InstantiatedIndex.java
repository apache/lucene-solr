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

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.*;

import java.io.IOException;
import java.io.Serializable;
import java.util.*;

/**
 * Represented as a coupled graph of class instances, this
 * all-in-memory index store implementation delivers search
 * results up to a 100 times faster than the file-centric RAMDirectory
 * at the cost of greater RAM consumption.
 *
 *  WARNING: This contrib is experimental and the APIs may change without warning.
 *
 * There are no read and write locks in this store.
 * {@link InstantiatedIndexReader} {@link InstantiatedIndexReader#isCurrent()} all the time
 * and {@link org.apache.lucene.store.instantiated.InstantiatedIndexWriter}
 * will attempt to update instances of the object graph in memory
 * at the same time as a searcher is reading from it.
 *
 * Consider using InstantiatedIndex as if it was immutable.
 */
public class InstantiatedIndex
    implements Serializable {

  private static final long serialVersionUID = 1l;

  private long version = System.currentTimeMillis();

  private InstantiatedDocument[] documentsByNumber;
  /** todo: this should be a BitSet */
  private Set<Integer> deletedDocuments;

  private Map<String, Map<String, InstantiatedTerm>> termsByFieldAndText;
  private InstantiatedTerm[] orderedTerms;

  private Map<String, byte[]> normsByFieldNameAndDocumentNumber;


  /**
   * Creates an empty instantiated index for you to fill with data using an {@link org.apache.lucene.store.instantiated.InstantiatedIndexWriter}. 
   */
  public InstantiatedIndex() {
    initialize();
  }
  
  void initialize() {
    // todo: clear index without loosing memory (uncouple stuff)
    termsByFieldAndText = new HashMap<String, Map<String, InstantiatedTerm>>();
    orderedTerms = new InstantiatedTerm[0];
    documentsByNumber = new InstantiatedDocument[0];
    normsByFieldNameAndDocumentNumber = new HashMap<String, byte[]>();
    deletedDocuments = new HashSet<Integer>();
  }

  /**
   * Creates a new instantiated index that looks just like the index in a specific state as represented by a reader.
   *
   * @param sourceIndexReader the source index this new instantiated index will be copied from.
   * @throws IOException if the source index is not optimized, or when accesing the source.
   */
  public InstantiatedIndex(IndexReader sourceIndexReader) throws IOException {
    this(sourceIndexReader, null);
  }

  /**
   * Creates a new instantiated index that looks just like the index in a specific state as represented by a reader.
   *
   * @param sourceIndexReader the source index this new instantiated index will be copied from.
   * @param fields fields to be added, or null for all
   * @throws IOException if the source index is not optimized, or when accesing the source.
   */
  public InstantiatedIndex(IndexReader sourceIndexReader, Set<String> fields) throws IOException {

    if (!sourceIndexReader.isOptimized()) {
      throw new IOException("Source index is not optimized.");
    }

    Collection<String> allFieldNames = sourceIndexReader.getFieldNames(IndexReader.FieldOption.ALL);

    initialize();

    documentsByNumber = new InstantiatedDocument[sourceIndexReader.numDocs()];

    // create documents
    for (int i = 0; i < sourceIndexReader.numDocs(); i++) {
      if (!sourceIndexReader.isDeleted(i)) {
        InstantiatedDocument document = new InstantiatedDocument();
        // copy stored fields from source reader
        Document sourceDocument = sourceIndexReader.document(i);
        for (Field field : (List<Field>) sourceDocument.getFields()) {
          if (fields == null || fields.contains(field.name())) {
            document.getDocument().add(field);
          }
        }
        document.setDocumentNumber(i);
        documentsByNumber[i] = document;
        for (Field field : (List<Field>) document.getDocument().getFields()) {
          if (fields == null || fields.contains(field.name())) {
            if (field.isTermVectorStored()) {
              if (document.getVectorSpace() == null) {
                document.setVectorSpace(new HashMap<String, List<InstantiatedTermDocumentInformation>>());
              }
              document.getVectorSpace().put(field.name(), new ArrayList<InstantiatedTermDocumentInformation>());
            }
          }
        }
      }
    }

    // create norms
    for (String fieldName : allFieldNames) {
      if (fields == null || fields.contains(fieldName)) {
        getNormsByFieldNameAndDocumentNumber().put(fieldName, sourceIndexReader.norms(fieldName));
      }
    }

    // create terms
    for (String fieldName : allFieldNames) {
      if (fields == null || fields.contains(fieldName)) {
        getTermsByFieldAndText().put(fieldName, new HashMap<String, InstantiatedTerm>(5000));
      }
    }
    List<InstantiatedTerm> terms = new ArrayList<InstantiatedTerm>(5000 * getTermsByFieldAndText().size());
    TermEnum termEnum = sourceIndexReader.terms();
    while (termEnum.next()) {
      if (fields == null || fields.contains(termEnum.term().field())) { // todo skipto if not using field
        InstantiatedTerm instantiatedTerm = new InstantiatedTerm(termEnum.term().field(), termEnum.term().text());
        getTermsByFieldAndText().get(termEnum.term().field()).put(termEnum.term().text(), instantiatedTerm);
        instantiatedTerm.setTermIndex(terms.size());
        terms.add(instantiatedTerm);
        instantiatedTerm.setAssociatedDocuments(new InstantiatedTermDocumentInformation[termEnum.docFreq()]);
      }
    }
    termEnum.close();
    orderedTerms = terms.toArray(new InstantiatedTerm[terms.size()]);

    // create term-document informations
    for (InstantiatedTerm term : orderedTerms) {
      TermPositions termPositions = sourceIndexReader.termPositions(term.getTerm());
      int position = 0;
      while (termPositions.next()) {
        InstantiatedDocument document = documentsByNumber[termPositions.doc()];

        byte[][] payloads = new byte[termPositions.freq()][];
        int[] positions = new int[termPositions.freq()];
        for (int i = 0; i < termPositions.freq(); i++) {
          positions[i] = termPositions.nextPosition();

          if (termPositions.isPayloadAvailable()) {
            payloads[i] = new byte[termPositions.getPayloadLength()];
            termPositions.getPayload(payloads[i], 0);
          }
        }

        InstantiatedTermDocumentInformation termDocumentInformation = new InstantiatedTermDocumentInformation(term, document, positions, payloads);
        term.getAssociatedDocuments()[position++] = termDocumentInformation;

        if (document.getVectorSpace() != null
            && document.getVectorSpace().containsKey(term.field())) {
          document.getVectorSpace().get(term.field()).add(termDocumentInformation);
        }

//        termDocumentInformation.setIndexFromTerm(indexFromTerm++);
      }
    }

    // load offsets to term-document informations
    for (InstantiatedDocument document : getDocumentsByNumber()) {
      for (Field field : (List<Field>) document.getDocument().getFields()) {
        if (field.isTermVectorStored() && field.isStoreOffsetWithTermVector()) {
          TermPositionVector termPositionVector = (TermPositionVector) sourceIndexReader.getTermFreqVector(document.getDocumentNumber(), field.name());
          if (termPositionVector != null) {
            for (int i = 0; i < termPositionVector.getTerms().length; i++) {
              String token = termPositionVector.getTerms()[i];
              InstantiatedTerm term = findTerm(field.name(), token);
              InstantiatedTermDocumentInformation termDocumentInformation = term.getAssociatedDocument(document.getDocumentNumber());
              termDocumentInformation.setTermOffsets(termPositionVector.getOffsets(i));
            }
          }
        }
      }
    }
  }

  public InstantiatedIndexWriter indexWriterFactory(Analyzer analyzer, boolean create) throws IOException {
    return new InstantiatedIndexWriter(this, analyzer, create);
  }

  public InstantiatedIndexReader indexReaderFactory() throws IOException {
    return new InstantiatedIndexReader(this);
  }

  public void close() throws IOException {
    // todo: decouple everything
  }

  InstantiatedTerm findTerm(Term term) {
    return findTerm(term.field(), term.text());
  }

  InstantiatedTerm findTerm(String field, String text) {
    Map<String, InstantiatedTerm> termsByField = termsByFieldAndText.get(field);
    if (termsByField == null) {
      return null;
    } else {
      return termsByField.get(text);
    }
  }

  public Map<String, Map<String, InstantiatedTerm>> getTermsByFieldAndText() {
    return termsByFieldAndText;
  }


  public InstantiatedTerm[] getOrderedTerms() {
    return orderedTerms;
  }

  public InstantiatedDocument[] getDocumentsByNumber() {
    return documentsByNumber;
  }

  public Map<String, byte[]> getNormsByFieldNameAndDocumentNumber() {
    return normsByFieldNameAndDocumentNumber;
  }

  void setNormsByFieldNameAndDocumentNumber(Map<String, byte[]> normsByFieldNameAndDocumentNumber) {
    this.normsByFieldNameAndDocumentNumber = normsByFieldNameAndDocumentNumber;
  }

  public Set<Integer> getDeletedDocuments() {
    return deletedDocuments;
  }


  void setOrderedTerms(InstantiatedTerm[] orderedTerms) {
    this.orderedTerms = orderedTerms;
  }

  void setDocumentsByNumber(InstantiatedDocument[] documentsByNumber) {
    this.documentsByNumber = documentsByNumber;
  }


  public long getVersion() {
    return version;
  }

  void setVersion(long version) {
    this.version = version;
  }
}

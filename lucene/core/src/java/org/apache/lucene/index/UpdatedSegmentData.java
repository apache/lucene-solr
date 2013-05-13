package org.apache.lucene.index;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeMap;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.FieldsUpdate.Operation;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.util.Bits;

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
 * Holds updates data for a certain segment.
 */
class UpdatedSegmentData {
  
  static final FieldInfos EMPTY_FIELD_INFOS = new FieldInfos(new FieldInfo[0]);
  
  /** Updates mapped by doc ID, for each do sorted list of updates. */
  private TreeMap<Integer,TreeMap<FieldsUpdate, Set<String>>> docIdToUpdatesMap;
  private HashMap<FieldsUpdate, List<Integer>> updatesToDocIdMap;
  private LinkedHashMap<FieldsUpdate,UpdateAtomicReader> allApplied;
  
  private long generation;
  private boolean exactSegment;
  
  private Map<String,FieldGenerationReplacements> fieldGenerationReplacments;
  
  private Iterator<Entry<Integer,TreeMap<FieldsUpdate,Set<String>>>> updatesIterator;
  private int currDocID;
  private int nextDocID;
  private int numDocs;
  private TreeMap<FieldsUpdate,Set<String>> nextUpdate;
  private Analyzer analyzer;
  
  UpdatedSegmentData(SegmentReader reader,
      SortedSet<FieldsUpdate> packetUpdates, boolean exactSegment)
      throws IOException {
    docIdToUpdatesMap = new TreeMap<>();
    updatesToDocIdMap = new HashMap<>();
    this.exactSegment = exactSegment;
    
    allApplied = new LinkedHashMap<>();
    
    for (FieldsUpdate update : packetUpdates) {
      // add updates according to the base reader
      DocsEnum docsEnum = reader.termDocsEnum(update.term);
      if (docsEnum != null) {
        int docId;
        while ((docId = docsEnum.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
          addUpdate(docId, update);
        }
      }
      
      // try applying on previous updates in this packet
      for (Entry<FieldsUpdate,UpdateAtomicReader> applied : allApplied
          .entrySet()) {
        if (applied.getValue().hasTerm(update.term)) {
          List<Integer> list = updatesToDocIdMap.get(applied.getKey());
          if (list != null) {
            for (Integer docId : list) {
              Set<String> ignoredFields = docIdToUpdatesMap.get(docId).get(
                  applied.getKey());
              if (ignoredFields == null
                  || !ignoredFields.contains(update.term.field())) {
                addUpdate(docId, update);
              }
            }
          }
        }
      }
      
      allApplied.put(update, new UpdateAtomicReader(update.directory,
          update.segmentInfo, IOContext.DEFAULT));
    }
    
  }
  
  private void addUpdate(int docId, FieldsUpdate fieldsUpdate) {
    if (exactSegment && docId > fieldsUpdate.docIdUpto) {
      return;
    }
    TreeMap<FieldsUpdate,Set<String>> prevUpdates = docIdToUpdatesMap.get(docId);
    if (prevUpdates == null) {
      prevUpdates = new TreeMap<>();
      docIdToUpdatesMap.put(docId, prevUpdates);
    } else if (fieldsUpdate.operation == Operation.REPLACE_FIELDS) {
      // set ignored fields in previous updates
      for (Entry<FieldsUpdate,Set<String>> addIgnore : prevUpdates.entrySet()) {
        if (addIgnore.getValue() == null) {
          prevUpdates.put(addIgnore.getKey(), new HashSet<>(fieldsUpdate.replacedFields));
        } else {
          addIgnore.getValue().addAll(fieldsUpdate.replacedFields);
        }
      }
    }
    prevUpdates.put(fieldsUpdate, null);
    List<Integer> prevDocIds = updatesToDocIdMap.get(fieldsUpdate);
    if (prevDocIds == null) {
      prevDocIds = new ArrayList<Integer>();
      updatesToDocIdMap.put(fieldsUpdate, prevDocIds);
    }
    prevDocIds.add(docId);
  }
  
  boolean hasUpdates() {
    return !docIdToUpdatesMap.isEmpty();
  }
  
  /**
   * Start writing updates to updates index.
   * 
   * @param generation
   *          The updates generation.
   * @param numDocs
   *          number of documents in the base segment
   */
  void startWriting(long generation, int numDocs) {
    this.generation = generation;
    this.numDocs = numDocs;
    
    updatesIterator = docIdToUpdatesMap.entrySet().iterator();
    currDocID = 0;
    // fetch the first actual updates document if exists
    nextDocUpdate();
  }
  
  /**
   * Fetch next update and set iteration fields appropriately.
   */
  private void nextDocUpdate() {
    if (updatesIterator.hasNext()) {
      Entry<Integer,TreeMap<FieldsUpdate,Set<String>>> docUpdates = updatesIterator.next();
      nextDocID = docUpdates.getKey();
      nextUpdate = docUpdates.getValue();
    } else {
      // no more updates
      nextDocID = numDocs;
    }
  }
  
  Analyzer getAnalyzer() {
    return analyzer;
  }
  
  Map<String,FieldGenerationReplacements> getFieldGenerationReplacments() {
    return fieldGenerationReplacments;
  }
  
  AtomicReader nextReader() throws IOException {
    AtomicReader toReturn = null;
    if (currDocID < nextDocID) {
      // empty documents reader required
      toReturn = new UpdateAtomicReader(nextDocID - currDocID);
      currDocID = nextDocID;
    } else if (currDocID < numDocs) {
      // get the an actual updates reader...
      FieldsUpdate update = nextUpdate.firstEntry().getKey();
      Set<String> ignore = nextUpdate.remove(update);
      toReturn = allApplied.get(update);
      
      // ... and if done for this document remove from updates map
      if (nextUpdate.isEmpty()) {
        updatesIterator.remove();
      }
      
      // add generation replacements if exist
      if (update.replacedFields != null) {
        if (fieldGenerationReplacments == null) {
          fieldGenerationReplacments = new HashMap<String,FieldGenerationReplacements>();
        }
        for (String fieldName : update.replacedFields) {
          FieldGenerationReplacements fieldReplacement = fieldGenerationReplacments
              .get(fieldName);
          if (fieldReplacement == null) {
            fieldReplacement = new FieldGenerationReplacements();
            fieldGenerationReplacments.put(fieldName, fieldReplacement);
          }
          fieldReplacement.set(currDocID, generation);
        }
      }
      // move to next doc id
      nextDocUpdate();
      currDocID++;
    }
    
    return toReturn;
  }
  
  boolean isEmpty() {
    return docIdToUpdatesMap.isEmpty();
  }
  
  private class UpdateAtomicReader extends AtomicReader {
    
    final private SegmentCoreReaders core;
    final private int numDocs;
    
    /**
     * Constructor with fields directory, for actual updates.
     * 
     * @param fieldsDir
     *          Directory with inverted fields.
     * @param segmentInfo
     *          Info of the inverted fields segment.
     * @param context
     *          IOContext to use.
     * @throws IOException
     *           If cannot create the reader.
     */
    UpdateAtomicReader(Directory fieldsDir, SegmentInfo segmentInfo,
        IOContext context) throws IOException {
      core = new SegmentCoreReaders(null, segmentInfo, -1, context, -1);
      numDocs = 1;
    }
    
    /**
     * Constructor with fields directory, for actual updates.
     */
    UpdateAtomicReader(int numDocs) {
      core = null;
      this.numDocs = numDocs;
    }
    
    boolean hasTerm(Term term) throws IOException {
      if (core == null) {
        return false;
      }
      DocsEnum termDocsEnum = termDocsEnum(term);
      if (termDocsEnum == null) {
        return false;
      }
      return termDocsEnum.nextDoc() != DocIdSetIterator.NO_MORE_DOCS;
    }

    @Override
    public Fields fields() throws IOException {
      if (core == null) {
        return null;
      }
      return core.fields;
    }
    
    @Override
    public FieldInfos getFieldInfos() {
      if (core == null) {
        return EMPTY_FIELD_INFOS;
      }
      return core.fieldInfos;
    }
    
    @Override
    public Bits getLiveDocs() {
      return null;
    }
    
    @Override
    public Fields getTermVectors(int docID) throws IOException {
      if (core == null) {
        return null;
      }
      return core.termVectorsLocal.get().get(docID);
    }
    
    @Override
    public int numDocs() {
      return numDocs;
    }
    
    @Override
    public int maxDoc() {
      return numDocs;
    }
    
    @Override
    public void document(int docID, StoredFieldVisitor visitor)
        throws IOException {
      if (core == null) {
        return;
      }
      core.fieldsReaderLocal.get().visitDocument(docID, visitor, null);
    }
    
    @Override
    public boolean hasDeletions() {
      return false;
    }
    
    @Override
    protected void doClose() throws IOException {
      if (core == null) {
        return;
      }
      core.decRef();
    }
    
    @Override
    public NumericDocValues getNumericDocValues(String field)
        throws IOException {
      if (core == null) {
        return null;
      }
      return core.getNumericDocValues(field);
    }
    
    @Override
    public BinaryDocValues getBinaryDocValues(String field) throws IOException {
      if (core == null) {
        return null;
      }
      return core.getBinaryDocValues(field);
    }
    
    @Override
    public SortedDocValues getSortedDocValues(String field) throws IOException {
      if (core == null) {
        return null;
      }
      return core.getSortedDocValues(field);
    }
    
    @Override
    public SortedSetDocValues getSortedSetDocValues(String field)
        throws IOException {
      if (core == null) {
        return null;
      }
      return core.getSortedSetDocValues(field);
    }
    
    @Override
    public NumericDocValues getNormValues(String field) throws IOException {
      if (core == null) {
        return null;
      }
      return core.getNormValues(field);
    }
  }
  
}

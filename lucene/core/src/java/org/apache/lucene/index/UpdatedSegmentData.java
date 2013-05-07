package org.apache.lucene.index;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.PriorityQueue;
import java.util.TreeMap;

import org.apache.lucene.analysis.Analyzer;
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
  private TreeMap<Integer,PriorityQueue<FieldsUpdate>> updatesMap;
  
  /** */
  private long generation;
  
  private Map<String,FieldGenerationReplacements> fieldGenerationReplacments = new HashMap<String,FieldGenerationReplacements>();
  
  private Iterator<Entry<Integer,PriorityQueue<FieldsUpdate>>> updatesIterator;
  private int currDocID;
  private int nextDocID;
  private int numDocs;
  private PriorityQueue<FieldsUpdate> nextUpdate;
  private Analyzer analyzer;
  
  private int termsIndexDivisor;
  
  UpdatedSegmentData() {
    updatesMap = new TreeMap<Integer,PriorityQueue<FieldsUpdate>>();
  }
  
  void addUpdate(int docId, FieldsUpdate fieldsUpdate, boolean checkDocId) {
    if (checkDocId && docId > fieldsUpdate.docIDUpto) {
      return;
    }
    PriorityQueue<FieldsUpdate> prevUpdates = updatesMap.get(docId);
    if (prevUpdates == null) {
      prevUpdates = new PriorityQueue<FieldsUpdate>();
      updatesMap.put(docId, prevUpdates);
    } else {
      System.out.println();
    }
    prevUpdates.add(fieldsUpdate);
  }
  
  boolean hasUpdates() {
    return !updatesMap.isEmpty();
  }
  
  /**
   * Start writing updates to updates index.
   * 
   * @param generation
   *          The updates generation.
   * @param numDocs
   *          number of documents in the base segment
   * @param termsIndexDivisor
   *          Terms index divisor to use in temporary segments
   */
  void startWriting(long generation, int numDocs, int termsIndexDivisor) {
    this.generation = generation;
    this.numDocs = numDocs;
    this.termsIndexDivisor = termsIndexDivisor;
    updatesIterator = updatesMap.entrySet().iterator();
    currDocID = 0;
    fieldGenerationReplacments.clear();
    // fetch the first actual updates document if exists
    nextDocUpdate();
  }
  
  /**
   * Fetch next update and set iteration fields appropriately.
   */
  private void nextDocUpdate() {
    if (updatesIterator.hasNext()) {
      Entry<Integer,PriorityQueue<FieldsUpdate>> docUpdates = updatesIterator
          .next();
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
      FieldsUpdate update = nextUpdate.poll();
      toReturn = new UpdateAtomicReader(update.directory, update.segmentInfo,
          IOContext.DEFAULT);
      
      // ... and if done for this document remove from updates map
      if (nextUpdate.isEmpty()) {
        updatesIterator.remove();
      }
      
      // add generation replacements if exist
      if (update.replacedFields != null) {
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
    return updatesMap.isEmpty();
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
      core = new SegmentCoreReaders(null, segmentInfo, -1, context,
          termsIndexDivisor);
      numDocs = 1;
    }
    
    /**
     * Constructor with fields directory, for actual updates.
     */
    UpdateAtomicReader(int numDocs) {
      core = null;
      this.numDocs = numDocs;
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
    protected void doClose() throws IOException {}

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

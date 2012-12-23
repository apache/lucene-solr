package org.apache.lucene.index;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.FieldsUpdate.Operation;

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
  
  /** Updates mapped by doc ID, for each do sorted list of updates. */
  private TreeMap<Integer,SortedSet<FieldsUpdate>> updatesMap;
  
  public long generation;
  
  private Map<String,FieldGenerationReplacements> fieldGenerationReplacments;
  
  private Iterator<Entry<Integer,SortedSet<FieldsUpdate>>> updatesIterator;
  private int currDocID;
  private int nextDocID;
  private int numDocs;
  private SortedSet<FieldsUpdate> nextUpdate;
  private Analyzer analyzer;
  
  UpdatedSegmentData() {
    updatesMap = new TreeMap<Integer,SortedSet<FieldsUpdate>>();
  }
  
  void addUpdate(int docID, FieldsUpdate update) {
    SortedSet<FieldsUpdate> prevUpdates = updatesMap.get(docID);
    if (prevUpdates == null) {
      prevUpdates = new TreeSet<FieldsUpdate>();
      updatesMap.put(docID, prevUpdates);
    }
    prevUpdates.add(update);
  }
  
  void addUpdates(int docID, FieldsUpdate[] updatesArray) {
    SortedSet<FieldsUpdate> prevUpdates = updatesMap.get(docID);
    if (prevUpdates == null) {
      prevUpdates = new TreeSet<FieldsUpdate>();
      updatesMap.put(docID, prevUpdates);
    }
    for (int i = 0; i < updatesArray.length; i++) {
      prevUpdates.add(updatesArray[i]);
    }
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
   */
  void startWriting(long generation, int numDocs) {
    this.generation = generation;
    this.numDocs = numDocs;
    updatesIterator = updatesMap.entrySet().iterator();
    currDocID = 0;
    // fetch the first actual updates document if exists
    nextDocUpdate();
  }
  
  /**
   * Fetch next update and set iteration fields appropriately.
   */
  private void nextDocUpdate() {
    if (updatesIterator.hasNext()) {
      Entry<Integer,SortedSet<FieldsUpdate>> docUpdates = updatesIterator
          .next();
      nextDocID = docUpdates.getKey();
      nextUpdate = docUpdates.getValue();
    } else {
      // no more updates
      nextDocID = numDocs;
    }
  }
  
  /**
   * Get the next document to put in the updates index, could be an empty
   * document. Updates the analyzer.
   * 
   * @throws IOException
   *           If different analyzers were assigned to field updates affecting
   *           the next document.
   */
  IndexDocument nextDocument() throws IOException {
    IndexDocument toReturn = null;
    if (currDocID < nextDocID) {
      // empty document required
      if (currDocID == numDocs - 1) {
        // add document with stored field for getting right size of segment when
        // reading stored documents
        toReturn = STORED_FIELD_DOCUMENT;
      } else {
        toReturn = EMPTY_DOCUMENT;
      }
    } else if (currDocID < numDocs) {
      // return an actual updates document...
      toReturn = new UpdatesIndexDocument(nextUpdate);
      // ... and fetch the next one if exists
      nextDocUpdate();
    } else {
      // no more documents required
      return null;
    }
    
    currDocID++;
    return toReturn;
  }
  
  Analyzer getAnalyzer() {
    return analyzer;
  }
  
  Map<String,FieldGenerationReplacements> getFieldGenerationReplacments() {
    return fieldGenerationReplacments;
  }
  
  /**
   * An {@link IndexDocument} containing all the updates to a certain document
   * in a stacked segment, taking into account replacements.
   * <p>
   * Constructing an {@link UpdatesIndexDocument} also updates the containing
   * {@link UpdatedSegmentData}'s analyzer and its
   * {@link FieldGenerationReplacements} vectors for the relevant fields.
   */
  private class UpdatesIndexDocument implements IndexDocument {
    
    Map<String,List<IndexableField>> indexablesByField = new HashMap<String,List<IndexableField>>();
    Map<String,List<StorableField>> storablesByField = new HashMap<String,List<StorableField>>();
    
    public UpdatesIndexDocument(SortedSet<FieldsUpdate> fieldsUpdates)
        throws IOException {
      boolean setAnalyzer = true;
      analyzer = null;
      for (FieldsUpdate fieldsUpdate : fieldsUpdates) {
        // set analyzer and check for analyzer conflict
        if (setAnalyzer) {
          analyzer = fieldsUpdate.analyzer;
          setAnalyzer = false;
        } else if (analyzer != fieldsUpdate.analyzer) {
          throw new IOException(
              "two analyzers assigned to one updated document");
        }
        
        if (fieldsUpdate.operation == Operation.REPLACE_FIELDS) {
          // handle fields replacement
          for (IndexableField field : fieldsUpdate.fields.indexableFields()) {
            replaceField(field.name());
          }
          for (StorableField field : fieldsUpdate.fields.storableFields()) {
            replaceField(field.name());
          }
        }
        
        // add new fields
        for (IndexableField field : fieldsUpdate.fields.indexableFields()) {
          List<IndexableField> fieldList = indexablesByField.get(field.name());
          if (fieldList == null) {
            fieldList = new ArrayList<IndexableField>();
            indexablesByField.put(field.name(), fieldList);
          }
          fieldList.add(field);
        }
        for (StorableField field : fieldsUpdate.fields.storableFields()) {
          List<StorableField> fieldList = storablesByField.get(field.name());
          if (fieldList == null) {
            fieldList = new ArrayList<StorableField>();
            storablesByField.put(field.name(), fieldList);
          }
          fieldList.add(field);
        }
      }
    }
    
    private void replaceField(String fieldName) {
      // remove previous fields
      indexablesByField.remove(fieldName);
      storablesByField.remove(fieldName);
      
      // update field generation replacement vector
      if (fieldGenerationReplacments == null) {
        fieldGenerationReplacments = new HashMap<String,FieldGenerationReplacements>();
      }
      FieldGenerationReplacements fieldReplacement = fieldGenerationReplacments
          .get(fieldName);
      if (fieldReplacement == null) {
        fieldReplacement = new FieldGenerationReplacements();
        fieldGenerationReplacments.put(fieldName, fieldReplacement);
      }
      fieldReplacement.set(currDocID, generation);
    }
    
    @Override
    public Iterable<IndexableField> indexableFields() {
      List<IndexableField> indexableFields = new ArrayList<IndexableField>();
      for (List<IndexableField> byField : indexablesByField.values()) {
        indexableFields.addAll(byField);
      }
      return indexableFields;
    }
    
    @Override
    public Iterable<StorableField> storableFields() {
      List<StorableField> storableFields = new ArrayList<StorableField>();
      for (List<StorableField> byField : storablesByField.values()) {
        storableFields.addAll(byField);
      }
      return storableFields;
    }
    
  }
  
  /**
   * An empty document to be used as filler to maintain doc IDs in stacked
   * segments.
   */
  private static final IndexDocument EMPTY_DOCUMENT = new IndexDocument() {
    @Override
    public Iterable<StorableField> storableFields() {
      return Collections.emptyList();
    }
    
    @Override
    public Iterable<IndexableField> indexableFields() {
      return Collections.emptyList();
    }
  };
  
  private static final ArrayList<StorableField> STORED_FIELD_LIST = new ArrayList<StorableField>(
      1);
  static {
    STORED_FIELD_LIST.add(new StoredField("dummy", ""));
  }
  
  /**
   * A document containing only one stored field to be used as the last document
   * in stacked segments.
   */
  private static final IndexDocument STORED_FIELD_DOCUMENT = new IndexDocument() {
    @Override
    public Iterable<StorableField> storableFields() {
      return STORED_FIELD_LIST;
    }
    
    @Override
    public Iterable<IndexableField> indexableFields() {
      return Collections.emptyList();
    }
  };
}

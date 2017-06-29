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
package org.apache.lucene.index;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.lucene.index.DocValuesUpdate.BinaryDocValuesUpdate;
import org.apache.lucene.index.DocValuesUpdate.NumericDocValuesUpdate;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.RamUsageEstimator;

/** Holds buffered deletes and updates, by docID, term or query for a
 *  single segment. This is used to hold buffered pending
 *  deletes and updates against the to-be-flushed segment.  Once the
 *  deletes and updates are pushed (on flush in DocumentsWriter), they
 *  are converted to a {@link FrozenBufferedUpdates} instance and
 *  pushed to the {@link BufferedUpdatesStream}. */

// NOTE: instances of this class are accessed either via a private
// instance on DocumentWriterPerThread, or via sync'd code by
// DocumentsWriterDeleteQueue

class BufferedUpdates {

  /* Rough logic: HashMap has an array[Entry] w/ varying
     load factor (say 2 * POINTER).  Entry is object w/ Term
     key, Integer val, int hash, Entry next
     (OBJ_HEADER + 3*POINTER + INT).  Term is object w/
     String field and String text (OBJ_HEADER + 2*POINTER).
     Term's field is String (OBJ_HEADER + 4*INT + POINTER +
     OBJ_HEADER + string.length*CHAR).
     Term's text is String (OBJ_HEADER + 4*INT + POINTER +
     OBJ_HEADER + string.length*CHAR).  Integer is
     OBJ_HEADER + INT. */
  final static int BYTES_PER_DEL_TERM = 9*RamUsageEstimator.NUM_BYTES_OBJECT_REF + 7*RamUsageEstimator.NUM_BYTES_OBJECT_HEADER + 10*Integer.BYTES;

  /* Rough logic: del docIDs are List<Integer>.  Say list
     allocates ~2X size (2*POINTER).  Integer is OBJ_HEADER
     + int */
  final static int BYTES_PER_DEL_DOCID = 2*RamUsageEstimator.NUM_BYTES_OBJECT_REF + RamUsageEstimator.NUM_BYTES_OBJECT_HEADER + Integer.BYTES;

  /* Rough logic: HashMap has an array[Entry] w/ varying
     load factor (say 2 * POINTER).  Entry is object w/
     Query key, Integer val, int hash, Entry next
     (OBJ_HEADER + 3*POINTER + INT).  Query we often
     undercount (say 24 bytes).  Integer is OBJ_HEADER + INT. */
  final static int BYTES_PER_DEL_QUERY = 5*RamUsageEstimator.NUM_BYTES_OBJECT_REF + 2*RamUsageEstimator.NUM_BYTES_OBJECT_HEADER + 2*Integer.BYTES + 24;

  /* Rough logic: NumericUpdate calculates its actual size,
   * including the update Term and DV field (String). The 
   * per-field map holds a reference to the updated field, and
   * therefore we only account for the object reference and 
   * map space itself. This is incremented when we first see
   * an updated field.
   * 
   * HashMap has an array[Entry] w/ varying load
   * factor (say 2*POINTER). Entry is an object w/ String key, 
   * LinkedHashMap val, int hash, Entry next (OBJ_HEADER + 3*POINTER + INT).
   * 
   * LinkedHashMap (val) is counted as OBJ_HEADER, array[Entry] ref + header, 4*INT, 1*FLOAT,
   * Set (entrySet) (2*OBJ_HEADER + ARRAY_HEADER + 2*POINTER + 4*INT + FLOAT)
   */
  final static int BYTES_PER_NUMERIC_FIELD_ENTRY =
      7*RamUsageEstimator.NUM_BYTES_OBJECT_REF + 3*RamUsageEstimator.NUM_BYTES_OBJECT_HEADER + 
      RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + 5*Integer.BYTES + Float.BYTES;
      
  /* Rough logic: Incremented when we see another Term for an already updated
   * field.
   * LinkedHashMap has an array[Entry] w/ varying load factor 
   * (say 2*POINTER). Entry is an object w/ Term key, NumericUpdate val, 
   * int hash, Entry next, Entry before, Entry after (OBJ_HEADER + 5*POINTER + INT).
   * 
   * Term (key) is counted only as POINTER.
   * NumericUpdate (val) counts its own size and isn't accounted for here.
   */
  final static int BYTES_PER_NUMERIC_UPDATE_ENTRY = 7*RamUsageEstimator.NUM_BYTES_OBJECT_REF + RamUsageEstimator.NUM_BYTES_OBJECT_HEADER + Integer.BYTES;

  /* Rough logic: BinaryUpdate calculates its actual size,
   * including the update Term and DV field (String). The 
   * per-field map holds a reference to the updated field, and
   * therefore we only account for the object reference and 
   * map space itself. This is incremented when we first see
   * an updated field.
   * 
   * HashMap has an array[Entry] w/ varying load
   * factor (say 2*POINTER). Entry is an object w/ String key, 
   * LinkedHashMap val, int hash, Entry next (OBJ_HEADER + 3*POINTER + INT).
   * 
   * LinkedHashMap (val) is counted as OBJ_HEADER, array[Entry] ref + header, 4*INT, 1*FLOAT,
   * Set (entrySet) (2*OBJ_HEADER + ARRAY_HEADER + 2*POINTER + 4*INT + FLOAT)
   */
  final static int BYTES_PER_BINARY_FIELD_ENTRY =
      7*RamUsageEstimator.NUM_BYTES_OBJECT_REF + 3*RamUsageEstimator.NUM_BYTES_OBJECT_HEADER + 
      RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + 5*Integer.BYTES + Float.BYTES;
      
  /* Rough logic: Incremented when we see another Term for an already updated
   * field.
   * LinkedHashMap has an array[Entry] w/ varying load factor 
   * (say 2*POINTER). Entry is an object w/ Term key, BinaryUpdate val, 
   * int hash, Entry next, Entry before, Entry after (OBJ_HEADER + 5*POINTER + INT).
   * 
   * Term (key) is counted only as POINTER.
   * BinaryUpdate (val) counts its own size and isn't accounted for here.
   */
  final static int BYTES_PER_BINARY_UPDATE_ENTRY = 7*RamUsageEstimator.NUM_BYTES_OBJECT_REF + RamUsageEstimator.NUM_BYTES_OBJECT_HEADER + Integer.BYTES;
  
  final AtomicInteger numTermDeletes = new AtomicInteger();
  final AtomicInteger numNumericUpdates = new AtomicInteger();
  final AtomicInteger numBinaryUpdates = new AtomicInteger();

  final Map<Term,Integer> deleteTerms = new HashMap<>();
  final Map<Query,Integer> deleteQueries = new HashMap<>();
  final List<Integer> deleteDocIDs = new ArrayList<>();

  // Map<dvField,Map<updateTerm,NumericUpdate>>
  // For each field we keep an ordered list of NumericUpdates, key'd by the
  // update Term. LinkedHashMap guarantees we will later traverse the map in
  // insertion order (so that if two terms affect the same document, the last
  // one that came in wins), and helps us detect faster if the same Term is
  // used to update the same field multiple times (so we later traverse it
  // only once).
  final Map<String,LinkedHashMap<Term,NumericDocValuesUpdate>> numericUpdates = new HashMap<>();
  
  // Map<dvField,Map<updateTerm,BinaryUpdate>>
  // For each field we keep an ordered list of BinaryUpdates, key'd by the
  // update Term. LinkedHashMap guarantees we will later traverse the map in
  // insertion order (so that if two terms affect the same document, the last
  // one that came in wins), and helps us detect faster if the same Term is
  // used to update the same field multiple times (so we later traverse it
  // only once).
  final Map<String,LinkedHashMap<Term,BinaryDocValuesUpdate>> binaryUpdates = new HashMap<>();

  public static final Integer MAX_INT = Integer.valueOf(Integer.MAX_VALUE);

  final AtomicLong bytesUsed;

  private final static boolean VERBOSE_DELETES = false;

  long gen;

  final String segmentName;
  
  public BufferedUpdates(String segmentName) {
    this.bytesUsed = new AtomicLong();
    this.segmentName = segmentName;
  }

  @Override
  public String toString() {
    if (VERBOSE_DELETES) {
      return "gen=" + gen + " numTerms=" + numTermDeletes + ", deleteTerms=" + deleteTerms
        + ", deleteQueries=" + deleteQueries + ", deleteDocIDs=" + deleteDocIDs + ", numericUpdates=" + numericUpdates
        + ", binaryUpdates=" + binaryUpdates + ", bytesUsed=" + bytesUsed;
    } else {
      String s = "gen=" + gen;
      if (numTermDeletes.get() != 0) {
        s += " " + numTermDeletes.get() + " deleted terms (unique count=" + deleteTerms.size() + ")";
      }
      if (deleteQueries.size() != 0) {
        s += " " + deleteQueries.size() + " deleted queries";
      }
      if (deleteDocIDs.size() != 0) {
        s += " " + deleteDocIDs.size() + " deleted docIDs";
      }
      if (numNumericUpdates.get() != 0) {
        s += " " + numNumericUpdates.get() + " numeric updates (unique count=" + numericUpdates.size() + ")";
      }
      if (numBinaryUpdates.get() != 0) {
        s += " " + numBinaryUpdates.get() + " binary updates (unique count=" + binaryUpdates.size() + ")";
      }
      if (bytesUsed.get() != 0) {
        s += " bytesUsed=" + bytesUsed.get();
      }

      return s;
    }
  }

  public void addQuery(Query query, int docIDUpto) {
    Integer current = deleteQueries.put(query, docIDUpto);
    // increment bytes used only if the query wasn't added so far.
    if (current == null) {
      bytesUsed.addAndGet(BYTES_PER_DEL_QUERY);
    }
  }

  public void addDocID(int docID) {
    deleteDocIDs.add(Integer.valueOf(docID));
    bytesUsed.addAndGet(BYTES_PER_DEL_DOCID);
  }

  public void addTerm(Term term, int docIDUpto) {
    Integer current = deleteTerms.get(term);
    if (current != null && docIDUpto < current) {
      // Only record the new number if it's greater than the
      // current one.  This is important because if multiple
      // threads are replacing the same doc at nearly the
      // same time, it's possible that one thread that got a
      // higher docID is scheduled before the other
      // threads.  If we blindly replace than we can
      // incorrectly get both docs indexed.
      return;
    }

    deleteTerms.put(term, Integer.valueOf(docIDUpto));
    // note that if current != null then it means there's already a buffered
    // delete on that term, therefore we seem to over-count. this over-counting
    // is done to respect IndexWriterConfig.setMaxBufferedDeleteTerms.
    numTermDeletes.incrementAndGet();
    if (current == null) {
      bytesUsed.addAndGet(BYTES_PER_DEL_TERM + term.bytes.length + (Character.BYTES * term.field().length()));
    }
  }
 
  public void addNumericUpdate(NumericDocValuesUpdate update, int docIDUpto) {
    LinkedHashMap<Term,NumericDocValuesUpdate> fieldUpdates = numericUpdates.get(update.field);
    if (fieldUpdates == null) {
      fieldUpdates = new LinkedHashMap<>();
      numericUpdates.put(update.field, fieldUpdates);
      bytesUsed.addAndGet(BYTES_PER_NUMERIC_FIELD_ENTRY);
    }
    final NumericDocValuesUpdate current = fieldUpdates.get(update.term);
    if (current != null && docIDUpto < current.docIDUpto) {
      // Only record the new number if it's greater than or equal to the current
      // one. This is important because if multiple threads are replacing the
      // same doc at nearly the same time, it's possible that one thread that
      // got a higher docID is scheduled before the other threads.
      return;
    }

    update.docIDUpto = docIDUpto;
    // since it's a LinkedHashMap, we must first remove the Term entry so that
    // it's added last (we're interested in insertion-order).
    if (current != null) {
      fieldUpdates.remove(update.term);
    }
    fieldUpdates.put(update.term, update);
    numNumericUpdates.incrementAndGet();
    if (current == null) {
      bytesUsed.addAndGet(BYTES_PER_NUMERIC_UPDATE_ENTRY + update.sizeInBytes());
    }
  }
  
  public void addBinaryUpdate(BinaryDocValuesUpdate update, int docIDUpto) {
    LinkedHashMap<Term,BinaryDocValuesUpdate> fieldUpdates = binaryUpdates.get(update.field);
    if (fieldUpdates == null) {
      fieldUpdates = new LinkedHashMap<>();
      binaryUpdates.put(update.field, fieldUpdates);
      bytesUsed.addAndGet(BYTES_PER_BINARY_FIELD_ENTRY);
    }
    final BinaryDocValuesUpdate current = fieldUpdates.get(update.term);
    if (current != null && docIDUpto < current.docIDUpto) {
      // Only record the new number if it's greater than or equal to the current
      // one. This is important because if multiple threads are replacing the
      // same doc at nearly the same time, it's possible that one thread that
      // got a higher docID is scheduled before the other threads.
      return;
    }
    
    update.docIDUpto = docIDUpto;
    // since it's a LinkedHashMap, we must first remove the Term entry so that
    // it's added last (we're interested in insertion-order).
    if (current != null) {
      fieldUpdates.remove(update.term);
    }
    fieldUpdates.put(update.term, update);
    numBinaryUpdates.incrementAndGet();
    if (current == null) {
      bytesUsed.addAndGet(BYTES_PER_BINARY_UPDATE_ENTRY + update.sizeInBytes());
    }
  }

  void clearDeleteTerms() {
    deleteTerms.clear();
    numTermDeletes.set(0);
  }
  
  void clear() {
    deleteTerms.clear();
    deleteQueries.clear();
    deleteDocIDs.clear();
    numericUpdates.clear();
    binaryUpdates.clear();
    numTermDeletes.set(0);
    numNumericUpdates.set(0);
    numBinaryUpdates.set(0);
    bytesUsed.set(0);
  }
  
  boolean any() {
    return deleteTerms.size() > 0 || deleteDocIDs.size() > 0 || deleteQueries.size() > 0 || numericUpdates.size() > 0 || binaryUpdates.size() > 0;
  }
}

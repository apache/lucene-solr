package org.apache.lucene.index;

import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.index.NumericDocValuesFieldUpdates;
import org.apache.lucene.search.DocIdSetIterator;

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
 * Holds updates of a single DocValues field, for a set of documents.
 * 
 * @lucene.experimental
 */
abstract class DocValuesFieldUpdates {
  
  static enum Type { NUMERIC, BINARY }
  
  /**
   * An iterator over documents and their updated values. Only documents with
   * updates are returned by this iterator, and the documents are returned in
   * increasing order.
   */
  static abstract class Iterator {
    
    /**
     * Returns the next document which has an update, or
     * {@link DocIdSetIterator#NO_MORE_DOCS} if there are no more documents to
     * return.
     */
    abstract int nextDoc();
    
    /** Returns the current document this iterator is on. */
    abstract int doc();
    
    /**
     * Returns the value of the document returned from {@link #nextDoc()}. A
     * {@code null} value means that it was unset for this document.
     */
    abstract Object value();
    
    /**
     * Reset the iterator's state. Should be called before {@link #nextDoc()}
     * and {@link #value()}.
     */
    abstract void reset();
    
  }

  static class Container {
  
    final Map<String,NumericDocValuesFieldUpdates> numericDVUpdates = new HashMap<>();
    final Map<String,BinaryDocValuesFieldUpdates> binaryDVUpdates = new HashMap<>();
    
    boolean any() {
      for (NumericDocValuesFieldUpdates updates : numericDVUpdates.values()) {
        if (updates.any()) {
          return true;
        }
      }
      for (BinaryDocValuesFieldUpdates updates : binaryDVUpdates.values()) {
        if (updates.any()) {
          return true;
        }
      }
      return false;
    }
    
    int size() {
      return numericDVUpdates.size() + binaryDVUpdates.size();
    }
    
    DocValuesFieldUpdates getUpdates(String field, Type type) {
      switch (type) {
        case NUMERIC:
          return numericDVUpdates.get(field);
        case BINARY:
          return binaryDVUpdates.get(field);
        default:
          throw new IllegalArgumentException("unsupported type: " + type);
      }
    }
    
    DocValuesFieldUpdates newUpdates(String field, Type type, int maxDoc) {
      switch (type) {
        case NUMERIC:
          assert numericDVUpdates.get(field) == null;
          NumericDocValuesFieldUpdates numericUpdates = new NumericDocValuesFieldUpdates(field, maxDoc);
          numericDVUpdates.put(field, numericUpdates);
          return numericUpdates;
        case BINARY:
          assert binaryDVUpdates.get(field) == null;
          BinaryDocValuesFieldUpdates binaryUpdates = new BinaryDocValuesFieldUpdates(field, maxDoc);
          binaryDVUpdates.put(field, binaryUpdates);
          return binaryUpdates;
        default:
          throw new IllegalArgumentException("unsupported type: " + type);
      }
    }
    
    @Override
    public String toString() {
      return "numericDVUpdates=" + numericDVUpdates + " binaryDVUpdates=" + binaryDVUpdates;
    }
  }
  
  final String field;
  final Type type;
  
  protected DocValuesFieldUpdates(String field, Type type) {
    this.field = field;
    this.type = type;
  }
  
  /**
   * Add an update to a document. For unsetting a value you should pass
   * {@code null}.
   */
  public abstract void add(int doc, Object value);
  
  /**
   * Returns an {@link Iterator} over the updated documents and their
   * values.
   */
  public abstract Iterator iterator();
  
  /**
   * Merge with another {@link DocValuesFieldUpdates}. This is called for a
   * segment which received updates while it was being merged. The given updates
   * should override whatever updates are in that instance.
   */
  public abstract void merge(DocValuesFieldUpdates other);

  /** Returns true if this instance contains any updates. 
   * @return TODO*/
  public abstract boolean any();
  
}

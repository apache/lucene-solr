package org.apache.lucene.index;

import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.InPlaceMergeSorter;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.packed.PackedInts;
import org.apache.lucene.util.packed.PagedGrowableWriter;
import org.apache.lucene.util.packed.PagedMutable;

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
 * Holds numeric values updates of documents, of a single
 * {@link NumericDocValuesField}.
 * 
 * @lucene.experimental
 */
interface NumericFieldUpdates {
  
  /**
   * An iterator over documents and their updated values. Only documents with
   * updates are returned by this iterator, and the documents are returned in
   * increasing order.
   */
  static abstract class UpdatesIterator {
    
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
    abstract Long value();
    
    /**
     * Reset the iterator's state. Should be called before {@link #nextDoc()}
     * and {@link #value()}.
     */
    abstract void reset();
    
  }
  
  /**
   * A {@link NumericFieldUpdates} which holds the updated documents and values
   * in packed structures. Only supports up to 2B entries (docs and values)
   * since we need to sort the docs/values and the Sorter interfaces currently
   * only take integer indexes.
   */
  static final class PackedNumericFieldUpdates implements NumericFieldUpdates {

    private FixedBitSet docsWithField;
    private PagedMutable docs;
    private PagedGrowableWriter values;
    private int size;
    
    public PackedNumericFieldUpdates(int maxDoc) {
      docsWithField = new FixedBitSet(64);
      docs = new PagedMutable(1, 1024, PackedInts.bitsRequired(maxDoc - 1), PackedInts.COMPACT);
      values = new PagedGrowableWriter(1, 1024, 1, PackedInts.FAST);
      size = 0;
    }
    
    @Override
    public void add(int doc, Long value) {
      assert value != null;
      // TODO: if the Sorter interface changes to take long indexes, we can remove that limitation
      if (size == Integer.MAX_VALUE) {
        throw new IllegalStateException("cannot support more than Integer.MAX_VALUE doc/value entries");
      }

      // grow the structures to have room for more elements
      if (docs.size() == size) {
        docs = docs.grow(size + 1);
        values = values.grow(size + 1);
        int numWords = (int) (docs.size() >> 6);
        if (docsWithField.getBits().length <= numWords) {
          numWords = ArrayUtil.oversize(numWords + 1, RamUsageEstimator.NUM_BYTES_LONG);
          docsWithField = new FixedBitSet(docsWithField, numWords << 6);
        }
      }
      
      if (value != NumericUpdate.MISSING) {
        // only mark the document as having a value in that field if the value wasn't set to null (MISSING)
        docsWithField.set(size);
      }
      
      docs.set(size, doc);
      values.set(size, value.longValue());
      ++size;
    }

    @Override
    public UpdatesIterator getUpdates() {
      final PagedMutable docs = this.docs;
      final PagedGrowableWriter values = this.values;
      final FixedBitSet docsWithField = this.docsWithField;
      new InPlaceMergeSorter() {
        @Override
        protected void swap(int i, int j) {
          long tmpDoc = docs.get(j);
          docs.set(j, docs.get(i));
          docs.set(i, tmpDoc);
          
          long tmpVal = values.get(j);
          values.set(j, values.get(i));
          values.set(i, tmpVal);
          
          boolean tmpBool = docsWithField.get(j);
          if (docsWithField.get(i)) {
            docsWithField.set(j);
          } else {
            docsWithField.clear(j);
          }
          if (tmpBool) {
            docsWithField.set(i);
          } else {
            docsWithField.clear(i);
          }
        }
        
        @Override
        protected int compare(int i, int j) {
          int x = (int) docs.get(i);
          int y = (int) docs.get(j);
          return (x < y) ? -1 : ((x == y) ? 0 : 1);
        }
      }.sort(0, size);

      final int size = this.size;
      return new UpdatesIterator() {
        private long idx = 0; // long so we don't overflow if size == Integer.MAX_VALUE
        private int doc = -1;
        private Long value = null;
        
        @Override
        Long value() {
          return value;
        }
        
        @Override
        int nextDoc() {
          if (idx >= size) {
            value = null;
            return doc = DocIdSetIterator.NO_MORE_DOCS;
          }
          doc = (int) docs.get(idx);
          ++idx;
          while (idx < size && docs.get(idx) == doc) {
            ++idx;
          }
          if (!docsWithField.get((int) (idx - 1))) {
            value = null;
          } else {
            // idx points to the "next" element
            value = Long.valueOf(values.get(idx - 1));
          }
          return doc;
        }
        
        @Override
        int doc() {
          return doc;
        }
        
        @Override
        void reset() {
          doc = -1;
          value = null;
          idx = 0;
        }
      };
    }

    @Override
    public void merge(NumericFieldUpdates other) {
      if (other instanceof PackedNumericFieldUpdates) {
        PackedNumericFieldUpdates packedOther = (PackedNumericFieldUpdates) other;
        if (size  + packedOther.size > Integer.MAX_VALUE) {
          throw new IllegalStateException(
              "cannot support more than Integer.MAX_VALUE doc/value entries; size="
                  + size + " other.size=" + packedOther.size);
        }
        docs = docs.grow(size + packedOther.size);
        values = values.grow(size + packedOther.size);
        int numWords = (int) (docs.size() >> 6);
        if (docsWithField.getBits().length <= numWords) {
          numWords = ArrayUtil.oversize(numWords + 1, RamUsageEstimator.NUM_BYTES_LONG);
          docsWithField = new FixedBitSet(docsWithField, numWords << 6);
        }
        for (int i = 0; i < packedOther.size; i++) {
          int doc = (int) packedOther.docs.get(i);
          if (packedOther.docsWithField.get(i)) {
            docsWithField.set(size);
          }
          docs.set(size, doc);
          values.set(size, packedOther.values.get(i));
          ++size;
        }
      } else {
        UpdatesIterator iter = other.getUpdates();
        int doc;
        while ((doc = iter.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
          Long value = iter.value();
          if (value == null) {
            value = NumericUpdate.MISSING;
          }
          add(doc, value);
        }
      }
    }
    
  }
  
  /**
   * Add an update to a document. For unsetting a value you should pass
   * {@link NumericUpdate#MISSING} instead of {@code null}.
   */
  public void add(int doc, Long value);
  
  /**
   * Returns an {@link UpdatesIterator} over the updated documents and their
   * values.
   */
  public UpdatesIterator getUpdates();
  
  /**
   * Merge with another {@link NumericFieldUpdates}. This is called for a
   * segment which received updates while it was being merged. The given updates
   * should override whatever numeric updates are in that instance.
   */
  public void merge(NumericFieldUpdates other);
  
}

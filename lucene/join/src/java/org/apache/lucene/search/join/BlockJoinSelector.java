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
package org.apache.lucene.search.join;

import java.io.IOException;

import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedNumericSelector;
import org.apache.lucene.search.SortedSetSelector;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;

/** Select a value from a block of documents.
 *  @lucene.internal */
public class BlockJoinSelector {

  private BlockJoinSelector() {}

  /** Type of selection to perform. If none of the documents in the block have
   *  a value then no value will be selected. */
  public enum Type {
    /** Only consider the minimum value from the block when sorting. */
    MIN,
    /** Only consider the maximum value from the block when sorting. */
    MAX;
  }

  /** Return a {@link Bits} instance that returns true if, and only if, any of
   *  the children of the given parent document has a value. */
  public static Bits wrap(final Bits docsWithValue, BitSet parents, BitSet children) {
    return new Bits() {

      @Override
      public boolean get(int docID) {
        assert parents.get(docID) : "this selector may only be used on parent documents";

        if (docID == 0) {
          // no children
          return false;
        }

        final int firstChild = parents.prevSetBit(docID - 1) + 1;
        for (int child = children.nextSetBit(firstChild); child < docID; child = children.nextSetBit(child + 1)) {
          if (docsWithValue.get(child)) {
            return true;
          }
        }
        return false;
      }

      @Override
      public int length() {
        return docsWithValue.length();
      }

    };
  }

  /** Wraps the provided {@link SortedSetDocValues} in order to only select
   *  one value per parent among its {@code children} using the configured
   *  {@code selection} type. */
  public static SortedDocValues wrap(SortedSetDocValues sortedSet, Type selection, BitSet parents, BitSet children) {
    SortedDocValues values;
    switch (selection) {
      case MIN:
        values = SortedSetSelector.wrap(sortedSet, SortedSetSelector.Type.MIN);
        break;
      case MAX:
        values = SortedSetSelector.wrap(sortedSet, SortedSetSelector.Type.MAX);
        break;
      default:
        throw new AssertionError();
    }
    return wrap(values, selection, parents, children);
  }

  /** Wraps the provided {@link SortedDocValues} in order to only select
   *  one value per parent among its {@code children} using the configured
   *  {@code selection} type. */
  public static SortedDocValues wrap(final SortedDocValues values, Type selection, BitSet parents, BitSet children) {
    if (values.docID() != -1) {
      throw new IllegalArgumentException("values iterator was already consumed: values.docID=" + values.docID());
    }
    return new SortedDocValues() {

      private int ord = -1;
      private int docID = -1;

      @Override
      public int docID() {
        return docID;
      }

      @Override
      public int nextDoc() throws IOException {
        assert docID != NO_MORE_DOCS;
        
        if (values.docID() == -1) {
          if (values.nextDoc() == NO_MORE_DOCS) {
            docID = NO_MORE_DOCS;
            return docID;
          }
        }

        if (values.docID() == NO_MORE_DOCS) {
          docID = NO_MORE_DOCS;
          return docID;
        }
        
        int nextParentDocID = parents.nextSetBit(values.docID());
        ord = values.ordValue();

        while (true) {
          int childDocID = values.nextDoc();
          assert childDocID != nextParentDocID;
          if (childDocID > nextParentDocID) {
            break;
          }
          if (children.get(childDocID) == false) {
            continue;
          }
          if (selection == Type.MIN) {
            ord = Math.min(ord, values.ordValue());
          } else if (selection == Type.MAX) {
            ord = Math.max(ord, values.ordValue());
          } else {
            throw new AssertionError();
          }
        }

        docID = nextParentDocID;
        return docID;
      }

      @Override
      public int advance(int target) throws IOException {
        if (target >= parents.length()) {
          docID = NO_MORE_DOCS;
          return docID;
        }
        if (target == 0) {
          assert docID() == -1;
          return nextDoc();
        }
        int prevParentDocID = parents.prevSetBit(target-1);
        if (values.docID() <= prevParentDocID) {
          values.advance(prevParentDocID+1);
        }
        return nextDoc();
      }

      @Override
      public boolean advanceExact(int targetParentDocID) throws IOException {
        if (targetParentDocID < docID) {
          throw new IllegalArgumentException("target must be after the current document: current=" + docID + " target=" + targetParentDocID);
        }
        int previousDocId = docID;
        docID = targetParentDocID;
        if (targetParentDocID == previousDocId) {
          return ord != -1;
        }
        docID = targetParentDocID;
        ord = -1;
        if (parents.get(targetParentDocID) == false) {
          return false;
        }
        int prevParentDocId = docID == 0 ? -1 : parents.prevSetBit(docID - 1);
        int childDoc = values.docID();
        if (childDoc <= prevParentDocId) {
          childDoc = values.advance(prevParentDocId + 1);
        }
        if (childDoc >= docID) {
          return false;
        }
        
        boolean hasValue = false;
        for (int doc = values.docID(); doc < docID; doc = values.nextDoc()) {
          if (children.get(doc)) {
            ord = values.ordValue();
            hasValue = true;
            values.nextDoc();
            break;
          }
        }
        if (hasValue == false) {
          return false;
        }

        for (int doc = values.docID(); doc < docID; doc = values.nextDoc()) {
          if (children.get(doc)) {
            switch (selection) {
              case MIN:
                ord = Math.min(ord, values.ordValue());
                break;
              case MAX:
                ord = Math.max(ord, values.ordValue());
                break;
              default:
                throw new AssertionError();
            }
          }
        }
        return true;
      }

      @Override
      public int ordValue() {
        return ord;
      }

      @Override
      public BytesRef lookupOrd(int ord) throws IOException {
        return values.lookupOrd(ord);
      }

      @Override
      public int getValueCount() {
        return values.getValueCount();
      }

      @Override
      public long cost() {
        return values.cost();
      }
    };
  }

  /** Wraps the provided {@link SortedNumericDocValues} in order to only select
   *  one value per parent among its {@code children} using the configured
   *  {@code selection} type. */
  public static NumericDocValues wrap(SortedNumericDocValues sortedNumerics, Type selection, BitSet parents, BitSet children) {
    NumericDocValues values;
    switch (selection) {
      case MIN:
        values = SortedNumericSelector.wrap(sortedNumerics, SortedNumericSelector.Type.MIN, SortField.Type.LONG);
        break;
      case MAX:
        values = SortedNumericSelector.wrap(sortedNumerics, SortedNumericSelector.Type.MAX, SortField.Type.LONG);
        break;
      default:
        throw new AssertionError();
    }
    return wrap(values, selection, parents, children);
  }

  /** Wraps the provided {@link NumericDocValues}, iterating over only
   *  child documents, in order to only select one value per parent among
   *  its {@code children} using the configured {@code selection} type. */
  public static NumericDocValues wrap(final NumericDocValues values, Type selection, BitSet parents, BitSet children) {
    return new NumericDocValues() {

      private int parentDocID = -1;
      private long value;

      @Override
      public int nextDoc() throws IOException {

        if (parentDocID == -1) {
          values.nextDoc();
        }

        while (true) {

          // TODO: make this crazy loop more efficient

          int childDocID = values.docID();
          if (childDocID == NO_MORE_DOCS) {
            parentDocID = NO_MORE_DOCS;
            return parentDocID;
          }
          if (children.get(childDocID) == false) {
            values.nextDoc();
            continue;
          }

          assert parents.get(childDocID) == false;
        
          parentDocID = parents.nextSetBit(childDocID);
          value = values.longValue();

          while (true) {
            childDocID = values.nextDoc();
            assert childDocID != parentDocID;
            if (childDocID > parentDocID) {
              break;
            }

            switch (selection) {
            case MIN:
              value = Math.min(value, values.longValue());
              break;
            case MAX:
              value = Math.max(value, values.longValue());
              break;
            default:
              throw new AssertionError();
            }
          }

          break;
        }

        return parentDocID;
      }

      @Override
      public int advance(int targetParentDocID) throws IOException {
        if (targetParentDocID <= parentDocID) {
          throw new IllegalArgumentException("target must be after the current document: current=" + parentDocID + " target=" + targetParentDocID);
        }

        if (targetParentDocID == 0) {
          return nextDoc();
        }
        
        int firstChild = parents.prevSetBit(targetParentDocID - 1) + 1;
        if (values.advance(firstChild) == NO_MORE_DOCS) {
          parentDocID = NO_MORE_DOCS;
          return parentDocID;
        } else {
          return nextDoc();
        }
      }

      @Override
      public boolean advanceExact(int targetParentDocID) throws IOException {
        if (targetParentDocID <= parentDocID) {
          throw new IllegalArgumentException("target must be after the current document: current=" + parentDocID + " target=" + targetParentDocID);
        }
        parentDocID = targetParentDocID;
        if (parents.get(targetParentDocID) == false) {
          return false;
        }
        int prevParentDocId = parentDocID == 0 ? -1 : parents.prevSetBit(parentDocID - 1);
        int childDoc = values.docID();
        if (childDoc <= prevParentDocId) {
          childDoc = values.advance(prevParentDocId + 1);
        }
        if (childDoc >= parentDocID) {
          return false;
        }
        
        boolean hasValue = false;
        for (int doc = values.docID(); doc < parentDocID; doc = values.nextDoc()) {
          if (children.get(doc)) {
            value = values.longValue();
            hasValue = true;
            values.nextDoc();
            break;
          }
        }
        if (hasValue == false) {
          return false;
        }

        for (int doc = values.docID(); doc < parentDocID; doc = values.nextDoc()) {
          if (children.get(doc)) {
            switch (selection) {
              case MIN:
                value = Math.min(value, values.longValue());
                break;
              case MAX:
                value = Math.max(value, values.longValue());
                break;
              default:
                throw new AssertionError();
            }
          }
        }
        return true;
      }

      @Override
      public long longValue() {
        return value;
      }
      
      @Override
      public int docID() {
        return parentDocID;
      }      

      @Override
      public long cost() {
        return values.cost();
      }      
    };
  }

}

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
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.join.BlockJoinSelector.Type;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.BytesRef;

class ToParentDocValues extends DocIdSetIterator {

  interface Accumulator{
    void reset() throws IOException;
    void increment() throws IOException;
  }

  private static final class SortedDVs extends SortedDocValues implements Accumulator{
    private final SortedDocValues values;
    private final BlockJoinSelector.Type selection;
    private int ord = -1;
    private final ToParentDocValues iter;
  
    private SortedDVs(SortedDocValues values, BlockJoinSelector.Type selection, BitSet parents, DocIdSetIterator children) {
      this.values = values;
      this.selection = selection;
      this.iter = new ToParentDocValues(values,parents, children, this);
    }
  
    @Override
    public int docID() {
      return iter.docID();
    }
  
    @Override
    public
    void reset() throws IOException {
      ord = values.ordValue();
    }
  
    @Override
    public
    void increment() throws IOException {
      if (selection == BlockJoinSelector.Type.MIN) {
        ord = Math.min(ord, values.ordValue());
      } else if (selection == BlockJoinSelector.Type.MAX) {
        ord = Math.max(ord, values.ordValue());
      } else {
        throw new AssertionError();
      }
    }
  
    @Override
    public int nextDoc() throws IOException {
      return iter.nextDoc();
    }
  
    @Override
    public int advance(int target) throws IOException {
      return iter.advance(target);
    }
  
    @Override
    public boolean advanceExact(int targetParentDocID) throws IOException {
      return iter.advanceExact(targetParentDocID);
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
  }

  static private final class NumDV extends NumericDocValues implements Accumulator{
    private final NumericDocValues values;
    private long value;
    private final BlockJoinSelector.Type selection;
  
    private final ToParentDocValues iter;
  
    private NumDV(NumericDocValues values, BlockJoinSelector.Type selection, BitSet parents, DocIdSetIterator children) {
      this.values = values;
      this.selection = selection;
      iter = new ToParentDocValues(values, parents, children, this);
    }
  
    @Override
    public void reset() throws IOException {
      value = values.longValue();
    }
    
    @Override
    public void increment() throws IOException {
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
    
    @Override
    public int nextDoc() throws IOException {
      return iter.nextDoc();
    }
  
    @Override
    public int advance(int targetParentDocID) throws IOException {
      return iter.advance(targetParentDocID);
    }
  
    @Override
    public boolean advanceExact(int targetParentDocID) throws IOException {
      return iter.advanceExact(targetParentDocID);
    }
  
    @Override
    public long longValue() {
      return value;
    }
  
    @Override
    public int docID() {
      return iter.docID();
    }
  
    @Override
    public long cost() {
      return values.cost();
    }
  }

  private ToParentDocValues(DocIdSetIterator values, BitSet parents, DocIdSetIterator children, Accumulator collect) {
    this.parents = parents;
    //childWithValues = ConjunctionDISI.intersectIterators(Arrays.asList(children, values));
    this.children = children;
    this.childValues = values;
    this.collector = collect;
  }

  
  final private BitSet parents;
  final private Accumulator collector;
  private int docID = -1;
  private boolean seen=false;
  final private DocIdSetIterator childValues;
  final private DocIdSetIterator children;

  @Override
  public int docID() {
    return docID;
  }

  @Override
  public int nextDoc() throws IOException {
    assert docID != NO_MORE_DOCS;
    
    assert children.docID()!=docID || docID==-1;
    while (children.docID()<docID || docID==-1 || !advanceExactChildDV(children.docID())) {
      children.nextDoc();
      if (children.docID() == NO_MORE_DOCS || advanceExactChildDV(children.docID())) {
        break;
      }
    }
    if (children.docID() == NO_MORE_DOCS) {
      docID = NO_MORE_DOCS;
      return docID;
    }
    

    assert parents.get(children.docID()) == false;
    
    int nextParentDocID = parents.nextSetBit(children.docID());
    collector.reset();
    seen=true;
    
    while (true) {
      int childDocID = children.nextDoc();
      assert childDocID != nextParentDocID;
      if (childDocID > nextParentDocID) {
        break;
      }
      if (advanceExactChildDV(childDocID)) {
        collector.increment();
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
    if (children.docID() <= prevParentDocID) {
      children.advance(prevParentDocID+1);
    }
    return nextDoc();
  }

  //@Override
  public boolean advanceExact(int targetParentDocID) throws IOException {
    if (targetParentDocID < docID) {
      throw new IllegalArgumentException("target must be after the current document: current=" + docID + " target=" + targetParentDocID);
    }
    int previousDocId = docID;
    docID = targetParentDocID;
    if (targetParentDocID == previousDocId) {
      return seen;
    }
    seen = false;

    if (parents.get(targetParentDocID) == false) {
      return false;
    }
    int prevParentDocId = docID == 0 ? -1 : parents.prevSetBit(docID - 1);
    int childDoc = children.docID();
    if (childDoc <= prevParentDocId) {
      childDoc = children.advance(prevParentDocId + 1);
    }
    
    for(;children.docID() < docID && !seen; children.nextDoc()) {
      if (advanceExactChildDV(children.docID())) {
        collector.reset();
        seen=true;
      } 
    }
    
    if (seen == false) {
      return false;
    }

    for (int doc = children.docID(); doc < docID; doc = children.nextDoc()) {
      if (advanceExactChildDV(doc)) {
        collector.increment();
      }
    }
    return true;
  }

  private boolean advanceExactChildDV(int docnum) throws IOException {
    if (childValues instanceof SortedDocValues) {
      return ((SortedDocValues) childValues).advanceExact(docnum);
    } else {
      if (childValues instanceof NumericDocValues) {
        return ((NumericDocValues) childValues).advanceExact(docnum);
      } else {
        throw new UnsupportedOperationException("unexpected " + childValues 
            + " for advanceExact()");
      }
    }
  }

  @Override
  public long cost() {
    return 0;
  }

  static NumericDocValues wrap(NumericDocValues values, Type selection, BitSet parents2,
      DocIdSetIterator children) {
    return new ToParentDocValues.NumDV(values,selection, parents2, children);
  }

  static SortedDocValues wrap(SortedDocValues values, Type selection, BitSet parents2,
      DocIdSetIterator children) {
    return new ToParentDocValues.SortedDVs(values, selection, parents2, children);
  }

}

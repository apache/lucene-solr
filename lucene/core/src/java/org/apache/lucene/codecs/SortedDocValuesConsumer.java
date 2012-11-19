package org.apache.lucene.codecs;

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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.PriorityQueue;

//nocommit - this needs an abort() method? to free opened files?
public abstract class SortedDocValuesConsumer {

  /** This is called, in value sort order, once per unique
   *  value. */
  public abstract void addValue(BytesRef value) throws IOException;

  /** This is called once per document after all values are
   *  added. */
  public abstract void addDoc(int ord) throws IOException;
  
  public abstract void finish() throws IOException;

  public static class Merger {

    public int fixedLength = -2;

    public int maxLength;

    public int numMergedTerms;

    private final List<BytesRef> mergedTerms = new ArrayList<BytesRef>();
    private final List<SegmentState> segStates = new ArrayList<SegmentState>();

    private static class SegmentState {
      AtomicReader reader;
      FixedBitSet liveTerms;
      int ord = -1;
      SortedDocValues values;
      BytesRef scratch = new BytesRef();

      // nocommit can we factor out the compressed fields
      // compression?  ie we have a good idea "roughly" what
      // the ord should be (linear projection) so we only
      // need to encode the delta from that ...:        
      int[] segOrdToMergedOrd;

      public BytesRef nextTerm() {
        while (ord < values.getValueCount()-1) {
          ord++;
          if (liveTerms == null || liveTerms.get(ord)) {
            values.lookupOrd(ord, scratch);
            return scratch;
          } else {
            // Skip "deleted" terms (ie, terms that were not
            // referenced by any live docs):
            values.lookupOrd(ord, scratch);
          }
        }

        return null;
      }
    }

    private static class TermMergeQueue extends PriorityQueue<SegmentState> {
      public TermMergeQueue(int maxSize) {
        super(maxSize);
      }

      @Override
      protected boolean lessThan(SegmentState a, SegmentState b) {
        return a.scratch.compareTo(b.scratch) <= 0;
      }
    }

    public void merge(MergeState mergeState) throws IOException {

      // First pass: mark "live" terms
      for (AtomicReader reader : mergeState.readers) {
        // nocommit what if this is null...?  need default source?
        int maxDoc = reader.maxDoc();

        SegmentState state = new SegmentState();
        state.reader = reader;
        state.values = reader.getSortedDocValues(mergeState.fieldInfo.name);
        if (state.values == null) {
          state.values = new SortedDocValues.EMPTY(maxDoc);
        }

        segStates.add(state);
        assert state.values.getValueCount() < Integer.MAX_VALUE;
        if (reader.hasDeletions()) {
          state.liveTerms = new FixedBitSet(state.values.getValueCount());
          Bits liveDocs = reader.getLiveDocs();
          for(int docID=0;docID<maxDoc;docID++) {
            if (liveDocs.get(docID)) {
              state.liveTerms.set(state.values.getOrd(docID));
            }
          }
        }

        // nocommit we can unload the bits to disk to reduce
        // transient ram spike...
      }

      // Second pass: merge only the live terms

      TermMergeQueue q = new TermMergeQueue(segStates.size());
      for(SegmentState segState : segStates) {
        if (segState.nextTerm() != null) {

          // nocommit we could defer this to 3rd pass (and
          // reduce transient RAM spike) but then
          // we'd spend more effort computing the mapping...:
          segState.segOrdToMergedOrd = new int[segState.values.getValueCount()];
          q.add(segState);
        }
      }

      BytesRef lastTerm = null;
      boolean first = true;
      int ord = 0;
      while (q.size() != 0) {
        SegmentState top = q.top();
        if (lastTerm == null || !lastTerm.equals(top.scratch)) {
          lastTerm = BytesRef.deepCopyOf(top.scratch);
          // nocommit we could spill this to disk instead of
          // RAM, and replay on finish...
          mergedTerms.add(lastTerm);
          if (lastTerm == null) {
            fixedLength = lastTerm.length;
          } else {
            ord++;
            if (lastTerm.length != fixedLength) {
              fixedLength = -1;
            }
          }
          maxLength = Math.max(maxLength, lastTerm.length);
        }

        top.segOrdToMergedOrd[top.ord] = ord-1;
        if (top.nextTerm() == null) {
          q.pop();
        } else {
          q.updateTop();
        }
      }

      numMergedTerms = ord;
    }

    public void finish(SortedDocValuesConsumer consumer) throws IOException {

      // Third pass: write merged result
      for(BytesRef term : mergedTerms) {
        consumer.addValue(term);
      }

      for(SegmentState segState : segStates) {
        Bits liveDocs = segState.reader.getLiveDocs();
        int maxDoc = segState.reader.maxDoc();
        for(int docID=0;docID<maxDoc;docID++) {
          if (liveDocs == null || liveDocs.get(docID)) {
            int segOrd = segState.values.getOrd(docID);
            int mergedOrd = segState.segOrdToMergedOrd[segOrd];
            consumer.addDoc(mergedOrd);
          }
        }
      }
    }
  }

  // nocommit why return int...?
  public void merge(MergeState mergeState, Merger merger) throws IOException {
    merger.finish(this);
    this.finish();
  }
}

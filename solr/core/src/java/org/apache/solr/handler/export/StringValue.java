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

package org.apache.solr.handler.export;

import java.io.IOException;

import com.carrotsearch.hppc.IntArrayDeque;
import com.carrotsearch.hppc.IntDeque;
import com.carrotsearch.hppc.IntIntHashMap;
import com.carrotsearch.hppc.IntIntMap;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.MultiDocValues;
import org.apache.lucene.index.OrdinalMap;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LongValues;

class StringValue implements SortValue {

  protected SortedDocValues globalDocValues;

  protected OrdinalMap ordinalMap;
  protected LongValues toGlobal = LongValues.IDENTITY; // this segment to global ordinal. NN;
  protected SortedDocValues docValues;

  protected String field;
  protected int currentOrd;
  protected IntComp comp;
  protected int lastDocID;
  private boolean present;

  private BytesRef lastBytes;
  private String lastString;
  private int lastOrd = -1;

  // simple LRU cache for ord values
  private static final int CACHE_SIZE = 100;
  private IntIntMap ordCache = new IntIntHashMap(CACHE_SIZE * 2);
  private IntDeque ordDeque = new IntArrayDeque(CACHE_SIZE);
  protected int cacheHit, cacheMiss, cacheEvict, cacheClear;

  public StringValue(SortedDocValues globalDocValues, String field, IntComp comp)  {
    this.globalDocValues = globalDocValues;
    this.docValues = globalDocValues;
    if (globalDocValues instanceof MultiDocValues.MultiSortedDocValues) {
      this.ordinalMap = ((MultiDocValues.MultiSortedDocValues) globalDocValues).mapping;
    }
    this.field = field;
    this.comp = comp;
    this.currentOrd = comp.resetValue();
    this.present = false;
  }

  public String getLastString() {
    return this.lastString;
  }

  public void setLastString(String lastString) {
    this.lastString = lastString;
  }

  public StringValue copy() {
    return new StringValue(globalDocValues, field, comp);
  }

  public void setCurrentValue(int docId) throws IOException {
    if (docId < lastDocID) {
      throw new AssertionError("docs were sent out-of-order: lastDocID=" + lastDocID + " vs doc=" + docId);
    }

    lastDocID = docId;

    if (docId > docValues.docID()) {
      docValues.advance(docId);
    }
    if (docId == docValues.docID()) {
      present = true;
      int ord = ordCache.getOrDefault(docValues.ordValue(), -1);
      if (ord == -1) { // cache miss
        cacheMiss++;
        // trim the cache if needed
        if (ordDeque.size() == CACHE_SIZE) {
          int remove = ordDeque.removeFirst();
          ordCache.remove(remove);
          cacheEvict++;
        }
        ord = (int) toGlobal.get(docValues.ordValue());
        ordDeque.addLast(ord);
        ordCache.put(docValues.ordValue(), ord);
      } else {
        cacheHit++;
      }
      currentOrd = ord;
    } else {
      present = false;
      currentOrd = -1;
    }
  }

  @Override
  public boolean isPresent() {
    return present;
  }

  public void setCurrentValue(SortValue sv) {
    StringValue v = (StringValue)sv;
    this.currentOrd = v.currentOrd;
    this.present = v.present;
  }

  public Object getCurrentValue() throws IOException {
    assert present == true;
    if(currentOrd == lastOrd) {
      return lastBytes;
    } else {
      lastBytes = docValues.lookupOrd(currentOrd);
      lastOrd = currentOrd;
      lastString = null;
      return lastBytes;
    }
  }

  public String getField() {
    return field;
  }

  public void setNextReader(LeafReaderContext context) throws IOException {
    if (ordinalMap != null) {
      toGlobal = ordinalMap.getGlobalOrds(context.ord);
      ordCache.clear();
      ordDeque.clear();
      cacheClear++;
    }
    docValues = DocValues.getSorted(context.reader(), field);
    lastDocID = 0;
  }

  public void reset() {
    this.currentOrd = comp.resetValue();
    this.present = false;
    ordCache.clear();
    ordDeque.clear();
  }

  public int compareTo(SortValue o) {
    StringValue sv = (StringValue)o;
    return comp.compare(currentOrd, sv.currentOrd);
  }

  public String toString() {
    return Integer.toString(this.currentOrd);
  }
}

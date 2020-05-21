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
package org.apache.solr.search.facet;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.apache.solr.common.util.Hash;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.util.hll.HLL;

abstract class UniqueSlotAcc extends SlotAcc {
  HLLAgg.HLLFactory factory;
  SchemaField field;
  FixedBitSet[] arr;
  int[] counts;  // populated with the cardinality once
  int nTerms;

  public UniqueSlotAcc(FacetContext fcontext, SchemaField field, int numSlots, HLLAgg.HLLFactory factory) throws IOException {
    super(fcontext);
    this.factory = factory;
    arr = new FixedBitSet[numSlots];
    this.field = field;
  }

  @Override
  public void reset() throws IOException {
    counts = null;
    for (FixedBitSet bits : arr) {
      if (bits == null) continue;
      bits.clear(0, bits.length());
    }
  }

  @Override
  public Object getValue(int slot) throws IOException {
    if (fcontext.isShard()) {
      return getShardValue(slot);
    }
    if (counts != null) {  // will only be pre-populated if this was used for sorting.
      return counts[slot];
    }

    FixedBitSet bs = arr[slot];
    return bs==null ? 0 : bs.cardinality();
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private Object getShardHLL(int slot) throws IOException {
    FixedBitSet ords = arr[slot];
    if (ords == null) return HLLAgg.NO_VALUES;

    HLL hll = factory.getHLL();
    long maxOrd = ords.length();
    Hash.LongPair hashResult = new Hash.LongPair();
    for(int ord=-1; ++ord < maxOrd;) {
      ord = ords.nextSetBit(ord);
      if (ord == DocIdSetIterator.NO_MORE_DOCS) break;
      BytesRef val = lookupOrd(ord);
      // way to avoid recomputing hash across slots?  Prob not worth space
      Hash.murmurhash3_x64_128(val.bytes, val.offset, val.length, 0, hashResult);
      // idea: if the set is small enough, just send the hashes?  We can add at the top
      // level or even just do a hash table at the top level.
      hll.addRaw(hashResult.val1);
    }

    SimpleOrderedMap map = new SimpleOrderedMap();
    map.add("hll", hll.toBytes());
    return map;
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private Object getShardValue(int slot) throws IOException {
    if (factory != null) return getShardHLL(slot);
    FixedBitSet ords = arr[slot];
    int unique;
    if (counts != null) {
      unique = counts[slot];
    } else {
      unique = ords==null ? 0 : ords.cardinality();
    }

    SimpleOrderedMap map = new SimpleOrderedMap();
    map.add("unique", unique);
    map.add("nTerms", nTerms);

    int maxExplicit=100;
    // TODO: make configurable
    // TODO: share values across buckets
    if (unique > 0) {

      List lst = new ArrayList( Math.min(unique, maxExplicit) );

      long maxOrd = ords.length();
      if (ords != null && ords.length() > 0) {
        for (int ord=0; lst.size() < maxExplicit;) {
          ord = ords.nextSetBit(ord);
          if (ord == DocIdSetIterator.NO_MORE_DOCS) break;
          BytesRef val = lookupOrd(ord);
          Object o = field.getType().toObject(field, val);
          lst.add(o);
          if (++ord >= maxOrd) break;
        }
      }

      map.add("vals", lst);
    }

    return map;
  }

  protected abstract BytesRef lookupOrd(int ord) throws IOException;

  // we only calculate all the counts when sorting by count
  public void calcCounts() {
    counts = new int[arr.length];
    for (int i=0; i<arr.length; i++) {
      FixedBitSet bs = arr[i];
      counts[i] = bs == null ? 0 : bs.cardinality();
    }
  }

  @Override
  public int compare(int slotA, int slotB) {
    if (counts == null) {  // TODO: a more efficient way to do this?  prepareSort?
      calcCounts();
    }
    return counts[slotA] - counts[slotB];
  }

  @Override
  public void resize(Resizer resizer) {
    arr = resizer.resize(arr, null);
    if (counts != null) {
      counts = resizer.resize(counts, 0);
    }
  }
}
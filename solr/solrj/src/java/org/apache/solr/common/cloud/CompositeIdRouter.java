package org.apache.solr.common.cloud;

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

import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.Hash;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

//
// user!uniqueid
// user/4!uniqueid
//
public class CompositeIdRouter extends HashBasedRouter {
  public static final String NAME = "compositeId";

  private int separator = '!';

  // separator used to optionally specify number of bits to allocate toward first part.
  private int bitsSepartor = '/';
  private int bits = 16;
  private int mask1 = 0xffff0000;
  private int mask2 = 0x0000ffff;

  protected void setBits(int firstBits) {
    this.bits = firstBits;
    // java can't shift 32 bits
    mask1 = firstBits==0 ? 0 : (-1 << (32-firstBits));
    mask2 = firstBits==32 ? 0 : (-1 >>> firstBits);
  }

  protected int getBits(String firstPart, int commaIdx) {
    int v = 0;
    for (int idx = commaIdx + 1; idx<firstPart.length(); idx++) {
      char ch = firstPart.charAt(idx);
      if (ch < '0' || ch > '9') return -1;
      v = v * 10 + (ch - '0');
    }
    return v > 32 ? -1 : v;
  }

  @Override
  protected int sliceHash(String id, SolrInputDocument doc, SolrParams params) {
    int idx = id.indexOf(separator);
    if (idx < 0) {
      return Hash.murmurhash3_x86_32(id, 0, id.length(), 0);
    }

    int m1 = mask1;
    int m2 = mask2;

    String part1 = id.substring(0,idx);
    int commaIdx = part1.indexOf(bitsSepartor);
    if (commaIdx > 0) {
      int firstBits = getBits(part1, commaIdx);
      if (firstBits >= 0) {
        m1 = firstBits==0 ? 0 : (-1 << (32-firstBits));
        m2 = firstBits==32 ? 0 : (-1 >>> firstBits);
        part1 = part1.substring(0, commaIdx);
      }
    }

    String part2 = id.substring(idx+1);

    int hash1 = Hash.murmurhash3_x86_32(part1, 0, part1.length(), 0);
    int hash2 = Hash.murmurhash3_x86_32(part2, 0, part2.length(), 0);
    return (hash1 & m1) | (hash2 & m2);
  }

  @Override
  public Collection<Slice> getSearchSlices(String shardKey, SolrParams params, DocCollection collection) {
    if (shardKey == null) {
      // search across whole collection
      // TODO: this may need modification in the future when shard splitting could cause an overlap
      return collection.getSlices();
    }
    String id = shardKey;

    int idx = shardKey.indexOf(separator);
    if (idx < 0) {
      // shardKey is a simple id, so don't do a range
      return Collections.singletonList(hashToSlice(Hash.murmurhash3_x86_32(id, 0, id.length(), 0), collection));
    }

    int m1 = mask1;
    int m2 = mask2;

    String part1 = id.substring(0,idx);
    int bitsSepIdx = part1.indexOf(bitsSepartor);
    if (bitsSepIdx > 0) {
      int firstBits = getBits(part1, bitsSepIdx);
      if (firstBits >= 0) {
        m1 = firstBits==0 ? 0 : (-1 << (32-firstBits));
        m2 = firstBits==32 ? 0 : (-1 >>> firstBits);
        part1 = part1.substring(0, bitsSepIdx);
      }
    }

    //  If the upper bits are 0xF0000000, the range we want to cover is
    //  0xF0000000 0xFfffffff

    int hash1 = Hash.murmurhash3_x86_32(part1, 0, part1.length(), 0);
    int upperBits = hash1 & m1;
    int lowerBound = upperBits;
    int upperBound = upperBits | m2;
    Range completeRange = new Range(lowerBound, upperBound);

    List<Slice> slices = new ArrayList(1);
    for (Slice slice : slices) {
      Range range = slice.getRange();
      if (range != null && range.overlaps(completeRange)) {
        slices.add(slice);
      }
    }

    return slices;
  }
}

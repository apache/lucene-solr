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

import org.apache.noggit.JSONWriter;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.Hash;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Class to partition int range into n ranges.
 * @lucene.experimental
 */
public abstract class DocRouter {
  public static final String DEFAULT_NAME = CompositeIdRouter.NAME;
  public static final DocRouter DEFAULT = new CompositeIdRouter();

  public static DocRouter getDocRouter(Object routerSpec) {
    DocRouter router = routerMap.get(routerSpec);
    if (router != null) return router;
    throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Unknown document router '"+ routerSpec + "'");
  }

  // currently just an implementation detail...
  private final static Map<String, DocRouter> routerMap;
  static {
    routerMap = new HashMap<String, DocRouter>();
    PlainIdRouter plain = new PlainIdRouter();
    // instead of doing back compat this way, we could always convert the clusterstate on first read to "plain" if it doesn't have any properties.
    routerMap.put(null, plain);     // back compat with 4.0
    routerMap.put(PlainIdRouter.NAME, plain);
    routerMap.put(CompositeIdRouter.NAME, DEFAULT_NAME.equals(CompositeIdRouter.NAME) ? DEFAULT : new CompositeIdRouter());
    routerMap.put(ImplicitDocRouter.NAME, new ImplicitDocRouter());
    // NOTE: careful that the map keys (the static .NAME members) are filled in by making them final
  }


  // Hash ranges can't currently "wrap" - i.e. max must be greater or equal to min.
  // TODO: ranges may not be all contiguous in the future (either that or we will
  // need an extra class to model a collection of ranges)
  public static class Range implements JSONWriter.Writable {
    public int min;  // inclusive
    public int max;  // inclusive

    public Range(int min, int max) {
      assert min <= max;
      this.min = min;
      this.max = max;
    }

    public boolean includes(int hash) {
      return hash >= min && hash <= max;
    }

    public boolean isSubsetOf(Range superset) {
      return superset.min <= min && superset.max >= max;
    }

    public boolean overlaps(Range other) {
      return includes(other.min) || includes(other.max) || isSubsetOf(other);
    }

    public String toString() {
      return Integer.toHexString(min) + '-' + Integer.toHexString(max);
    }


    @Override
    public int hashCode() {
      // difficult numbers to hash... only the highest bits will tend to differ.
      // ranges will only overlap during a split, so we can just hash the lower range.
      return (min>>28) + (min>>25) + (min>>21) + min;
    }

    @Override
    public boolean equals(Object obj) {
      if (obj.getClass() != getClass()) return false;
      Range other = (Range)obj;
      return this.min == other.min && this.max == other.max;
    }

    @Override
    public void write(JSONWriter writer) {
      writer.write(toString());
    }
  }

  public Range fromString(String range) {
    int middle = range.indexOf('-');
    String minS = range.substring(0, middle);
    String maxS = range.substring(middle+1);
    long min = Long.parseLong(minS, 16);  // use long to prevent the parsing routines from potentially worrying about overflow
    long max = Long.parseLong(maxS, 16);
    return new Range((int)min, (int)max);
  }

  public Range fullRange() {
    return new Range(Integer.MIN_VALUE, Integer.MAX_VALUE);
  }

  public List<Range> partitionRange(int partitions, Range range) {
    return partitionRange(partitions, range.min, range.max);
  }

  /**
   * Returns the range for each partition
   */
  public List<Range> partitionRange(int partitions, int min, int max) {
    assert max >= min;
    if (partitions == 0) return Collections.EMPTY_LIST;
    long range = (long)max - (long)min;
    long srange = Math.max(1, range / partitions);

    List<Range> ranges = new ArrayList<Range>(partitions);

    long start = min;
    long end = start;

    while (end < max) {
      end = start + srange;
      // make last range always end exactly on MAX_VALUE
      if (ranges.size() == partitions - 1) {
        end = max;
      }
      ranges.add(new Range((int)start, (int)end));
      start = end + 1L;
    }

    return ranges;
  }


  public abstract Slice getTargetSlice(String id, SolrInputDocument sdoc, SolrParams params, DocCollection collection);

  /** This method is consulted to determine what slices should be queried for a request when
   *  an explicit shards parameter was not used.
   *  shardKey (normally from shard.keys) and params may be null.
   **/
  public abstract Collection<Slice> getSearchSlices(String shardKey, SolrParams params, DocCollection collection);

}

abstract class HashBasedRouter extends DocRouter {

  @Override
  public Slice getTargetSlice(String id, SolrInputDocument sdoc, SolrParams params, DocCollection collection) {
    if (id == null) id = getId(sdoc, params);
    int hash = sliceHash(id, sdoc, params);
    return hashToSlice(hash, collection);
  }

  protected int sliceHash(String id, SolrInputDocument sdoc, SolrParams params) {
    return Hash.murmurhash3_x86_32(id, 0, id.length(), 0);
  }

  protected String getId(SolrInputDocument sdoc, SolrParams params) {
    Object  idObj = sdoc.getFieldValue("id");  // blech
    String id = idObj != null ? idObj.toString() : "null";  // should only happen on client side
    return id;
  }

  protected Slice hashToSlice(int hash, DocCollection collection) {
    for (Slice slice : collection.getSlices()) {
      DocRouter.Range range = slice.getRange();
      if (range != null && range.includes(hash)) return slice;
    }
    throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "No slice servicing hash code " + Integer.toHexString(hash) + " in " + collection);
  }


  @Override
  public Collection<Slice> getSearchSlices(String shardKey, SolrParams params, DocCollection collection) {
    if (shardKey == null) {
      // search across whole collection
      // TODO: this may need modification in the future when shard splitting could cause an overlap
      return collection.getSlices();
    }

    // use the shardKey as an id for plain hashing
    return Collections.singletonList(getTargetSlice(shardKey, null, params, collection));
  }
}

class PlainIdRouter extends HashBasedRouter {
  public static final String NAME = "plain";
}

//
// user!uniqueid
// user/4!uniqueid
//
class CompositeIdRouter extends HashBasedRouter {
  public static final String NAME = "compositeId";

  private int separator = '!';

  // separator used to optionally specify number of bits to allocate toward first part.
  private int bitsSepartor = '/';
  private int bits = 16;
  private int mask1 = 0xffff0000;
  private int mask2 = 0x0000ffff;

  protected void setBits(int bits) {
    this.bits = bits;
    mask1 = -1 << (32-bits);
    mask2 = -1 >>> bits;
  }

  protected int getBits(String firstPart, int commaIdx) {
    int v = 0;
    for (int idx = commaIdx + 1; idx<firstPart.length(); idx++) {
      char ch = firstPart.charAt(idx);
      if (ch < '0' || ch > '9') return -1;
      v *= 10 + (ch - '0');
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
        m1 = -1 << (32-firstBits);
        m2 = -1 >>> firstBits;
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
    int commaIdx = part1.indexOf(bitsSepartor);
    if (commaIdx > 0) {
      int firstBits = getBits(part1, commaIdx);
      if (firstBits >= 0) {
        m1 = -1 << (32-firstBits);
        m2 = -1 >>> firstBits;
        part1 = part1.substring(0, commaIdx);
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

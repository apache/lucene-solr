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
package org.apache.solr.common.cloud;

import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.Hash;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * CompositeIdRouter partitions ids based on a {@link #SEPARATOR}, hashes each partition and merges the hashes together
 * to map the id to a slice. This allows bucketing of like groups of ids.
 *
 * Allows basic separation split between 32 bits as example given below, or using {@link #bitsSeparator} can specify exact bits
 * <pre>
 * Example inputs:
 * user!uniqueid
 * app!user!uniqueid
 * user/4!uniqueid
 * app/2!user/4!uniqueid
 * </pre>
 * Lets say you had a set of records you want to index together such as a contact in a database, using a prefix of contact!contactid
 * would allow all contact ids to be bucketed together.
 *
 * <pre>
 * An Example:
 * If the id "contact!0000000KISS is passed 😘
 * Take "contact"" and hash it with murmurhash3_x86_32
 * result: -541354036
 * bits: 11011111101110111001011111001100
 *
 * Take 0000000KISS and hash it with murmurhash3_x86_32
 * result: 2099700320
 * bits: 01111101001001101110001001100000
 *
 * Now we take the bits and apply a mask, since this is 32 bits the mask is the first 16 bits or the last 16 bits
 * So uppermask = 0xFFFF0000  11111111111111110000000000000000
 * So we bitwise AND to get half the original hash and only the upper 16 bits for 00T
 * 11011111101110111001011111001100
 * 11111111111111110000000000000000
 * ________________________________
 * 11011111101110110000000000000000
 *
 * lowermask = 0x0000FFFF 00000000000000001111111111111111
 * So we bitwise AND and get the lower 16 bits of the original hash for 0000000KISS
 * 01111101001001101110001001100000
 * 00000000000000001111111111111111
 * ________________________________
 * 00000000000000001110001001100000
 *
 * Now we combine the hashes with a bitwise OR
 * 11011111101110110000000000000000
 * 00000000000000001110001001100000
 * ________________________________
 * 11011111101110111110001001100000
 *
 * 11011111101110111110001001100000 is the hash we return, bucketing the suffixed by prefix type prefix!suffix
 * </pre>
 */
public class CompositeIdRouter extends HashBasedRouter {
  public static final String NAME = "compositeId";

  public static final String SEPARATOR = "!";

  // separator used to optionally specify number of bits to allocate toward first part.
  public static final int bitsSeparator = '/';
  private int bits = 16;

  @Override
  public int sliceHash(String id, SolrInputDocument doc, SolrParams params, DocCollection collection) {
    String shardFieldName = getRouteField(collection);
    if (shardFieldName != null && doc != null) {
      Object o = doc.getFieldValue(shardFieldName);
      if (o == null)
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "No value for :" + shardFieldName + ". Unable to identify shard");
      id = o.toString();
    }
    if (id.indexOf(SEPARATOR) < 0) {
      return Hash.murmurhash3_x86_32(id, 0, id.length(), 0);
    }

    return new KeyParser(id).getHash();
  }


  /**
   * Get Range for a given CompositeId based route key
   *
   * @param routeKey to return Range for
   * @return Range for given routeKey
   */
  public Range keyHashRange(String routeKey) {
    if (routeKey.indexOf(SEPARATOR) < 0) {
      int hash = sliceHash(routeKey, null, null, null);
      return new Range(hash, hash);
    }

    return new KeyParser(routeKey).getRange();
  }

  @Override
  public Range getSearchRangeSingle(String shardKey, SolrParams params, DocCollection collection) {
    if (shardKey == null) {
      // search across whole range
      return fullRange();
    }

    if (shardKey.indexOf(SEPARATOR) < 0) {
      // shardKey is a simple id, so don't do a range
      int hash = Hash.murmurhash3_x86_32(shardKey, 0, shardKey.length(), 0);
      return new Range(hash, hash);
    }

    return new KeyParser(shardKey).getRange();
  }

  @Override
  public Collection<Slice> getSearchSlicesSingle(String shardKey, SolrParams params, DocCollection collection) {
    if (shardKey == null) {
      // search across whole collection
      // TODO: this may need modification in the future when shard splitting could cause an overlap
      return collection.getActiveSlices();
    }
    String id = shardKey;

    if (shardKey.indexOf(SEPARATOR) < 0) {
      // shardKey is a simple id, so don't do a range
      return Collections.singletonList(hashToSlice(Hash.murmurhash3_x86_32(id, 0, id.length(), 0), collection));
    }

    Range completeRange = new KeyParser(id).getRange();

    List<Slice> targetSlices = new ArrayList<>(1);
    for (Slice slice : collection.getActiveSlicesArr()) {
      Range range = slice.getRange();
      if (range != null && range.overlaps(completeRange)) {
        targetSlices.add(slice);
      }
    }

    return targetSlices;
  }

  @Override
  public String getName() {
    return NAME;
  }

  public List<Range> partitionRangeByKey(String key, Range range) {
    List<Range> result = new ArrayList<>(3);
    Range keyRange = keyHashRange(key);
    if (!keyRange.overlaps(range)) {
      throw new IllegalArgumentException("Key range does not overlap given range");
    }
    if (keyRange.equals(range)) {
      return Collections.singletonList(keyRange);
    } else if (keyRange.isSubsetOf(range)) {
      result.add(new Range(range.min, keyRange.min - 1));
      result.add(keyRange);
      result.add((new Range(keyRange.max + 1, range.max)));
    } else if (range.includes(keyRange.max)) {
      result.add(new Range(range.min, keyRange.max));
      result.add(new Range(keyRange.max + 1, range.max));
    } else {
      result.add(new Range(range.min, keyRange.min - 1));
      result.add(new Range(keyRange.min, range.max));
    }
    return result;
  }

  @Override
  public List<Range> partitionRange(int partitions, Range range) {
    int min = range.min;
    int max = range.max;

    assert max >= min;
    if (partitions == 0) return Collections.emptyList();
    long rangeSize = (long) max - (long) min;
    long rangeStep = Math.max(1, rangeSize / partitions);

    List<Range> ranges = new ArrayList<>(partitions);

    long start = min;
    long end = start;

    // keep track of the idealized target to avoid accumulating rounding errors
    long targetStart = min;
    long targetEnd = targetStart;

    // Round to avoid splitting hash domains across ranges if such rounding is not significant.
    // With default bits==16, one would need to create more than 4000 shards before this
    // becomes false by default.
    int mask = 0x0000ffff;
    boolean round = rangeStep >= (1 << bits) * 16;

    while (end < max) {
      targetEnd = targetStart + rangeStep;
      end = targetEnd;

      if (round && ((end & mask) != mask)) {
        // round up or down?
        int increment = 1 << bits;  // 0x00010000
        long roundDown = (end | mask) - increment;
        long roundUp = (end | mask) + increment;
        if (end - roundDown < roundUp - end && roundDown > start) {
          end = roundDown;
        } else {
          end = roundUp;
        }
      }

      // make last range always end exactly on MAX_VALUE
      if (ranges.size() == partitions - 1) {
        end = max;
      }
      ranges.add(new Range((int) start, (int) end));
      start = end + 1L;
      targetStart = targetEnd + 1L;
    }

    return ranges;
  }

  /**
   * Helper class to calculate parts, masks etc for an id.
   */
  static class KeyParser {
    String key;
    int[] numBits;
    int[] hashes;
    int[] masks;
    boolean triLevel;
    int pieces;

    public KeyParser(final String key) {
      this.key = key;
      List<String> partsList = new ArrayList<>(3);
      int firstSeparatorPos = key.indexOf(SEPARATOR);
      if (-1 == firstSeparatorPos) {
        partsList.add(key);
      } else {
        partsList.add(key.substring(0, firstSeparatorPos));
        int lastPos = key.length() - 1;
        // Don't make any more parts if the first separator is the last char
        if (firstSeparatorPos < lastPos) {
          int secondSeparatorPos = key.indexOf(SEPARATOR, firstSeparatorPos + 1);
          if (-1 == secondSeparatorPos) {
            partsList.add(key.substring(firstSeparatorPos + 1));
          } else if (secondSeparatorPos == lastPos) {
            // Don't make any more parts if the key has exactly two separators and 
            // they're the last two chars - back-compatibility with the behavior of
            // String.split() - see SOLR-6257.
            if (firstSeparatorPos < secondSeparatorPos - 1) {
              partsList.add(key.substring(firstSeparatorPos + 1, secondSeparatorPos));
            }
          } else { // The second separator is not the last char
            partsList.add(key.substring(firstSeparatorPos + 1, secondSeparatorPos));
            partsList.add(key.substring(secondSeparatorPos + 1));
          }
          // Ignore any further separators beyond the first two
        }
      }
      pieces = partsList.size();
      String[] parts = partsList.toArray(new String[pieces]);
      numBits = new int[2];
      if (key.endsWith("!") && pieces < 3)
        pieces++;
      hashes = new int[pieces];

      if (pieces == 3) {
        numBits[0] = 8;
        numBits[1] = 8;
        triLevel = true;
      } else {
        numBits[0] = 16;
        triLevel = false;
      }

      for (int i = 0; i < pieces; i++) {
        if (i < pieces - 1) {
          int commaIdx = parts[i].indexOf(bitsSeparator);

          if (commaIdx > 0) {
            numBits[i] = getNumBits(parts[i], commaIdx);
            parts[i] = parts[i].substring(0, commaIdx);
          }
        }
        //Last component of an ID that ends with a '!'
        if(i >= parts.length)
          hashes[i] = Hash.murmurhash3_x86_32("", 0, "".length(), 0);
        else
          hashes[i] = Hash.murmurhash3_x86_32(parts[i], 0, parts[i].length(), 0);
      }
      masks = getMasks();
    }

    Range getRange() {
      int lowerBound;
      int upperBound;

      if (triLevel) {
        lowerBound = hashes[0] & masks[0] | hashes[1] & masks[1];
        upperBound = lowerBound | masks[2];
      } else {
        lowerBound = hashes[0] & masks[0];
        upperBound = lowerBound | masks[1];
      }
      //  If the upper bits are 0xF0000000, the range we want to cover is
      //  0xF0000000 0xFfffffff

      if ((masks[0] == 0 && !triLevel) || (masks[0] == 0 && masks[1] == 0 && triLevel)) {
        // no bits used from first part of key.. the code above will produce 0x000000000->0xffffffff
        // which only works on unsigned space, but we're using signed space.
        lowerBound = Integer.MIN_VALUE;
        upperBound = Integer.MAX_VALUE;
      }
      Range r = new Range(lowerBound, upperBound);
      return r;
    }

    /**
     * Get bit masks for routing based on routing level
     */
    private int[] getMasks() {
      int[] masks;
      if (triLevel)
        masks = getBitMasks(numBits[0], numBits[1]);
      else
        masks = getBitMasks(numBits[0]);

      return masks;
    }

    private int[] getBitMasks(int firstBits, int secondBits) {
      // java can't shift 32 bits
      int[] masks = new int[3];
      masks[0] = firstBits == 0 ? 0 : (-1 << (32 - firstBits));
      masks[1] = (firstBits + secondBits) == 0 ? 0 : (-1 << (32 - firstBits - secondBits));
      masks[1] = masks[0] ^ masks[1];
      masks[2] = (firstBits + secondBits) == 32 ? 0 : ~(masks[0] | masks[1]);
      return masks;
    }

    private int getNumBits(String firstPart, int commaIdx) {
      int v = 0;
      for (int idx = commaIdx + 1; idx < firstPart.length(); idx++) {
        char ch = firstPart.charAt(idx);
        if (ch < '0' || ch > '9') return -1;
        v = v * 10 + (ch - '0');
      }
      return v > 32 ? -1 : v;
    }

    private int[] getBitMasks(int firstBits) {
      // java can't shift 32 bits
      int[] masks;
      masks = new int[2];
      masks[0] = firstBits == 0 ? 0 : (-1 << (32 - firstBits));
      masks[1] = firstBits == 32 ? 0 : (-1 >>> firstBits);
      return masks;
    }

    int getHash() {
      int result = hashes[0] & masks[0];

      for (int i = 1; i < pieces; i++)
        result = result | (hashes[i] & masks[i]);
      return result;
    }

  }
}

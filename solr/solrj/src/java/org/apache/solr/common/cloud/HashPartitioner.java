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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Class to partition int range into n ranges.
 *
 */
public class HashPartitioner {

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

}

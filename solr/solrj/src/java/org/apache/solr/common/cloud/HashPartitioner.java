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

import java.util.ArrayList;
import java.util.List;

/**
 * Class to partition int range into n ranges.
 * 
 */
public class HashPartitioner {

  // Hash ranges can't currently "wrap" - i.e. max must be greater or equal to min.
  // TODO: ranges may not be all contiguous in the future (either that or we will
  // need an extra class to model a collection of ranges)
  public static class Range {
    public int min;  // inclusive
    public int max;  // inclusive
    
    public Range(int min, int max) {
      this.min = min;
      this.max = max;
    }

    public boolean includes(int hash) {
      return hash >= min && hash <= max;
    }

    public String toString() {
      return Integer.toHexString(min) + '-' + Integer.toHexString(max);
    }

    public static Range fromString(String range) {
      return null; // TODO
    }
  }



  public List<Range> partitionRange(int partitions, Range range) {
    return partitionRange(partitions, range.min, range.max);
  }

  /**
   *
   * @param partitions
   * @return Range for each partition
   */
  public List<Range> partitionRange(int partitions, int min, int max) {
    assert max >= min;
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

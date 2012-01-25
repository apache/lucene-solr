package org.apache.solr.common.cloud;

/**
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
  
  public static class Range {
    public long min;
    public long max;
    
    public Range(long min, long max) {
      this.min = min;
      this.max = max;
    }
  }
  
  /**
   * works up to 65537 before requested num of ranges is one short
   * 
   * @param partitions
   * @return
   */
  public List<Range> partitionRange(int partitions) {
    // some hokey code to partition the int space
    long range = Integer.MAX_VALUE + (Math.abs((long) Integer.MIN_VALUE));
    long srange = range / partitions;
    
    List<Range> ranges = new ArrayList<Range>(partitions);
    
    long end = 0;
    long start = Integer.MIN_VALUE;
    
    while (end < Integer.MAX_VALUE) {
      end = start + srange;
      ranges.add(new Range(start, end));
      start = end + 1L;
    }
    
    return ranges;
  }
}

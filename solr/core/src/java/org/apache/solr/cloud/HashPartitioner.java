package org.apache.solr.cloud;

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


public class HashPartitioner {
  
  public static class Range {
    public long min;
    public long max;
    
    public Range(long min, long max) {
      this.min = min;
      this.max = max;
    }
  }
  
  public List<Range> partitionRange(int partitions) {
    // some hokey code to partition the int space
    long range = Integer.MAX_VALUE + (Math.abs((long)Integer.MIN_VALUE));
    long srange = range / partitions;
    
    System.out.println("min:" + Integer.MIN_VALUE);
    System.out.println("max:" + Integer.MAX_VALUE);
    
    System.out.println("range:" + range);
    System.out.println("srange:" + srange);
    
    List<Range> ranges = new ArrayList<Range>(partitions);
    
    long end = 0;
    long start = Integer.MIN_VALUE;

    while (end < Integer.MAX_VALUE) {
      end = start + srange;
      System.out.println("from:" + start + ":" + end);
      start = end + 1L;
      ranges.add(new Range(start, end));
    }
    
    return ranges;
  }
}

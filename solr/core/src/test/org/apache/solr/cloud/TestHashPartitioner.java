package org.apache.solr.cloud;

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

import java.util.List;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.cloud.HashPartitioner;
import org.apache.solr.common.cloud.HashPartitioner.Range;

public class TestHashPartitioner extends SolrTestCaseJ4 {
  
  public void testMapHashes() throws Exception {
    HashPartitioner hp = new HashPartitioner();
    List<Range> ranges;

    // make sure the partitioner uses the "natural" boundaries and doesn't suffer from an off-by-one
    ranges = hp.partitionRange(2, hp.fullRange());
    assertEquals(Integer.MIN_VALUE, ranges.get(0).min);
    assertEquals(0x80000000, ranges.get(0).min);
    assertEquals(0xffffffff, ranges.get(0).max);
    assertEquals(0x00000000, ranges.get(1).min);
    assertEquals(0x7fffffff, ranges.get(1).max);

    ranges = hp.partitionRange(2, 0, 0x7fffffff);
    assertEquals(0x00000000, ranges.get(0).min);
    assertEquals(0x3fffffff, ranges.get(0).max);
    assertEquals(0x40000000, ranges.get(1).min);
    assertEquals(0x7fffffff, ranges.get(1).max);

    for (int i = 1; i <= 30000; i += 13) {
      ranges = hp.partitionRange(i, hp.fullRange());
      assertEquals(i, ranges.size());
      assertTrue("First range does not start before " + Integer.MIN_VALUE
          + " it is:" + ranges.get(0).min,
          ranges.get(0).min <= Integer.MIN_VALUE);
      assertTrue("Last range does not end after " + Integer.MAX_VALUE
          + " it is:" + ranges.get(ranges.size() - 1).max,
          ranges.get(ranges.size() - 1).max >= Integer.MAX_VALUE);

      for (Range range : ranges) {
        String s = range.toString();
        Range newRange = hp.fromString(s);
        assertEquals(range, newRange);
      }


    }
  }
  
}

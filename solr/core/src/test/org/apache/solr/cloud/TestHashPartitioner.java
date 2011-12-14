package org.apache.solr.cloud;

/**
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
    
    for (int i = 1; i <= 30000; i++) {
      List<Range> ranges = hp.partitionRange(i);
      
      assertEquals(i, ranges.size());
      
      assertTrue("First range does not start before " + Integer.MIN_VALUE
          + " it is:" + ranges.get(0).min,
          ranges.get(0).min <= Integer.MIN_VALUE);
      assertTrue("Last range does not end after " + Integer.MAX_VALUE
          + " it is:" + ranges.get(ranges.size() - 1).max,
          ranges.get(ranges.size() - 1).max >= Integer.MAX_VALUE);
    }
  }
  
}

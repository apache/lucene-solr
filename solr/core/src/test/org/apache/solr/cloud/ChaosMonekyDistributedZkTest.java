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

import org.junit.BeforeClass;

/**
 *
 */
public class ChaosMonekyDistributedZkTest extends FullDistributedZkTest {
  
  
  @BeforeClass
  public static void beforeSuperClass() throws Exception {
    
  }
  
  public ChaosMonekyDistributedZkTest() {
    super();
  }
  
  @Override
  public void doTest() throws Exception {
    initCloud();
    
    handle.clear();
    handle.put("QTime", SKIPVAL);
    handle.put("timestamp", SKIPVAL);
    
    del("*:*");
    
    indexr(id, 1, i1, 100, tlong, 100, t1, "now is the time for all good men",
        "foo_f", 1.414f, "foo_b", "true", "foo_d", 1.414d);
    
    commit();
    
    // these queries should be exactly ordered and scores should exactly match
    query("q", "*:*", "sort", i1 + " desc");
  }
  
  @Override
  public void tearDown() throws Exception {
    super.tearDown();
  }

}

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
import org.junit.Ignore;

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
    
    handle.clear();
    handle.put("QTime", SKIPVAL);
    handle.put("timestamp", SKIPVAL);
    
    del("*:*");
    
    chaosMonkey.startTheMonkey();
    
    Thread.sleep(12000);
    
    chaosMonkey.stopTheMonkey();
  }
  
  @Override
  public void tearDown() throws Exception {
    super.tearDown();
  }

}

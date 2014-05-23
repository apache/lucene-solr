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
package org.apache.solr.hadoop;

import java.util.Comparator;

import org.junit.Assert;
import org.junit.Test;

public class AlphaNumericComparatorTest extends Assert {

  @Test
  public void testBasic() {
    Comparator c = new AlphaNumericComparator();
    assertTrue(c.compare("a", "b") < 0);
    assertTrue(c.compare("shard1", "shard1") == 0);
    //assertTrue(c.compare("shard01", "shard1") == 0);
    assertTrue(c.compare("shard10", "shard10") == 0);
    assertTrue(c.compare("shard1", "shard2") < 0);
    assertTrue(c.compare("shard9", "shard10") < 0);
    assertTrue(c.compare("shard09", "shard10") < 0);
    assertTrue(c.compare("shard019", "shard10") > 0);
    assertTrue(c.compare("shard10", "shard11") < 0);
    assertTrue(c.compare("shard10z", "shard10z") == 0);
    assertTrue(c.compare("shard10z", "shard11z") < 0);
    assertTrue(c.compare("shard10a", "shard10z") < 0);
    assertTrue(c.compare("shard10z", "shard10a") > 0);
    assertTrue(c.compare("shard1z", "shard1z") == 0);
    assertTrue(c.compare("shard2", "shard1") > 0);
  }
  
}

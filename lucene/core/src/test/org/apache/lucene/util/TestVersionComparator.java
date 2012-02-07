package org.apache.lucene.util;

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

import java.util.Comparator;

/**
 * Tests for StringHelper.getVersionComparator
 */
public class TestVersionComparator extends LuceneTestCase {
  public void testVersions() {
    Comparator<String> comp = StringHelper.getVersionComparator();
    assertTrue(comp.compare("1", "2") < 0);
    assertTrue(comp.compare("1", "1") == 0);
    assertTrue(comp.compare("2", "1") > 0);
    
    assertTrue(comp.compare("1.1", "1") > 0);
    assertTrue(comp.compare("1", "1.1") < 0);
    assertTrue(comp.compare("1.1", "1.1") == 0);
    
    assertTrue(comp.compare("1.0", "1") == 0);
    assertTrue(comp.compare("1", "1.0") == 0);
    assertTrue(comp.compare("1.0.1", "1.0") > 0);
    assertTrue(comp.compare("1.0", "1.0.1") < 0);
    
    assertTrue(comp.compare("1.02.003", "1.2.3.0") == 0);
    assertTrue(comp.compare("1.2.3.0", "1.02.003") == 0);
    
    assertTrue(comp.compare("1.10", "1.9") > 0);
    assertTrue(comp.compare("1.9", "1.10") < 0);
    
    assertTrue(comp.compare("0", "1.0") < 0);
    assertTrue(comp.compare("00", "1.0") < 0);
    assertTrue(comp.compare("-1.0", "1.0") < 0);
    assertTrue(comp.compare("3.0", Integer.toString(Integer.MIN_VALUE)) > 0);
  }
}

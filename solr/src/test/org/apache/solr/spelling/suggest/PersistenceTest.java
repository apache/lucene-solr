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
package org.apache.solr.spelling.suggest;

import java.io.File;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.spelling.suggest.jaspell.JaspellLookup;
import org.apache.solr.spelling.suggest.tst.TSTLookup;
import org.junit.Test;

public class PersistenceTest extends SolrTestCaseJ4 {
  
  public static final String[] keys = new String[] {
    "one",
    "two",
    "three",
    "four",
    "oneness",
    "onerous",
    "onesimus",
    "twofold",
    "twonk",
    "thrive",
    "through",
    "threat",
    "foundation",
    "fourier",
    "fourty"
  };

  @Test
  public void testTSTPersistence() throws Exception {
    TSTLookup lookup = new TSTLookup();
    for (String k : keys) {
      lookup.add(k, new Float(k.length()));
    }
    File storeDir = new File(TEST_HOME);
    lookup.store(storeDir);
    lookup = new TSTLookup();
    lookup.load(storeDir);
    for (String k : keys) {
      Float val = (Float)lookup.get(k);
      assertNotNull(k, val);
      assertEquals(k, k.length(), val.intValue());
    }
  }
  
  @Test
  public void testJaspellPersistence() throws Exception {
    JaspellLookup lookup = new JaspellLookup();
    for (String k : keys) {
      lookup.add(k, new Float(k.length()));
    }
    File storeDir = new File(TEST_HOME);
    lookup.store(storeDir);
    lookup = new JaspellLookup();
    lookup.load(storeDir);
    for (String k : keys) {
      Float val = (Float)lookup.get(k);
      assertNotNull(k, val);
      assertEquals(k, k.length(), val.intValue());
    }
  }
  
}

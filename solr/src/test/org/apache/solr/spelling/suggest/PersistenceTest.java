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
import org.apache.solr.spelling.suggest.fst.FSTLookup;
import org.apache.solr.spelling.suggest.jaspell.JaspellLookup;
import org.apache.solr.spelling.suggest.tst.TSTLookup;
import org.junit.Test;

public class PersistenceTest extends SolrTestCaseJ4 {
  public final String[] keys = new String[] {
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
      "fourty"};

  @Test
  public void testTSTPersistence() throws Exception {
    runTest(TSTLookup.class, true);
  }
  
  @Test
  public void testJaspellPersistence() throws Exception {
    runTest(JaspellLookup.class, true);
  }

  @Test
  public void testFSTPersistence() throws Exception {
    runTest(FSTLookup.class, false);
  }
  
  private void runTest(Class<? extends Lookup> lookupClass,
      boolean supportsExactWeights) throws Exception {

    // Add all input keys.
    Lookup lookup = lookupClass.newInstance();
    TermFreq[] keys = new TermFreq[this.keys.length];
    for (int i = 0; i < keys.length; i++)
      keys[i] = new TermFreq(this.keys[i], (float) i);
    lookup.build(new TermFreqArrayIterator(keys));

    // Store the suggester.
    File storeDir = new File(TEST_HOME());
    lookup.store(storeDir);

    // Re-read it from disk.
    lookup = lookupClass.newInstance();
    lookup.load(storeDir);

    // Assert validity.
    float previous = Float.NEGATIVE_INFINITY;
    for (TermFreq k : keys) {
      Float val = (Float) lookup.get(k.term);
      assertNotNull(k.term, val);

      if (supportsExactWeights) { 
        assertEquals(k.term, Float.valueOf(k.v), val);
      } else {
        assertTrue(val + ">=" + previous, val >= previous);
        previous = val.floatValue();
      }
    }
  }
}

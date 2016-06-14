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
package org.apache.lucene.search.suggest;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Random;

import org.apache.lucene.search.suggest.Lookup.LookupResult;
import org.apache.lucene.search.suggest.fst.FSTCompletionLookup;
import org.apache.lucene.search.suggest.jaspell.JaspellLookup;
import org.apache.lucene.search.suggest.tst.TSTLookup;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;

public class PersistenceTest extends LuceneTestCase {
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

  public void testTSTPersistence() throws Exception {
    runTest(TSTLookup.class, true);
  }
  
  public void testJaspellPersistence() throws Exception {
    runTest(JaspellLookup.class, true);
  }

  public void testFSTPersistence() throws Exception {
    runTest(FSTCompletionLookup.class, false);
  }

  private Directory getDirectory() {     
    return newDirectory();
  }

  private void runTest(Class<? extends Lookup> lookupClass, boolean supportsExactWeights) throws Exception {

    // Add all input keys.
    Lookup lookup;
    Directory tempDir = getDirectory();
    if (lookupClass == TSTLookup.class) {
      lookup = new TSTLookup(tempDir, "suggest");
    } else if (lookupClass == FSTCompletionLookup.class) {
      lookup = new FSTCompletionLookup(tempDir, "suggest");
    } else {
      lookup = lookupClass.newInstance();
    }
    Input[] keys = new Input[this.keys.length];
    for (int i = 0; i < keys.length; i++)
      keys[i] = new Input(this.keys[i], i);
    lookup.build(new InputArrayIterator(keys));

    // Store the suggester.
    Path storeDir = createTempDir(LuceneTestCase.getTestClass().getSimpleName());
    lookup.store(Files.newOutputStream(storeDir.resolve("lookup.dat")));

    // Re-read it from disk.
    lookup = lookupClass.newInstance();
    lookup.load(Files.newInputStream(storeDir.resolve("lookup.dat")));

    // Assert validity.
    Random random = random();
    long previous = Long.MIN_VALUE;
    for (Input k : keys) {
      List<LookupResult> list = lookup.lookup(TestUtil.bytesToCharSequence(k.term, random), false, 1);
      assertEquals(1, list.size());
      LookupResult lookupResult = list.get(0);
      assertNotNull(k.term.utf8ToString(), lookupResult.key);

      if (supportsExactWeights) { 
        assertEquals(k.term.utf8ToString(), k.v, lookupResult.value);
      } else {
        assertTrue(lookupResult.value + ">=" + previous, lookupResult.value >= previous);
        previous = lookupResult.value;
      }
    }
    tempDir.close();
  }
}

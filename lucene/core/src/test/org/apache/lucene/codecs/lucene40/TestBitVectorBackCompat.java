package org.apache.lucene.codecs.lucene40;

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

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;

/** Tests BitVector against all released 3.x versions */
public class TestBitVectorBackCompat extends LuceneTestCase {
  static final List<Class<? extends AncientBitVector>> clazzes = new ArrayList<>();
  static {
    clazzes.add(BitVector_3_0_0.class);
    clazzes.add(BitVector_3_0_2.class);
    clazzes.add(BitVector_3_1_0.class);
    clazzes.add(BitVector_3_4_0.class);
    clazzes.add(BitVector_3_5_0.class);
  }
  
  static AncientBitVector newBitVector(int size, Class<? extends AncientBitVector> clazz) throws Exception {
    Constructor<? extends AncientBitVector> ctor = clazz.getConstructor(int.class);
    return ctor.newInstance(size);
  }
  
  public void testRandom() throws Exception {
    for (Class<? extends AncientBitVector> clazz : clazzes) {
      for (int i = 0; i < 50; i++) {
        try {
          doTestRandom(clazz);
        } catch (Throwable t) {
          System.out.println("failed with version: " + clazz.getSimpleName());
          IOUtils.reThrow(t);
        }
      }
    }
  }
  
  private void doTestRandom(Class<? extends AncientBitVector> clazz) throws Exception {    
    // set random bits, of some sparsity
    int size = TestUtil.nextInt(random(), 1, 100_000);
    int numSet = random().nextInt(size);
    
    AncientBitVector bv = newBitVector(size, clazz);
    if (numSet == size) {
      for (int i = 0; i < size; i++) {
        bv.set(i);
      }
    } else {
      for (int i = 0; i < numSet; i++) {
        while (true) {
          final int o = random().nextInt(size);
          if (!bv.get(o)) {
            bv.set(o);
            break;
          }
        }
      }
    }
    
    // serialize to ramdir
    RAMDirectory ramdir = new RAMDirectory();
    bv.write(ramdir, "bits");
    
    // read back with current code
    BitVector current = new BitVector(ramdir, "bits", IOContext.DEFAULT);
    
    assertEquals(size, current.size());
    assertEquals(numSet, current.size() - current.count());
    for (int i = 0; i < size; i++) {
      // we must assert they are opposites: because we look at "live docs" but these wrote "deleted docs"
      assertEquals(bv.get(i), !current.get(i));
    }
    
    ramdir.close();
  }
}

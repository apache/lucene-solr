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
package org.apache.lucene.index;


import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;

import org.apache.lucene.index.MergePolicy.MergeSpecification;
import org.junit.Test;

public class TestNoMergePolicy extends BaseMergePolicyTestCase {

  public MergePolicy mergePolicy() {
    return NoMergePolicy.INSTANCE;
  }

  @Test
  public void testNoMergePolicy() throws Exception {
    MergePolicy mp = mergePolicy();
    assertNull(mp.findMerges(null, (SegmentInfos)null, null));
    assertNull(mp.findForcedMerges(null, 0, null, null));
    assertNull(mp.findForcedDeletesMerges(null, null));
  }

  @Test
  public void testFinalSingleton() throws Exception {
    assertTrue(Modifier.isFinal(NoMergePolicy.class.getModifiers()));
    Constructor<?>[] ctors = NoMergePolicy.class.getDeclaredConstructors();
    assertEquals("expected 1 private ctor only: " + Arrays.toString(ctors), 1, ctors.length);
    assertTrue("that 1 should be private: " + ctors[0], Modifier.isPrivate(ctors[0].getModifiers()));
  }

  @Test
  public void testMethodsOverridden() throws Exception {
    // Ensures that all methods of MergePolicy are overridden. That's important
    // to ensure that NoMergePolicy overrides everything, so that no unexpected
    // behavior/error occurs
    for (Method m : NoMergePolicy.class.getMethods()) {
      // getDeclaredMethods() returns just those methods that are declared on
      // NoMergePolicy. getMethods() returns those that are visible in that
      // context, including ones from Object. So just filter out Object. If in
      // the future MergePolicy will extend a different class than Object, this
      // will need to change.
      if (m.getName().equals("clone")) {
        continue;
      }
      if (m.getDeclaringClass() != Object.class && !Modifier.isFinal(m.getModifiers())) {
        assertTrue(m + " is not overridden ! ", m.getDeclaringClass() == NoMergePolicy.class);
      }
    }
  }

  @Override
  protected void assertSegmentInfos(MergePolicy policy, SegmentInfos infos) throws IOException {
    for (SegmentCommitInfo info : infos) {
      assertEquals(IndexWriter.SOURCE_FLUSH, info.info.getAttribute(IndexWriter.SOURCE));
    }
  }

  @Override
  protected void assertMerge(MergePolicy policy, MergeSpecification merge) throws IOException {
    fail(); // should never happen
  }

  @Override
  public void testSimulateAppendOnly() throws IOException {
    // Reduce numbers as this merge policy doesn't work well with lots of data
    doTestSimulateAppendOnly(mergePolicy(), 1_000_000, 10_000);
  }

  @Override
  public void testSimulateUpdates() throws IOException {
    // Reduce numbers as this merge policy doesn't work well with lots of data
    doTestSimulateUpdates(mergePolicy(), 100_000, 1000);
  }
}

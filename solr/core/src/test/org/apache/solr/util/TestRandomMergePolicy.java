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

package org.apache.solr.util;

import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.util.LuceneTestCase;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.InvocationTargetException;

/** 
 * A "test the test" sanity check using reflection to ensure that 
 * {@linke RandomMergePolicy} is working as expected
 */
public class TestRandomMergePolicy extends LuceneTestCase {  

  /**
   * Ensure every MP method is overridden by RMP 
   * (future proof ourselves against new methods being added to MP)
   */
  public void testMethodOverride() {
    Class rmp = RandomMergePolicy.class;
    for (Method meth : rmp.getMethods()) {
      if (// ignore things like hashCode, equals, etc...
          meth.getDeclaringClass().equals(Object.class)
          // can't do anything about it regardless of what class declares it
          || Modifier.isFinal(meth.getModifiers())) {
        continue;
      }
      assertEquals("method not overridden by RandomMergePolicy: " + 
                   meth.toGenericString(), 
                   rmp, meth.getDeclaringClass());
    }
  }

  /**
   * Ensure any "getter" methods return the same value as
   * the wrapped MP
   * (future proof ourselves against new final getter/setter pairs being 
   * added to MP w/o dealing with them in the RMP Constructor)
   */
  public void testGetters() throws IllegalAccessException, InvocationTargetException {
    final int iters = atLeast(20);
    for (int i = 0; i < iters; i++) {
      RandomMergePolicy rmp = new RandomMergePolicy();
      Class mp = MergePolicy.class;
      for (Method meth : mp.getDeclaredMethods()) {
        if (meth.getName().startsWith("get") &&
            (0 == meth.getParameterTypes().length)) {

          assertEquals("MergePolicy getter gave diff results for RandomMergePolicy and the policy it wrapped: " + meth.toGenericString(),
                       meth.invoke(rmp), meth.invoke(rmp.inner));
        }
      }
    }
  }
}

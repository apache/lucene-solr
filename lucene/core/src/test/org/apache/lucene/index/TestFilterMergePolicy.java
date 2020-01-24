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

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;

import org.apache.lucene.util.LuceneTestCase;

public class TestFilterMergePolicy extends LuceneTestCase {

  public void testMethodsOverridden() {
    for (Method m : MergePolicy.class.getDeclaredMethods()) {
      if (Modifier.isFinal(m.getModifiers()) || Modifier.isPrivate(m.getModifiers())) continue;
      try {
        FilterMergePolicy.class.getDeclaredMethod(m.getName(),  m.getParameterTypes());
      } catch (NoSuchMethodException e) {
        fail("FilterMergePolicy needs to override '" + m + "'");
      }
    }
  }
}

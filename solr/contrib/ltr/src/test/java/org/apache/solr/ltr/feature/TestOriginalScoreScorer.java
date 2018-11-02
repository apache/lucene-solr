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
package org.apache.solr.ltr.feature;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;

import org.apache.lucene.search.Scorer;
import org.apache.lucene.util.LuceneTestCase;
import org.junit.Test;

public class TestOriginalScoreScorer extends LuceneTestCase {

  @Test
  public void testOverridesAbstractScorerMethods() {
    final Class<?> ossClass = OriginalScoreFeature.OriginalScoreWeight.OriginalScoreScorer.class;
    for (final Method scorerClassMethod : Scorer.class.getDeclaredMethods()) {
      final int modifiers = scorerClassMethod.getModifiers();
      if (!Modifier.isAbstract(modifiers)) continue;

      try {
        final Method ossClassMethod = ossClass.getDeclaredMethod(
            scorerClassMethod.getName(),
            scorerClassMethod.getParameterTypes());
        assertEquals("getReturnType() difference",
            scorerClassMethod.getReturnType(),
            ossClassMethod.getReturnType());
      } catch (NoSuchMethodException e) {
        fail(ossClass + " needs to override '" + scorerClassMethod + "'");
      }
    }
  }
}

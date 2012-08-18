package org.apache.lucene.util;

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

import org.apache.lucene.search.FieldCache;
import org.apache.lucene.util.FieldCacheSanityChecker; // javadocs
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

/**
 * This rule will fail the test if it has insane field caches.
 * <p>
 * calling assertSaneFieldCaches here isn't as useful as having test
 * classes call it directly from the scope where the index readers
 * are used, because they could be gc'ed just before this tearDown
 * method is called.
 * <p>
 * But it's better then nothing.
 * <p>
 * If you are testing functionality that you know for a fact
 * "violates" FieldCache sanity, then you should either explicitly
 * call purgeFieldCache at the end of your test method, or refactor
 * your Test class so that the inconsistent FieldCache usages are
 * isolated in distinct test methods
 * 
 * @see FieldCacheSanityChecker
 */
public class TestRuleFieldCacheSanity implements TestRule {
  
  @Override
  public Statement apply(final Statement s, final Description d) {
    return new Statement() {
      @Override
      public void evaluate() throws Throwable {
        s.evaluate();

        Throwable problem = null;
        try {
          LuceneTestCase.assertSaneFieldCaches(d.getDisplayName());
        } catch (Throwable t) {
          problem = t;
        }

        FieldCache.DEFAULT.purgeAllCaches();

        if (problem != null) {
          Rethrow.rethrow(problem);
        }
      }
    };
  }  
}

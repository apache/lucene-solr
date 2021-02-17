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
package org.apache.lucene.analysis.hunspell;

import static org.apache.lucene.analysis.hunspell.StemmerTestBase.loadDictionary;

import java.util.Collections;
import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.lucene.util.LuceneTestCase;

public class HunspellTest extends LuceneTestCase {
  public void testCheckCanceled() throws Exception {
    AtomicBoolean canceled = new AtomicBoolean();
    Runnable checkCanceled =
        () -> {
          if (canceled.get()) {
            throw new CancellationException();
          }
        };
    Hunspell hunspell =
        new Hunspell(loadDictionary(false, "simple.aff", "simple.dic"), checkCanceled);

    assertTrue(hunspell.spell("apache"));
    assertEquals(Collections.singletonList("apach"), hunspell.suggest("apac"));

    canceled.set(true);
    assertThrows(CancellationException.class, () -> hunspell.spell("apache"));
    assertThrows(CancellationException.class, () -> hunspell.suggest("apac"));
  }
}

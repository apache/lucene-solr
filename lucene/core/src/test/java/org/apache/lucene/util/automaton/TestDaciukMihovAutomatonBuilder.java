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
package org.apache.lucene.util.automaton;

import java.util.Arrays;
import java.util.Collections;

import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;

public class TestDaciukMihovAutomatonBuilder extends LuceneTestCase {

  public void testLargeTerms() {
    byte[] b10k = new byte[10_000];
    Arrays.fill(b10k, (byte) 'a');
    IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
        () -> DaciukMihovAutomatonBuilder.build(Collections.singleton(new BytesRef(b10k))));
    assertTrue(e.getMessage().startsWith("This builder doesn't allow terms that are larger than 1,000 characters"));

    byte[] b1k = ArrayUtil.copyOfSubArray(b10k, 0, 1000);
    DaciukMihovAutomatonBuilder.build(Collections.singleton(new BytesRef(b1k))); // no exception
  }

}

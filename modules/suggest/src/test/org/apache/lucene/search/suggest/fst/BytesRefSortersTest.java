package org.apache.lucene.search.suggest.fst;

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

import java.util.Iterator;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;
import org.junit.Test;

public class BytesRefSortersTest extends LuceneTestCase {
  @Test
  public void testExternalRefSorter() throws Exception {
    check(new ExternalRefSorter(new Sort()));
  }

  @Test
  public void testInMemorySorter() throws Exception {
    check(new InMemorySorter());
  }

  private void check(BytesRefSorter sorter) throws Exception {
    for (int i = 0; i < 100; i++) {
      byte [] current = new byte [random.nextInt(256)];
      random.nextBytes(current);
      sorter.add(new BytesRef(current));
    }

    // Create two iterators and check that they're aligned with each other.
    Iterator<BytesRef> i1 = sorter.iterator();
    Iterator<BytesRef> i2 = sorter.iterator();
    
    // Verify sorter contract.
    try {
      sorter.add(new BytesRef(new byte [1]));
      fail("expected contract violation.");
    } catch (IllegalStateException e) {
      // Expected.
    }

    while (i1.hasNext() && i2.hasNext()) {
      assertEquals(i1.next(), i2.next());
    }
    assertEquals(i1.hasNext(), i2.hasNext());
  }  
}

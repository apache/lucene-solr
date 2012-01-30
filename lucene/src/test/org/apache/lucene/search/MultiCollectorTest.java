package org.apache.lucene.search;

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

import java.io.IOException;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.util.LuceneTestCase;
import org.junit.Test;

public class MultiCollectorTest extends LuceneTestCase {

  private static class DummyCollector extends Collector {

    boolean acceptsDocsOutOfOrderCalled = false;
    boolean collectCalled = false;
    boolean setNextReaderCalled = false;
    boolean setScorerCalled = false;

    @Override
    public boolean acceptsDocsOutOfOrder() {
      acceptsDocsOutOfOrderCalled = true;
      return true;
    }

    @Override
    public void collect(int doc) throws IOException {
      collectCalled = true;
    }

    @Override
    public void setNextReader(AtomicReaderContext context) throws IOException {
      setNextReaderCalled = true;
    }

    @Override
    public void setScorer(Scorer scorer) throws IOException {
      setScorerCalled = true;
    }

  }

  @Test
  public void testNullCollectors() throws Exception {
    // Tests that the collector rejects all null collectors.
    try {
      MultiCollector.wrap(null, null);
      fail("only null collectors should not be supported");
    } catch (IllegalArgumentException e) {
      // expected
    }

    // Tests that the collector handles some null collectors well. If it
    // doesn't, an NPE would be thrown.
    Collector c = MultiCollector.wrap(new DummyCollector(), null, new DummyCollector());
    assertTrue(c instanceof MultiCollector);
    assertTrue(c.acceptsDocsOutOfOrder());
    c.collect(1);
    c.setNextReader(null);
    c.setScorer(null);
  }

  @Test
  public void testSingleCollector() throws Exception {
    // Tests that if a single Collector is input, it is returned (and not MultiCollector).
    DummyCollector dc = new DummyCollector();
    assertSame(dc, MultiCollector.wrap(dc));
    assertSame(dc, MultiCollector.wrap(dc, null));
  }
  
  @Test
  public void testCollector() throws Exception {
    // Tests that the collector delegates calls to input collectors properly.

    // Tests that the collector handles some null collectors well. If it
    // doesn't, an NPE would be thrown.
    DummyCollector[] dcs = new DummyCollector[] { new DummyCollector(), new DummyCollector() };
    Collector c = MultiCollector.wrap(dcs);
    assertTrue(c.acceptsDocsOutOfOrder());
    c.collect(1);
    c.setNextReader(null);
    c.setScorer(null);

    for (DummyCollector dc : dcs) {
      assertTrue(dc.acceptsDocsOutOfOrderCalled);
      assertTrue(dc.collectCalled);
      assertTrue(dc.setNextReaderCalled);
      assertTrue(dc.setScorerCalled);
    }

  }

}

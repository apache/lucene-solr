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

import org.apache.solr.SolrTestCase;
import org.apache.solr.common.util.NamedList;

public class TestRTimerTree extends SolrTestCase {

  private static class MockTimerImpl implements RTimer.TimerImpl {
    static private long systemTime;
    static public void incrementSystemTime(long ms) {
      systemTime += ms;
    }

    private long start;
    public void start() {
      start = systemTime;
    }
    public double elapsed() {
      return systemTime - start;
    }
  }

  private class MockRTimerTree extends RTimerTree {
    @Override
    protected TimerImpl newTimerImpl() {
      return new MockTimerImpl();
    }
    @Override
    protected RTimerTree newTimer() {
      return new MockRTimerTree();
    }
  }

  public void test() {
    RTimerTree rt = new MockRTimerTree(), subt, st;

    MockTimerImpl.incrementSystemTime(100);
    assertEquals(100, (int) rt.getTime());

    subt = rt.sub("sub1");
    MockTimerImpl.incrementSystemTime(50);
    assertEquals(150, (int) rt.getTime());
    assertEquals(50, (int) subt.getTime());

    st = subt.sub("sub1.1");
    st.resume();
    MockTimerImpl.incrementSystemTime(10);
    assertEquals(10, (int) st.getTime());
    st.pause();
    MockTimerImpl.incrementSystemTime(50);
    assertEquals(10, (int) st.getTime());
    st.resume();
    MockTimerImpl.incrementSystemTime(10);
    st.pause();
    subt.stop();
    rt.stop();

    assertEquals(20, (int) st.getTime());
    assertEquals(120, (int) subt.getTime());
    assertEquals(220, (int) rt.getTime());

    @SuppressWarnings({"rawtypes"})
    NamedList nl = rt.asNamedList();
    assertEquals(220, ((Double) nl.get("time")).intValue());
    @SuppressWarnings({"rawtypes"})
    NamedList sub1nl = (NamedList) nl.get("sub1");
    assertNotNull(sub1nl);
    assertEquals(120, ((Double) sub1nl.get("time")).intValue());
    @SuppressWarnings({"rawtypes"})
    NamedList sub11nl = (NamedList) sub1nl.get("sub1.1");
    assertNotNull(sub11nl);
    assertEquals(20, ((Double) sub11nl.get("time")).intValue());
  }
}

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

package org.apache.solr.common.util;

import org.apache.solr.SolrTestCaseJ4;
import org.junit.Test;

/**
 *
 */
public class TestTimeSource extends SolrTestCaseJ4 {

  @Test
  public void testEpochTime() throws Exception {
    doTestEpochTime(TimeSource.CURRENT_TIME);
    doTestEpochTime(TimeSource.NANO_TIME);
    doTestEpochTime(TimeSource.get("simTime:50"));
  }

  private void doTestEpochTime(TimeSource ts) throws Exception {

    // XXX the method below doesn't work reliably because
    // XXX there could be a long thread context switch between these two calls:
    // long prevTime = ts.getTimeNs();
    // long prevEpochTime = ts.getEpochTimeNs();

    long[] prevTimeAndEpoch = ts.getTimeAndEpochNs();
    long delta = 500000000; // 500 ms
    long maxDiff = 1000;
    for (int i = 0; i < 10; i++) {
      ts.sleep(500);
      long[] curTimeAndEpoch = ts.getTimeAndEpochNs();
      long diffTime = prevTimeAndEpoch[0] + delta - curTimeAndEpoch[0];
      long diffEpoch = prevTimeAndEpoch[1] + delta - curTimeAndEpoch[1];
      assertTrue(ts + " diffTime=" + diffTime + ", diffEpoch=" + diffEpoch, Math.abs(diffTime - diffEpoch) < maxDiff);
      prevTimeAndEpoch = curTimeAndEpoch;
    }
  }
}

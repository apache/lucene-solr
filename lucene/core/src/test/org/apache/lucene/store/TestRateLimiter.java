package org.apache.lucene.store;

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
import java.util.concurrent.TimeUnit;

import org.apache.lucene.store.RateLimiter.SimpleRateLimiter;
import org.apache.lucene.util.LuceneTestCase;

/**
 * Simple testcase for RateLimiter.SimpleRateLimiter
 */
public final class TestRateLimiter extends LuceneTestCase {

  public void testPause() {
    SimpleRateLimiter limiter = new SimpleRateLimiter(10); // 10 MB / Sec
    limiter.pause(2);//init
    long pause = 0;
    for (int i = 0; i < 3; i++) {
      pause += limiter.pause(4 * 1024 * 1024); // fire up 3 * 4 MB 
    }
    final long convert = TimeUnit.MILLISECONDS.convert(pause, TimeUnit.NANOSECONDS);
    assertTrue("we should sleep less than 2 seconds but did: " + convert + " millis", convert < 2000l); 
    assertTrue("we should sleep at least 1 second but did only: " + convert + " millis", convert > 1000l); 
  }
}

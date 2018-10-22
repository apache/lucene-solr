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

package org.apache.solr.common;

import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * This class mimicks the operation of {@link java.util.concurrent.CountDownLatch}, but it also
 * periodically checks the state of the provided {@link SolrCloseable} and terminates the wait
 * if it's closed by throwing an {@link InterruptedException}.
 */
public class SolrCloseableLatch {

  private final SolrCloseable closeable;
  private final CountDownLatch latch;

  public SolrCloseableLatch(int count, SolrCloseable closeable) {
    Objects.requireNonNull(closeable);
    this.closeable = closeable;
    this.latch = new CountDownLatch(count);
  }

  public void await() throws InterruptedException {
    await(Long.MAX_VALUE, TimeUnit.SECONDS);
  }

  public boolean await(long timeout, TimeUnit unit) throws InterruptedException {
    timeout = unit.toMillis(timeout);
    while (timeout > 0) {
      if (latch.await(100, TimeUnit.MILLISECONDS)) {
        return true;
      }
      if (closeable.isClosed()) {
        throw new InterruptedException(closeable + " has been closed.");
      }
      timeout -= 100;
    }
    return false;
  }

  public void countDown() {
    latch.countDown();
  }

  public long getCount() {
    return latch.getCount();
  }
}

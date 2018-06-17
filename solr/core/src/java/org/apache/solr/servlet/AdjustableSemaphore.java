/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF liceHealthCheckHandlerTestnses this file to You under the Apache License, Version 2.0
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
package org.apache.solr.servlet;

import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

final public class AdjustableSemaphore {

  private final ExposeSemaphore semaphore;

  private int maxPermits = 0;

  public AdjustableSemaphore(boolean fair) {
    semaphore = new ExposeSemaphore(fair);
  }

  synchronized void setMaxPermits(int newMax) {
    int diff = newMax - this.maxPermits;

    if (diff == 0) {
      return;
    } else if (diff > 0) {
      this.semaphore.release(diff);
    } else {
      diff *= -1;
      this.semaphore.reducePermits(diff);
    }

    this.maxPermits = newMax;
  }

  void release() {
    this.semaphore.release();
  }

  void acquire() throws InterruptedException {
    this.semaphore.acquire();
  }

  int availablePermits() {
    return this.semaphore.availablePermits();
  }

  private static final class ExposeSemaphore extends Semaphore {

    private static final long serialVersionUID = 1L;

    ExposeSemaphore(boolean fair) {
      super(0, fair);
    }

    @Override
    protected void reducePermits(int reduction) {
      super.reducePermits(reduction);
    }
  }

  public boolean tryAcquire(long waitMs, TimeUnit milliseconds) throws InterruptedException {
    return this.semaphore.tryAcquire(waitMs, milliseconds);
  }
}
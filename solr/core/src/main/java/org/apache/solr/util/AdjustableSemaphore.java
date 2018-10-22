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

import java.util.concurrent.Semaphore;

final public class AdjustableSemaphore {

  private final ResizeableSemaphore semaphore;
  
  private int maxPermits = 0;

  public AdjustableSemaphore(int size) {
    semaphore = new ResizeableSemaphore(size);
  }
  
  public synchronized void setMaxPermits(int newMax) {
    if (newMax < 1) {
      throw new IllegalArgumentException("Semaphore size must be at least 1,"
          + " was " + newMax);
    }
    
    int delta = newMax - this.maxPermits;
    
    if (delta == 0) {
      return;
    } else if (delta > 0) {
      this.semaphore.release(delta);
    } else {
      delta *= -1;
      this.semaphore.reducePermits(delta);
    }
    
    this.maxPermits = newMax;
  }
  
  public void release() {
    this.semaphore.release();
  }
  
  public void release(int numPermits) {
    this.semaphore.release(numPermits);
  }
  
  public void acquire() throws InterruptedException {
    this.semaphore.acquire();
  }
  
  public synchronized int getMaxPermits() {
    return maxPermits;
  }
  
  private static final class ResizeableSemaphore extends Semaphore {

    ResizeableSemaphore(int size) {
      super(size);
    }
    
    @Override
    protected void reducePermits(int reduction) {
      super.reducePermits(reduction);
    }
  }
}
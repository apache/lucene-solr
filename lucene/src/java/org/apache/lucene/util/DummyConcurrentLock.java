package org.apache.lucene.util;

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

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A dummy lock as a replacement for {@link ReentrantLock} to disable locking
 * @lucene.internal
 */
public final class DummyConcurrentLock implements Lock {

  /** a default instance, can be always used, as this {@link Lock} is stateless. */
  public static final DummyConcurrentLock INSTANCE = new DummyConcurrentLock();

  public void lock() {}
  
  public void lockInterruptibly() {}
  
  public boolean tryLock() {
    return true;
  }
  
  public boolean tryLock(long time, TimeUnit unit) {
    return true;
  }
  
  public void unlock() {}
  
  public Condition newCondition() {
    throw new UnsupportedOperationException();
  }

}

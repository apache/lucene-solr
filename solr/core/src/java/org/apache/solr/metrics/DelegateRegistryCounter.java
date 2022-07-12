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

package org.apache.solr.metrics;

import com.codahale.metrics.Counter;

public class DelegateRegistryCounter extends Counter {

  private final Counter primaryCounter;
  private final Counter delegateCounter;

  public DelegateRegistryCounter(Counter primaryCounter, Counter delegateCounter) {
    this.primaryCounter = primaryCounter;
    this.delegateCounter = delegateCounter;
  }

  @Override
  public void inc() {
    primaryCounter.inc();
    delegateCounter.inc();
  }

  @Override
  public void inc(long n) {
    primaryCounter.inc(n);
    delegateCounter.inc(n);
  }

  @Override
  public void dec() {
    primaryCounter.dec();
    delegateCounter.dec();
  }

  @Override
  public void dec(long n) {
    primaryCounter.dec(n);
    delegateCounter.dec(n);
  }

  @Override
  public long getCount() {
    return primaryCounter.getCount();
  }

  public Counter getPrimaryCounter() {
    return primaryCounter;
  }

  public Counter getDelegateCounter() {
    return delegateCounter;
  }
}

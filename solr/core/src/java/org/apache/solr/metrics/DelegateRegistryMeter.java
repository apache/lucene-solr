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

import com.codahale.metrics.Meter;

public class DelegateRegistryMeter extends Meter {

  private final Meter primaryMeter;
  private final Meter delegateMeter;

  public DelegateRegistryMeter(Meter primaryMeter, Meter delegateMeter) {
    this.primaryMeter = primaryMeter;
    this.delegateMeter = delegateMeter;
  }

  @Override
  public void mark() {
    primaryMeter.mark();
    delegateMeter.mark();
  }

  @Override
  public void mark(long n) {
    primaryMeter.mark(n);
    delegateMeter.mark(n);
  }

  @Override
  public long getCount() {
    return primaryMeter.getCount();
  }

  @Override
  public double getFifteenMinuteRate() {
    return primaryMeter.getFifteenMinuteRate();
  }

  @Override
  public double getFiveMinuteRate() {
    return primaryMeter.getFiveMinuteRate();
  }

  @Override
  public double getMeanRate() {
    return primaryMeter.getMeanRate();
  }

  @Override
  public double getOneMinuteRate() {
    return primaryMeter.getOneMinuteRate();
  }

  public Meter getPrimaryMeter() {
    return primaryMeter;
  }

  public Meter getDelegateMeter() {
    return delegateMeter;
  }
}

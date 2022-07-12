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

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Snapshot;

public class DelegateRegistryHistogram extends Histogram {

  private final Histogram primaryHistogram;
  private final Histogram delegateHistogram;

  public DelegateRegistryHistogram(Histogram primaryHistogram, Histogram delegateHistogram) {
    super(null);
    this.primaryHistogram = primaryHistogram;
    this.delegateHistogram = delegateHistogram;
  }

  @Override
  public void update(int value) {
    primaryHistogram.update(value);
    delegateHistogram.update(value);
  }

  @Override
  public void update(long value) {
    primaryHistogram.update(value);
    delegateHistogram.update(value);
  }

  @Override
  public long getCount() {
    return primaryHistogram.getCount();
  }

  @Override
  public Snapshot getSnapshot() {
    return primaryHistogram.getSnapshot();
  }

  public Histogram getPrimaryHistogram() {
    return primaryHistogram;
  }

  public Histogram getDelegateHistogram() {
    return delegateHistogram;
  }
}

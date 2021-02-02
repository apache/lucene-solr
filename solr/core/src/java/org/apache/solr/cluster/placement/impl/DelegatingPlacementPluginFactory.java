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
package org.apache.solr.cluster.placement.impl;

import com.google.common.annotations.VisibleForTesting;
import org.apache.solr.cluster.placement.PlacementPlugin;
import org.apache.solr.cluster.placement.PlacementPluginConfig;
import org.apache.solr.cluster.placement.PlacementPluginFactory;

import java.util.concurrent.Phaser;

/**
 * Helper class to support dynamic reloading of plugin implementations.
 */
public final class DelegatingPlacementPluginFactory implements PlacementPluginFactory<PlacementPluginFactory.NoConfig> {
  private volatile PlacementPluginFactory<? extends PlacementPluginConfig> delegate;
  // support for tests to make sure the update is completed
  private volatile Phaser phaser;

  @Override
  public PlacementPlugin createPluginInstance() {
    if (delegate != null) {
      return delegate.createPluginInstance();
    } else {
      return null;
    }
  }

  /**
   * A phaser that will advance phases every time {@link #setDelegate(PlacementPluginFactory)} is called
   */
  @VisibleForTesting
  public void setDelegationPhaser(Phaser phaser) {
    phaser.register();
    this.phaser = phaser;
  }

  public void setDelegate(PlacementPluginFactory<? extends PlacementPluginConfig> delegate) {
    this.delegate = delegate;
    Phaser localPhaser = phaser; // volatile read
    if (localPhaser != null) {
      localPhaser.arrive(); // we should be the only ones registered, so this will advance phase each time
    }
  }

  @VisibleForTesting
  public PlacementPluginFactory<? extends PlacementPluginConfig> getDelegate() {
    return delegate;
  }
}

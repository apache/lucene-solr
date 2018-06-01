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

package org.apache.solr.core;

public class SynchronizedDisruptionConfig {

  private final PluginInfo[] synchronizedDisruptions;


  private SynchronizedDisruptionConfig(PluginInfo[] synchronizedDisruptions) {
    this.synchronizedDisruptions = synchronizedDisruptions;
  }

  public PluginInfo[] getSynchronizedDisruptions() { return synchronizedDisruptions; }

  public static class SynchronizedDisruptionConfigBuilder {
    private PluginInfo[] synchronizedDisruptions = new PluginInfo[0];

    public SynchronizedDisruptionConfigBuilder() {}

    public SynchronizedDisruptionConfigBuilder setSynchronizedDisruptions(PluginInfo[] synchronizedDisruptions) {
      this.synchronizedDisruptions = synchronizedDisruptions != null ? synchronizedDisruptions : new PluginInfo[0];
      return this;
    }

    public SynchronizedDisruptionConfig build() {
      return new SynchronizedDisruptionConfig(synchronizedDisruptions);
    }
  }

}

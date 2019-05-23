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

import io.opentracing.Tracer;
import io.opentracing.noop.NoopTracerFactory;
import org.apache.solr.util.plugin.NamedListInitializedPlugin;
import org.apache.solr.util.tracing.GlobalTracer;

public abstract class TracerConfigurator implements NamedListInitializedPlugin {

  public abstract Tracer getTracer();

  public static void loadTracer(SolrResourceLoader loader, PluginInfo info) {
    if (info == null) {
      GlobalTracer.setup(NoopTracerFactory.create(), 0.0);
    } else {
      TracerConfigurator configurator = loader
          .newInstance(info.className, TracerConfigurator.class);
      configurator.init(info.initArgs);

      double sampleRate = 1.0;
      Object sampleRateObj = info.initArgs.get("sampleRate");
      if (sampleRateObj != null) {
        sampleRate = Double.parseDouble(sampleRateObj.toString());
      }
      GlobalTracer.setup(configurator.getTracer(), sampleRate);
    }
  }
}

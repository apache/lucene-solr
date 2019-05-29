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

import java.lang.invoke.MethodHandles;

import io.opentracing.Tracer;
import io.opentracing.noop.NoopTracerFactory;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.util.plugin.NamedListInitializedPlugin;
import org.apache.solr.util.tracing.GlobalTracer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class TracerConfigurator implements NamedListInitializedPlugin {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public abstract Tracer getTracer();

  public static void loadTracer(SolrResourceLoader loader, PluginInfo info, ZkStateReader stateReader) {
    if (info == null) {
      // in case of a Tracer is registered to OpenTracing through javaagent
      if (io.opentracing.util.GlobalTracer.isRegistered()) {
        GlobalTracer.setup(io.opentracing.util.GlobalTracer.get());
        registerListener(stateReader);
      } else {
        GlobalTracer.setup(NoopTracerFactory.create());
        GlobalTracer.get().setSamplePercentage(0.0);
      }
    } else {
      TracerConfigurator configurator = loader
          .newInstance(info.className, TracerConfigurator.class);
      configurator.init(info.initArgs);

      GlobalTracer.setup(configurator.getTracer());
      registerListener(stateReader);
    }
  }

  private static void registerListener(ZkStateReader stateReader) {
    stateReader.registerClusterPropertiesListener(properties -> {
      if (properties.containsKey(ZkStateReader.SAMPLE_PERCENTAGE)) {
        try {
          double sampleRate = Double.parseDouble(properties.get(ZkStateReader.SAMPLE_PERCENTAGE).toString());
          GlobalTracer.get().setSamplePercentage(sampleRate);
        } catch (NumberFormatException e) {
          log.error("Unable to set sample rate", e);
        }
      } else {
        GlobalTracer.get().setSamplePercentage(0.1);
      }
      return false;
    });
  }
}

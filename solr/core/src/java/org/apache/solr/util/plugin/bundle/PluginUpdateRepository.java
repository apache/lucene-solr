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

package org.apache.solr.util.plugin.bundle;

import java.lang.invoke.MethodHandles;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ro.fortsoft.pf4j.update.DefaultUpdateRepository;
import ro.fortsoft.pf4j.update.PluginInfo;

/**
 * An update repo that defers URL location resolving to runtime
 */
public class PluginUpdateRepository extends DefaultUpdateRepository {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private String resolvedUrl;
  private boolean locationResolved = false;
  private Map<String, PluginInfo> plugins;

  public PluginUpdateRepository(String id) {
    this(id, "");
  }

  public PluginUpdateRepository(String id, String location) {
    super(id, location);
  }

  public PluginUpdateRepository(String id, String location, String pluginsJson) {
    super(id, location, pluginsJson);
  }

  @Override
  public String getUrl() {
    if (resolvedUrl == null && !locationResolved) {
      resolvedUrl = resolveUrl();
      locationResolved = true;
    }
    return resolvedUrl;
  }

  /**
   * Resolves location of repo at load time. Override this to dynamically resolve real repo location
   * @return URL of the repository location
   */
  protected String resolveUrl() {
    return super.getUrl();
  }
}

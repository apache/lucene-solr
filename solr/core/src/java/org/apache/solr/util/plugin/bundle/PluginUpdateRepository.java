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
import java.net.URL;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ro.fortsoft.pf4j.update.DefaultUpdateRepository;

/**
 * An update repo that defers URL location resolving to runtime
 */
public class PluginUpdateRepository extends DefaultUpdateRepository {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private URL resolvedUrl;
  private boolean locationResolved = false;

  public PluginUpdateRepository(String id) {
    this(id, null);
  }

  public PluginUpdateRepository(String id, URL url) {
    super(id, url);
  }

  public PluginUpdateRepository(String id, URL url, String pluginsJson) {
    super(id, url, pluginsJson);
  }

  @Override
  public URL getUrl() {
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
  protected URL resolveUrl() {
    return super.getUrl();
  }
}

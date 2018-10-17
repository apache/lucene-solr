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
package org.apache.solr.handler.component;

import java.util.Collections;
import java.util.Locale;

import com.google.common.collect.ImmutableMap;
import org.apache.solr.common.SolrException;
import org.apache.solr.core.PluginInfo;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.security.HttpClientBuilderPlugin;
import org.apache.solr.util.plugin.PluginInfoInitialized;

public abstract class ShardHandlerFactory {

  public abstract ShardHandler getShardHandler();

  public abstract void close();

  public void setSecurityBuilder(HttpClientBuilderPlugin clientBuilderPlugin){};

  /**
   * Create a new ShardHandlerFactory instance
   * @param info    a PluginInfo object defining which type to create.  If null,
   *                the default {@link HttpShardHandlerFactory} will be used
   * @param loader  a SolrResourceLoader used to find the ShardHandlerFactory classes
   * @return a new, initialized ShardHandlerFactory instance
   */
  public static ShardHandlerFactory newInstance(PluginInfo info, SolrResourceLoader loader) {
    if (info == null)
      info = DEFAULT_SHARDHANDLER_INFO;

    try {
      ShardHandlerFactory shf = loader.findClass(info.className, ShardHandlerFactory.class).newInstance();
      if (PluginInfoInitialized.class.isAssignableFrom(shf.getClass()))
        PluginInfoInitialized.class.cast(shf).init(info);
      return shf;
    }
    catch (Exception e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
          String.format(Locale.ROOT, "Error instantiating shardHandlerFactory class [%s]: %s",
                        info.className, e.getMessage()), e);
    }

  }

  public static final PluginInfo DEFAULT_SHARDHANDLER_INFO =
      new PluginInfo("shardHandlerFactory", ImmutableMap.of("class", HttpShardHandlerFactory.class.getName()),
          null, Collections.<PluginInfo>emptyList());
}

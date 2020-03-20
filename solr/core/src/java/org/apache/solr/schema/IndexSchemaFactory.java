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
package org.apache.solr.schema;

import java.io.InputStream;
import java.lang.invoke.MethodHandles;

import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.core.PluginInfo;
import org.apache.solr.core.SolrConfig;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.util.SystemIdResolver;
import org.apache.solr.util.plugin.NamedListInitializedPlugin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.InputSource;

/** Base class for factories for IndexSchema implementations */
public abstract class IndexSchemaFactory implements NamedListInitializedPlugin {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  /** Instantiates the configured schema factory, then calls create on it. */
  public static IndexSchema buildIndexSchema(String resourceName, SolrConfig config) {
    return newIndexSchemaFactory(config).create(resourceName, config);
  }

  /** Instantiates us from {@link SolrConfig}. */
  public static IndexSchemaFactory newIndexSchemaFactory(SolrConfig config) {
    PluginInfo info = config.getPluginInfo(IndexSchemaFactory.class.getName());
    IndexSchemaFactory factory;
    if (null != info) {
      factory = config.getResourceLoader().newInstance(info.className, IndexSchemaFactory.class);
      factory.init(info.initArgs);
    } else {
      factory = config.getResourceLoader().newInstance(ManagedIndexSchemaFactory.class.getName(), IndexSchemaFactory.class);
    }
    return factory;
  }

  /**
   * Returns the resource (file) name that will be used for the schema itself.  The answer may be a guess.
   * Do not pass the result of this to {@link #create(String, SolrConfig)}.
   * The input is the name coming from the {@link org.apache.solr.core.CoreDescriptor}
   * which acts as a default or asked-for name.
   */
  public String getSchemaResourceName(String cdResourceName) {
    return cdResourceName;
  }

  /**
   * Returns an index schema created from a local resource.  The input is usually from the core descriptor.
   */
  public IndexSchema create(String resourceName, SolrConfig config) {
    SolrResourceLoader loader = config.getResourceLoader();
    InputStream schemaInputStream = null;

    if (null == resourceName) {
      resourceName = IndexSchema.DEFAULT_SCHEMA_FILE;
    }

    try {
      schemaInputStream = loader.openSchema(resourceName);
    } catch (Exception e) {
      final String msg = "Error loading schema resource " + resourceName;
      log.error(msg, e);
      throw new SolrException(ErrorCode.SERVER_ERROR, msg, e);
    }
    InputSource inputSource = new InputSource(schemaInputStream);
    inputSource.setSystemId(SystemIdResolver.createSystemIdFromResourceName(resourceName));
    IndexSchema schema = new IndexSchema(resourceName, inputSource, config.luceneMatchVersion, loader);
    return schema;
  }

}

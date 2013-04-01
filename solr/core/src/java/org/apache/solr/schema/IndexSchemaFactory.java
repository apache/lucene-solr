package org.apache.solr.schema;
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

import org.apache.solr.core.PluginInfo;
import org.apache.solr.core.SolrConfig;
import org.apache.solr.util.plugin.NamedListInitializedPlugin;

public abstract class IndexSchemaFactory implements NamedListInitializedPlugin {
  
  public abstract IndexSchema create(String resourceName, SolrConfig config);

  public static IndexSchema buildIndexSchema(String resourceName, SolrConfig config) {
    PluginInfo info = config.getPluginInfo(IndexSchemaFactory.class.getName());
    IndexSchemaFactory factory;
    if (null != info) {
      factory = config.getResourceLoader().newInstance(info.className, IndexSchemaFactory.class);
      factory.init(info.initArgs);
    } else {
      factory = new ClassicIndexSchemaFactory();
    }
    IndexSchema schema = factory.create(resourceName, config);
    return schema;
  }
}

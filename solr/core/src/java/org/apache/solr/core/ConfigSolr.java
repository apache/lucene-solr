package org.apache.solr.core;

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

import org.apache.solr.cloud.ZkController;
import org.apache.solr.handler.component.ShardHandlerFactory;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * ConfigSolr is a new interface  to aid us in obsoleting solr.xml and replacing it with solr.properties. The problem here
 * is that the Config class is used for _all_ the xml file, e.g. solrconfig.xml and we can't mess with _that_ as part
 * of this issue. Primarily used in CoreContainer at present.
 * <p/>
 * This is already deprecated, it's only intended to exist for while transitioning to properties-based replacement for
 * solr.xml
 *
 * @since solr 4.3
 */
public interface ConfigSolr {

  public static enum ConfLevel {
    SOLR, SOLR_CORES, SOLR_CORES_CORE, SOLR_LOGGING, SOLR_LOGGING_WATCHER
  };

  public final static String CORE_PROP_FILE = "core.properties";
  public final static String SOLR_XML_FILE = "solr.xml";

  public int getInt(ConfLevel level, String tag, int def);

  public boolean getBool(ConfLevel level, String tag, boolean defValue);

  public String get(ConfLevel level, String tag, String def);

  public void substituteProperties();

  public ShardHandlerFactory initShardHandler();

  public Properties getSolrProperties(ConfigSolr cfg, String context);

  public SolrConfig getSolrConfigFromZk(ZkController zkController, String zkConfigName, String solrConfigFileName,
                                        SolrResourceLoader resourceLoader);

  public void initPersist();

  public void addPersistCore(String coreName, Properties attribs, Map<String, String> props);

  public void addPersistAllCores(Properties containerProperties, Map<String, String> rootSolrAttribs, Map<String, String> coresAttribs,
                                 File file);

  public String getCoreNameFromOrig(String origCoreName, SolrResourceLoader loader, String coreName);

  public List<String> getAllCoreNames();

  public String getProperty(String coreName, String property, String defaultVal);

  public Properties readCoreProperties(String coreName);

  public Map<String, String> readCoreAttributes(String coreName);

  // If the core is not to be loaded (say two cores defined with the same name or with the same data dir), return
  // the reason. If it's OK to load the core, return null.
  public String getBadCoreMessage(String name);
}

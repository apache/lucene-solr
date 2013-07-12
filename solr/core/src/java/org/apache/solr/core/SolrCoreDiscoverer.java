package org.apache.solr.core;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.io.IOUtils;
import org.apache.solr.common.SolrException;
import org.apache.solr.util.PropertiesUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

public class SolrCoreDiscoverer {
  protected static Logger log = LoggerFactory.getLogger(SolrCoreDiscoverer.class);
  
  public final static String CORE_PROP_FILE = "core.properties";
  
  public Map<String, CoreDescriptor> discover(CoreContainer container, File root) throws IOException {
    Map<String, CoreDescriptor> coreDescriptorMap = new HashMap<String, CoreDescriptor>();

    walkFromHere(root, container, coreDescriptorMap);
    
    return coreDescriptorMap;
  }
  
  // Basic recursive tree walking, looking for "core.properties" files. Once one is found, we'll stop going any
  // deeper in the tree.
  //
  private void walkFromHere(File file, CoreContainer container, Map<String,CoreDescriptor> coreDescriptorMap)
      throws IOException {
    log.info("Looking for cores in " + file.getCanonicalPath());
    if (! file.exists()) return;

    for (File childFile : file.listFiles()) {
      // This is a little tricky, we are asking if core.properties exists in a child directory of the directory passed
      // in. In other words we're looking for core.properties in the grandchild directories of the parameter passed
      // in. That allows us to gracefully stop recursing deep but continue looking wide.
      File propFile = new File(childFile, CORE_PROP_FILE);
      if (propFile.exists()) { // Stop looking after processing this file!
        addCore(container, childFile, propFile, coreDescriptorMap);
        continue; // Go on to the sibling directory, don't descend any deeper.
      }
      if (childFile.isDirectory()) {
        walkFromHere(childFile, container, coreDescriptorMap);
      }
    }
  }
  
  private void addCore(CoreContainer container, File childFile, File propFile, Map<String,CoreDescriptor> coreDescriptorMap) throws IOException {
    log.info("Discovered properties file {}, adding to cores", propFile.getAbsolutePath());
    Properties propsOrig = new Properties();
    InputStream is = new FileInputStream(propFile);
    try {
      propsOrig.load(new InputStreamReader(is, "UTF-8"));
    } finally {
      IOUtils.closeQuietly(is);
    }
    Properties props = new Properties();
    for (String prop : propsOrig.stringPropertyNames()) {
      props.put(prop, PropertiesUtil.substituteProperty(propsOrig.getProperty(prop), null));
    }

    // Too much of the code depends on this value being here, but it is NOT supported in discovery mode, so
    // ignore it if present in the core.properties file.
    props.setProperty(CoreDescriptor.CORE_INSTDIR, childFile.getCanonicalPath());

    if (props.getProperty(CoreDescriptor.CORE_NAME) == null) {
      // Should default to this directory
      props.setProperty(CoreDescriptor.CORE_NAME, childFile.getName());
    }
    CoreDescriptor desc = new CoreDescriptor(container, props);
    CoreDescriptor check = coreDescriptorMap.get(desc.getName());
    if (check != null) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Core " + desc.getName() +
          " defined more than once, once in " + desc.getInstanceDir() + " and once in " + check.getInstanceDir());
    }
    coreDescriptorMap.put(desc.getName(), desc);
  }
}

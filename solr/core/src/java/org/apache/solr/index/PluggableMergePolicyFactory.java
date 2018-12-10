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

package org.apache.solr.index;

import java.util.Map;

import org.apache.lucene.index.MergePolicy;
import org.apache.solr.cloud.CloudDescriptor;
import org.apache.solr.cloud.ZkSolrResourceLoader;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.util.Utils;
import org.apache.solr.core.PluginInfo;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.util.plugin.SolrCoreAware;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class PluggableMergePolicyFactory extends SimpleMergePolicyFactory implements SolrCoreAware {
  public static Logger log = LoggerFactory.getLogger(PluggableMergePolicyFactory.class);

  public static final String MERGE_POLICY_PROP = "ext.mergePolicyFactory.collections.";
  public static final String DEFAULT_POLICY_PROP = "ext.mergePolicyFactory.default";
  private final MergePolicyFactory defaultMergePolicyFactory;
  private PluginInfo pluginInfo;

  public PluggableMergePolicyFactory(SolrResourceLoader resourceLoader, MergePolicyFactoryArgs args, IndexSchema schema) {
    super(resourceLoader, args, schema);
    defaultMergePolicyFactory = new TieredMergePolicyFactory(resourceLoader, args, schema);
  }

  @Override
  public void inform(SolrCore core) {
    CloudDescriptor cd = core.getCoreDescriptor().getCloudDescriptor();
    if (cd == null) {
      log.info("Solr not in Cloud mode - using default MergePolicy");
      return;
    }
    // we can safely assume here that our loader is ZK enabled
    ZkStateReader zkStateReader = ((ZkSolrResourceLoader)resourceLoader).getZkController().getZkStateReader();
    Map<String, Object> clusterProps = zkStateReader.getClusterProperties();
    log.debug("-- clusterprops: {}", clusterProps);
    String propName = MERGE_POLICY_PROP + cd.getCollectionName();
    Object o = clusterProps.get(propName);
    if (o == null) {
      // try getting the default one
      o = clusterProps.get(DEFAULT_POLICY_PROP);
      if (o != null) {
        log.debug("Using default MergePolicy configured in cluster properties.");
      }
    } else {
      log.debug("Using collection-specific MergePolicy configured in cluster properties.");
    }
    if (o == null) {
      log.info("No configuration in cluster properties - using default MergePolicy.");
      return;
    }
    Map<String, Object> props = null;
    if (o instanceof String) {
      // JSON string
      props = (Map<String, Object>)Utils.fromJSONString(String.valueOf(o));
    } else if (o instanceof Map) {
      props = (Map)o;
    }
    if (!props.containsKey(FieldType.CLASS_NAME)) {
      log.error("MergePolicy plugin info missing class name, using default: " + props);
      return;
    }
    log.info("Using pluggable MergePolicy: {}", props.get(FieldType.CLASS_NAME));
    pluginInfo = new PluginInfo("mergePolicyFactory", props);
  }

  private static final String NO_SUB_PACKAGES[] = new String[0];

  @Override
  protected MergePolicy getMergePolicyInstance() {
    if (pluginInfo != null) {
      String mpfClassName = pluginInfo.className;
      MergePolicyFactoryArgs mpfArgs = pluginInfo.initArgs != null ?
          new MergePolicyFactoryArgs(pluginInfo.initArgs) : new MergePolicyFactoryArgs();
      try {
        MergePolicyFactory policyFactory = resourceLoader.newInstance(
            mpfClassName,
            MergePolicyFactory.class,
            NO_SUB_PACKAGES,
            new Class[] { SolrResourceLoader.class, MergePolicyFactoryArgs.class, IndexSchema.class },
            new Object[] { resourceLoader, mpfArgs, schema });

        return policyFactory.getMergePolicy();
      } catch (Exception e) {
        log.error("Error instantiating pluggable MergePolicy, using default instead", e);
        return defaultMergePolicyFactory.getMergePolicy();
      }
    } else {
      return defaultMergePolicyFactory.getMergePolicy();
    }
  }

}

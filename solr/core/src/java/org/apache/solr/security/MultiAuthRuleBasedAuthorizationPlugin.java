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
package org.apache.solr.security;

import java.security.Principal;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import org.apache.solr.common.SolrException;
import org.apache.solr.common.StringUtils;
import org.apache.solr.common.util.CommandOperation;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrResourceLoader;

import static org.apache.solr.security.MultiAuthPlugin.applyEditCommandToSchemePlugin;

/**
 * Authorization plugin designed to work with the MultiAuthPlugin to support different AuthorizationPlugin per scheme.
 *
 * @lucene.experimental
 */
public class MultiAuthRuleBasedAuthorizationPlugin extends RuleBasedAuthorizationPluginBase {
  private final Map<String, RuleBasedAuthorizationPluginBase> pluginMap = new LinkedHashMap<>();
  private final SolrResourceLoader loader;

  // Need the CC to get the resource loader for loading the sub-plugins
  public MultiAuthRuleBasedAuthorizationPlugin(CoreContainer cc) {
    this.loader = cc.getResourceLoader();
  }

  @Override
  @SuppressWarnings({"unchecked"})
  public void init(Map<String, Object> initInfo) {
    super.init(initInfo);

    Object o = initInfo.get(MultiAuthPlugin.PROPERTY_SCHEMES);
    if (!(o instanceof List)) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Invalid config: " + getClass().getName() + " requires a list of schemes!");
    }

    List<Object> schemeList = (List<Object>) o;
    if (schemeList.size() < 2) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Invalid config: " + getClass().getName() + " requires at least two schemes!");
    }

    for (Object s : schemeList) {
      if (!(s instanceof Map)) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Invalid scheme config, expected JSON object but found: " + s);
      }
      initPluginForScheme((Map<String, Object>) s);
    }
  }

  protected void initPluginForScheme(Map<String, Object> schemeMap) {
    Map<String, Object> schemeConfig = new HashMap<>(schemeMap);

    String scheme = (String) schemeConfig.remove(MultiAuthPlugin.PROPERTY_SCHEME);
    if (StringUtils.isEmpty(scheme)) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "'scheme' is a required attribute: " + schemeMap);
    }

    String clazz = (String) schemeConfig.remove("class");
    if (StringUtils.isEmpty(clazz)) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "'class' is a required attribute: " + schemeMap);
    }

    RuleBasedAuthorizationPluginBase pluginForScheme = loader.newInstance(clazz, RuleBasedAuthorizationPluginBase.class);
    pluginForScheme.init(schemeConfig);
    pluginMap.put(scheme.toLowerCase(Locale.ROOT), pluginForScheme);
  }

  /**
   * Pulls roles from the Principal
   *
   * @param principal the user Principal which should contain roles
   * @return set of roles as strings
   */
  @Override
  public Set<String> getUserRoles(Principal principal) {
    Set<String> mergedRoles = new HashSet<>();
    for (RuleBasedAuthorizationPluginBase plugin : pluginMap.values()) {
      final Set<String> userRoles = plugin.getUserRoles(principal);
      if (userRoles != null) {
        mergedRoles.addAll(userRoles);
      }
    }
    return mergedRoles;
  }

  @Override
  public Map<String, Object> edit(Map<String, Object> latestConf, List<CommandOperation> commands) {
    boolean madeChanges = false;
    for (CommandOperation c : commands) {

      // just let the base class handle permission commands
      if (c.name.endsWith("-permission")) {
        Map<String, Object> updated = super.edit(latestConf, Collections.singletonList(c));
        if (updated != null) {
          madeChanges = true;
          latestConf = updated;
        }
        continue;
      }

      Map<String, Object> dataMap = c.getDataMap();
      // expect the "scheme" wrapper map around the actual command data
      if (dataMap == null || dataMap.size() != 1) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "All edit commands must include a 'scheme' wrapper object!");
      }

      final String scheme = dataMap.keySet().iterator().next().toLowerCase(Locale.ROOT);
      RuleBasedAuthorizationPluginBase plugin = pluginMap.get(scheme);
      if (plugin == null) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "No authorization plugin configured for the '" + scheme +
            "' scheme! Did you forget to wrap the command with a scheme object?");
      }

      CommandOperation cmdForPlugin = new CommandOperation(c.name, dataMap.get(scheme));
      if (applyEditCommandToSchemePlugin(scheme, plugin, cmdForPlugin, latestConf)) {
        madeChanges = true;
      }
      // copy over any errors from the cloned command
      for (String err : cmdForPlugin.getErrors()) {
        c.addError(err);
      }
    }

    return madeChanges ? latestConf : null;
  }
}

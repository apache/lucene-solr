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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.solr.common.cloud.ClusterPropertiesListener;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class LibListener implements ClusterPropertiesListener {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final CoreContainer coreContainer;

  Map<String, RuntimeLib> runtimeLibs = new HashMap<>();
  MemClassLoader memClassLoader;

  LibListener(CoreContainer coreContainer) {
    this.coreContainer = coreContainer;
  }


  @Override
  public boolean onChange(Map<String, Object> properties) {
    Map m = (Map) properties.getOrDefault("runtimeLib", Collections.emptyMap());
    boolean needsReload[] = new boolean[1];
    if (m.size() == runtimeLibs.size()) {
      m.forEach((k, v) -> {
        if(!runtimeLibs.containsKey(k)) needsReload[0] = true;

      });
    } else {
      needsReload[0] = true;
    }
    if(needsReload[0]){
      createNewClassLoader(m);
    }

    return false;
  }
  void createNewClassLoader(Map m) {
    List<RuntimeLib> libs = new ArrayList<>(m.size());
    boolean[] loadedAll = new boolean[1];
    loadedAll[0] = true;
    Map<String , RuntimeLib> libMap = new LinkedHashMap<>();
    m.forEach((k, v) -> {
      if (v instanceof Map) {
        Map map = new HashMap((Map) v);
        map.put(CoreAdminParams.NAME, String.valueOf(k));
        RuntimeLib lib = new RuntimeLib(coreContainer);
        try {
          lib.init(new PluginInfo(null,map));
          if(lib.getUrl() == null){
            log.error("Unable to initialize runtimeLib : "+ Utils.toJSONString(v));
            loadedAll[0] = false;
          }
          lib.loadJar();
          libMap.put(lib.getName(), lib);
        } catch (Exception e) {
          log.error("error loading a runtimeLib "+Utils.toJSONString(v), e );
          loadedAll[0] = false;

        }
      }
    });

   if(loadedAll[0] ){
     this.runtimeLibs = libMap;
     this.memClassLoader = new MemClassLoader( libs, coreContainer.getResourceLoader());
     coreContainer.reloadAllComponents();
   }
  }

}

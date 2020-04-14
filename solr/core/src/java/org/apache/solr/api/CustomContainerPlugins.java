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

package org.apache.solr.api;

import java.io.Closeable;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Constructor;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableSet;
import org.apache.solr.client.solrj.request.beans.PluginMeta;
import org.apache.solr.common.MapWriter;
import org.apache.solr.common.annotation.JsonProperty;
import org.apache.solr.common.cloud.ClusterPropertiesListener;
import org.apache.solr.common.util.Pair;
import org.apache.solr.common.util.ReflectMapWriter;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.common.util.Utils;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.handler.admin.ContainerPluginsApi;
import org.apache.solr.pkg.PackageLoader;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.util.SolrJacksonAnnotationInspector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.lucene.util.IOUtils.closeWhileHandlingException;

public class CustomContainerPlugins implements MapWriter, ClusterPropertiesListener {
  private ObjectMapper mapper = SolrJacksonAnnotationInspector.createObjectMapper();
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  final CoreContainer coreContainer;
  final ApiBag containerApiBag;
  private Map<String, ApiHolder> plugins = new HashMap<>();
  private Map<String, String> pluginNameVsPath = new HashMap<>();

  @Override
  public boolean onChange(Map<String, Object> properties) {
    refresh(null);
    return false;
  }

  @Override
  public void writeMap(EntryWriter ew) {
    plugins.forEach((s, apiHolder) -> ew.putNoEx(s, apiHolder.apiInfo));
  }

  public CustomContainerPlugins(CoreContainer coreContainer, ApiBag apiBag) {
    this.coreContainer = coreContainer;
    this.containerApiBag = apiBag;
  }

  public void refresh(Map<String, Object> pluginInfos) {
    try {
      pluginInfos = ContainerPluginsApi.plugins(coreContainer.zkClientSupplier);
    } catch (IOException e) {
      log.error("Could not read plugins data", e);
      return;
    }
    if(pluginInfos.isEmpty()) return;

    for (Map.Entry<String, Object> e : pluginInfos.entrySet()) {
      PluginMeta info = null;
      try {
        info = mapper.readValue(Utils.toJSON(e.getValue()), PluginMeta.class);
      } catch (IOException ioException) {
        log.error("Invalid apiInfo configuration :", ioException);
      }

      ApiInfo apiInfo = null;
      try {
        List<String> errs = new ArrayList<>();
        apiInfo = new ApiInfo(info, errs);
        if (!errs.isEmpty()) {
          log.error(StrUtils.join(errs, ','));
          continue;
        }
      } catch (Exception ex) {
        log.error("unable to instantiate apiInfo ", ex);
        continue;
      }

      String path = pluginNameVsPath.get(e.getKey());
      if (path == null) {
        // there is a new apiInfo . let's register it
        try {
          apiInfo.init();
          ApiHolder holder = new ApiHolder(apiInfo);
          plugins.put(holder.key, holder);
          pluginNameVsPath.put(apiInfo.info.name, holder.key);
          containerApiBag.register(holder, Collections.EMPTY_MAP);
        } catch (Exception exp) {
          log.error("Cannot install apiInfo ", exp);
        }
      } else {
        ApiHolder old = plugins.get(apiInfo.key);
        if (path.equals(apiInfo.key)) {
          if (Objects.equals(info.version, old.apiInfo.info.version)) {
            //this plugin uses the same version. No need to update
            continue;
          }
          //this apiInfo existed at the same path but uses a newer version of the package
          //refresh the existing Api holder
          try {
            apiInfo.init();
          } catch (Exception exception) {
            log.error("COuld not inititlaize Plugin", exception);
          }
          plugins.get(apiInfo.key).refresh(apiInfo);
        } else {// the path is changed for the same apiInfo. it's not allowed
          log.error("Cannot register the same apiInfo at a different path old path: " + path + "new path : " + apiInfo.key);
        }
      }
    }
    Set<String> toBeRemoved = new HashSet<>();
    for (String s : pluginNameVsPath.keySet()) {
      if (!pluginInfos.containsKey(s)) {
        toBeRemoved.add(s);
      }
    }
    for (String s : toBeRemoved) {
      String pathKey = pluginNameVsPath.remove(s);
      ApiHolder holder = plugins.remove(pathKey);
      if (holder != null) {
        Api old = containerApiBag.unregister(holder.apiInfo.endPoint.method()[0], holder.apiInfo.endPoint.path()[0]);
        if (old instanceof Closeable) {
          closeWhileHandlingException((Closeable) old);
        }

      }
    }
  }

  private class ApiHolder extends Api {
    private final String key;
    private ApiInfo apiInfo;

    protected ApiHolder(ApiInfo apiInfo) throws Exception {
      super(apiInfo.delegate);
      this.apiInfo = apiInfo;
      this.key = apiInfo.key;
    }

    @Override
    public void call(SolrQueryRequest req, SolrQueryResponse rsp) {
      apiInfo.delegate.call(req, rsp);
    }

    void refresh(ApiInfo info) {
      this.apiInfo = info;
      super.spec = info.delegate;
    }
  }

  public class ApiInfo implements ReflectMapWriter  {
    /*This is the path at which this handler is
     *
     */
    @JsonProperty
    public String key;
    @JsonProperty
    private final PluginMeta info;

    @JsonProperty(value = "package")
    public final String pkg;
    private PackageLoader.Package.Version pkgVersion;
    EndPoint endPoint;

    private Class klas;


    private AnnotatedApi delegate;


    public ApiInfo(PluginMeta info, List<String> errs) {
      this.info = info;
      Pair<String, String> klassInfo = org.apache.solr.core.PluginInfo.parseClassName(info.klass);
      pkg = klassInfo.first();
      if (pkg != null) {
        PackageLoader.Package p = coreContainer.getPackageLoader().getPackage(pkg);
        if (p == null) {
          errs.add("Invalid package " + klassInfo.first());
          return;
        }
        this.pkgVersion = p.getLatest(info.version);
        try {
          klas = pkgVersion.getLoader().findClass(klassInfo.second(), Object.class);
        } catch (Exception e) {
          log.error("Error loading class", e);
          errs.add("Error loading class " + e.getMessage());
          return;
        }
      } else {
        try {
          klas = Class.forName(klassInfo.second());
        } catch (ClassNotFoundException e) {
          errs.add("Error loading class " + e.getMessage());
          return;
        }
        pkgVersion = null;
      }
      if (!Modifier.isPublic(klas.getModifiers())) {
        errs.add("Class must be public and static : " + klas.getName());
        return;
      }

      endPoint = (EndPoint) klas.getAnnotation(EndPoint.class);
      if (endPoint == null) {
        errs.add("Invalid class, no @EndPoint annotation");
        return;
      }
      if (endPoint.path().length > 1 || endPoint.method().length > 1) {
        errs.add("Only one HTTP method and url supported for each API");
        return;
      }
      if (endPoint.method().length != 1 || endPoint.path().length != 1) {
        errs.add("The @EndPint must have exactly one method and path attributes");
        return;
      }
      List<String> pathSegments = StrUtils.splitSmart(endPoint.path()[0], '/', true);
      if (pathSegments.size()<2 || !ContainerPluginsApi.PLUGIN.equals(pathSegments.get(0))) {
        errs.add("path must have a /plugin/ prefix ");
        return;
      }
      this.key = endPoint.method()[0].toString() + " " + endPoint.path()[0];
      Constructor constructor = klas.getConstructors()[0];
      if (constructor.getParameterTypes().length > 1 ||
          (constructor.getParameterTypes().length == 1 && constructor.getParameterTypes()[0] != CoreContainer.class)) {
        errs.add("Must have a no-arg constructor or CoreContainer constructor and it must not be a non static inner class");
        return;
      }
      if (!Modifier.isPublic(constructor.getModifiers())) {
        errs.add("Must have a public constructor ");
        return;
      }
    }

    public AnnotatedApi init() throws Exception {
      if (delegate != null) return delegate;
      Constructor constructor = klas.getConstructors()[0];
      if (constructor.getParameterTypes().length == 0) {
        return delegate = new AnnotatedApi(constructor.newInstance(null));
      } else if (constructor.getParameterTypes().length == 1 && constructor.getParameterTypes()[0] == CoreContainer.class) {
        return delegate = new AnnotatedApi(constructor.newInstance(coreContainer));
      } else {
        throw new RuntimeException("Must have a no-arg constructor or CoreContainer constructor ");
      }

    }
  }

  public ApiInfo createInfo(PluginMeta info, List<String> errs) {
    return new ApiInfo(info, errs);

  }

  static final Set<String> supportedPaths = ImmutableSet.of("node", "cluster");
}

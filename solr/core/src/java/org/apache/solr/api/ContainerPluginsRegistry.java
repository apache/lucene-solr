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
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Supplier;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.lucene.util.ResourceLoaderAware;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.request.beans.PluginMeta;
import org.apache.solr.common.MapWriter;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.annotation.JsonProperty;
import org.apache.solr.common.cloud.ClusterPropertiesListener;
import org.apache.solr.common.util.IOUtils;
import org.apache.solr.common.util.PathTrie;
import org.apache.solr.common.util.ReflectMapWriter;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.common.util.Utils;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.PluginInfo;
import org.apache.solr.handler.admin.ContainerPluginsApi;
import org.apache.solr.pkg.PackageLoader;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.util.SolrJacksonAnnotationInspector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.lucene.util.IOUtils.closeWhileHandlingException;
import static org.apache.solr.common.util.Utils.makeMap;

/**
 * This class manages the container-level plugins and their Api-s. It is
 * responsible for adding / removing / replacing the plugins according to the updated
 * configuration obtained from {@link ContainerPluginsApi#plugins(Supplier)}.
 * <p>Plugins instantiated by this class may implement zero or more {@link Api}-s, which
 * are then registered in the CoreContainer {@link ApiBag}. They may be also post-processed
 * for additional functionality by {@link PluginRegistryListener}-s registered with
 * this class.</p>
 */
public class ContainerPluginsRegistry implements ClusterPropertiesListener, MapWriter, Closeable {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final ObjectMapper mapper = SolrJacksonAnnotationInspector.createObjectMapper()
      .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

  private final List<PluginRegistryListener> listeners = new CopyOnWriteArrayList<>();

  private final CoreContainer coreContainer;
  private final ApiBag containerApiBag;

  private final Map<String, ApiInfo> currentPlugins = new HashMap<>();

  @Override
  public boolean onChange(Map<String, Object> properties) {
    refresh();
    return false;
  }

  public void registerListener(PluginRegistryListener listener) {
    listeners.add(listener);
  }
  public void unregisterListener(PluginRegistryListener listener) {
    listeners.remove(listener);
  }

  public ContainerPluginsRegistry(CoreContainer coreContainer, ApiBag apiBag) {
    this.coreContainer = coreContainer;
    this.containerApiBag = apiBag;
  }

  @Override
  public synchronized void writeMap(EntryWriter ew) throws IOException {
    currentPlugins.forEach(ew.getBiConsumer());
  }

  @Override
  public synchronized void close() throws IOException {
    currentPlugins.values().forEach(apiInfo -> {
      if (apiInfo.instance instanceof Closeable) {
        IOUtils.closeQuietly((Closeable) apiInfo.instance);
      }
    });
  }

  public synchronized ApiInfo getPlugin(String name) {
    return currentPlugins.get(name);
  }

  static class PluginMetaHolder {
    private final Map<String, Object> original;
    private final PluginMeta meta;

    PluginMetaHolder(Map<String, Object> original) throws IOException {
      this.original = original;
      meta = mapper.readValue(Utils.toJSON(original), PluginMeta.class);
    }

    @Override
    public boolean equals(Object obj) {
      if (obj instanceof PluginMetaHolder) {
        PluginMetaHolder that = (PluginMetaHolder) obj;
        return Objects.equals(this.original,that.original);
      }
      return false;
    }

    @Override
    public int hashCode() {
      return original.hashCode();
    }
  }
  @SuppressWarnings("unchecked")
  public synchronized void refresh() {
    Map<String, Object> pluginInfos;
    try {
      pluginInfos = ContainerPluginsApi.plugins(coreContainer.zkClientSupplier);
    } catch (IOException e) {
      log.error("Could not read plugins data", e);
      return;
    }
    Map<String,PluginMetaHolder> newState = new HashMap<>(pluginInfos.size());
    for (Map.Entry<String, Object> e : pluginInfos.entrySet()) {
      try {
        newState.put(e.getKey(),new PluginMetaHolder((Map<String, Object>) e.getValue()));
      } catch (Exception exp) {
        log.error("Invalid apiInfo configuration :", exp);
      }
    }

    Map<String, PluginMetaHolder> currentState = new HashMap<>();
    for (Map.Entry<String, ApiInfo> e : currentPlugins.entrySet()) {
      currentState.put(e.getKey(), e.getValue().holder);
    }
    Map<String, Diff> diff = compareMaps(currentState, newState);
    if (diff == null) return;//nothing has changed
    for (Map.Entry<String, Diff> e : diff.entrySet()) {
      if (e.getValue() == Diff.UNCHANGED) continue;
      if (e.getValue() == Diff.REMOVED) {
        ApiInfo apiInfo = currentPlugins.remove(e.getKey());
        if (apiInfo == null) continue;
        listeners.forEach(listener -> listener.deleted(apiInfo));
        for (ApiHolder holder : apiInfo.holders) {
          Api old = containerApiBag.unregister(holder.api.getEndPoint().method()[0],
              getActualPath(apiInfo, holder.api.getEndPoint().path()[0]));
          if (old instanceof Closeable) {
            closeWhileHandlingException((Closeable) old);
          }
        }
      } else {
        //ADDED or UPDATED
        PluginMetaHolder info = newState.get(e.getKey());
        List<String> errs = new ArrayList<>();
        ApiInfo apiInfo = new ApiInfo(info,errs);
        if (!errs.isEmpty()) {
          log.error(StrUtils.join(errs, ','));
          continue;
        }
        try {
          apiInfo.init();
        } catch (Exception exp) {
          log.error("Cannot install apiInfo ", exp);
          continue;
        }
        if (e.getValue() == Diff.ADDED) {
          // this plugin is totally new
          for (ApiHolder holder : apiInfo.holders) {
            containerApiBag.register(holder, getTemplateVars(apiInfo.info));
          }
          currentPlugins.put(e.getKey(), apiInfo);
          final ApiInfo apiInfoFinal = apiInfo;
          listeners.forEach(listener -> listener.added(apiInfoFinal));
        } else {
          //this plugin is being updated
          ApiInfo old = currentPlugins.put(e.getKey(), apiInfo);
          for (ApiHolder holder : apiInfo.holders) {
            //register all new paths
            containerApiBag.register(holder, getTemplateVars(apiInfo.info));
          }
          final ApiInfo apiInfoFinal = apiInfo;
          listeners.forEach(listener -> listener.modified(old, apiInfoFinal));
          if (old != null) {
            //this is an update of the plugin. But, it is possible that
            // some paths are remved in the newer version of the plugin
            for (ApiHolder oldHolder : old.holders) {
              if(apiInfo.get(oldHolder.api.getEndPoint()) == null) {
                //there was a path in the old plugin which is not present in the new one
                containerApiBag.unregister(oldHolder.getMethod(),getActualPath(old, oldHolder.getPath()));
              }
            }
            if (old instanceof Closeable) {
              //close the old instance of the plugin
              closeWhileHandlingException((Closeable) old);
            }
          }
        }
      }

    }
  }

  private static String getActualPath(ApiInfo apiInfo, String path) {
    path = path.replaceAll("\\$path-prefix", apiInfo.info.pathPrefix);
    path = path.replaceAll("\\$plugin-name", apiInfo.info.name);
    return path;
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  private static  Map<String, String> getTemplateVars(PluginMeta pluginMeta) {
    return (Map) makeMap("plugin-name", pluginMeta.name, "path-prefix", pluginMeta.pathPrefix);
  }

  private static class ApiHolder extends Api {
    final AnnotatedApi api;

    protected ApiHolder(AnnotatedApi api) {
      super(api);
      this.api = api;
    }

    @Override
    public void call(SolrQueryRequest req, SolrQueryResponse rsp) {
      api.call(req, rsp);
    }

    public String getPath(){
      return api.getEndPoint().path()[0];
    }

    public SolrRequest.METHOD getMethod(){
      return api.getEndPoint().method()[0];

    }
  }

  @SuppressWarnings({"rawtypes"})
  public class ApiInfo implements ReflectMapWriter {
    List<ApiHolder> holders;

    private final PluginMetaHolder holder;

    @JsonProperty
    private final PluginMeta info;

    @JsonProperty(value = "package")
    public final String pkg;

    private PackageLoader.Package.Version pkgVersion;
    private Class klas;
    Object instance;

    ApiHolder get(EndPoint endPoint) {
      for (ApiHolder holder : holders) {
        EndPoint e = holder.api.getEndPoint();
        if(Objects.equals(endPoint.method()[0] , e.method()[0]) &&
            Objects.equals(endPoint.path()[0], e.path()[0])) {
          return holder;
        }
      }
      return null;
    }

    public Object getInstance() {
      return instance;
    }

    public PluginMeta getInfo() {
      return info.copy();
    }
    @SuppressWarnings({"unchecked","rawtypes"})
    public ApiInfo(PluginMetaHolder infoHolder, List<String> errs) {
      this.holder = infoHolder;
      this.info = infoHolder.meta;
      PluginInfo.ClassName klassInfo = new PluginInfo.ClassName(info.klass);
      pkg = klassInfo.pkg;
      if (pkg != null) {
        Optional<PackageLoader.Package.Version> ver = coreContainer.getPackageLoader().getPackageVersion(pkg, info.version);
        if (ver.isEmpty()) {
          //may be we are a bit early. Do a refresh and try again
         coreContainer.getPackageLoader().getPackageAPI().refreshPackages(null);
         ver = coreContainer.getPackageLoader().getPackageVersion(pkg, info.version);
        }
        if (ver.isEmpty()) {
          PackageLoader.Package p = coreContainer.getPackageLoader().getPackage(pkg);
          if (p == null) {
            errs.add("Invalid package " + klassInfo.pkg);
            return;
          } else {
            errs.add("No such package version:" + pkg + ":" + info.version + " . available versions :" + p.allVersions());
            return;
          }
        }
        this.pkgVersion = ver.get();
        try {
          klas = pkgVersion.getLoader().findClass(klassInfo.className, Object.class);
        } catch (Exception e) {
          log.error("Error loading class", e);
          errs.add("Error loading class " + e.toString());
          return;
        }
      } else {
        try {
          klas = Class.forName(klassInfo.className);
        } catch (ClassNotFoundException e) {
          errs.add("Error loading class " + e.toString());
          return;
        }
        pkgVersion = null;
      }
      if (!Modifier.isPublic(klas.getModifiers())) {
        errs.add("Class must be public and static : " + klas.getName());
        return;
      }

      try {
        List<Api> apis = AnnotatedApi.getApis(klas, null, true);
        for (Object api : apis) {
          EndPoint endPoint = ((AnnotatedApi) api).getEndPoint();
          if (endPoint.path().length > 1 || endPoint.method().length > 1) {
            errs.add("Only one HTTP method and url supported for each API");
          }
          if (endPoint.method().length != 1 || endPoint.path().length != 1) {
            errs.add("The @EndPoint must have exactly one method and path attributes");
          }
          List<String> pathSegments = StrUtils.splitSmart(endPoint.path()[0], '/', true);
          PathTrie.replaceTemplates(pathSegments, getTemplateVars(info));
          if (V2HttpCall.knownPrefixes.contains(pathSegments.get(0))) {
            errs.add("path must not have a prefix: "+pathSegments.get(0));
          }

        }
      } catch (Exception e) {
        errs.add(e.toString());
      }
      if (!errs.isEmpty()) return;

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

    @SuppressWarnings({"rawtypes", "unchecked"})
    public void init() throws Exception {
      if (this.holders != null) return;
      Constructor constructor = klas.getConstructors()[0];
      if (constructor.getParameterTypes().length == 0) {
        instance = constructor.newInstance();
      } else if (constructor.getParameterTypes().length == 1 && constructor.getParameterTypes()[0] == CoreContainer.class) {
        instance = constructor.newInstance(coreContainer);
      } else {
        throw new RuntimeException("Must have a no-arg constructor or CoreContainer constructor ");
      }
      if (instance instanceof ConfigurablePlugin) {
        Class<? extends MapWriter> c = getConfigClass((ConfigurablePlugin<? extends MapWriter>) instance);
        if (c != null && holder.meta.config != null) {
          MapWriter initVal = mapper.readValue(Utils.toJSON(holder.meta.config), c);
          ((ConfigurablePlugin) instance).configure(initVal);
        }
      }
      if (instance instanceof ResourceLoaderAware) {
        try {
          ((ResourceLoaderAware) instance).inform(pkgVersion.getLoader());
        } catch (IOException e) {
          throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
        }
      }
      this.holders = new ArrayList<>();
      for (Api api : AnnotatedApi.getApis(instance.getClass(), instance, true)) {
        holders.add(new ApiHolder((AnnotatedApi) api));
      }
    }

  }

  /**
   * Get the generic type of a {@link ConfigurablePlugin}
   */
  @SuppressWarnings("rawtypes")
  public static Class getConfigClass(ConfigurablePlugin<?> o) {
    Class klas = o.getClass();
    do {
      Type[] interfaces = klas.getGenericInterfaces();
      for (Type type : interfaces) {
        if (type instanceof ParameterizedType) {
          ParameterizedType parameterizedType = (ParameterizedType) type;
          Type rawType = parameterizedType.getRawType();
          if (rawType == ConfigurablePlugin.class ||
              // or if a super interface is a ConfigurablePlugin
              ((rawType instanceof Class) && ConfigurablePlugin.class.isAssignableFrom((Class) rawType))) {
            return (Class) parameterizedType.getActualTypeArguments()[0];
          }
        }
      }
      klas = klas.getSuperclass();
    } while (klas != null && klas != Object.class);
    return null;
  }

  public ApiInfo createInfo(Map<String,Object> info, List<String> errs) throws IOException {
    return new ApiInfo(new PluginMetaHolder(info), errs);

  }
  public ApiInfo createInfo(PluginMetaHolder info, List<String> errs) {
    return new ApiInfo(info, errs);

  }

  public enum Diff {
    ADDED, REMOVED, UNCHANGED, UPDATED
  }

  public static Map<String, Diff> compareMaps(Map<String, ?> a, Map<String, ?> b) {
    if(a.isEmpty() && b.isEmpty()) return null;
    Map<String, Diff> result = new HashMap<>(Math.max(a.size(), b.size()));
    a.forEach((k, v) -> {
      Object newVal = b.get(k);
      if (newVal == null) {
        result.put(k, Diff.REMOVED);
        return;
      }
      result.put(k, Objects.equals(v, newVal) ?
          Diff.UNCHANGED :
          Diff.UPDATED);
    });

    b.forEach((k, v) -> {
      if (a.get(k) == null) result.put(k, Diff.ADDED);
    });

    for (Diff value : result.values()) {
      if (value != Diff.UNCHANGED) return result;
    }

    return null;
  }

  /**
   * Listener for notifications about added / deleted / modified plugins.
   */
  public interface PluginRegistryListener {

    /** Called when a new plugin is added. */
    void added(ApiInfo plugin);

    /** Called when an existing plugin is deleted. */
    void deleted(ApiInfo plugin);

    /** Called when an existing plugin is replaced. */
    void modified(ApiInfo old, ApiInfo replacement);

  }
}

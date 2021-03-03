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

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.RejectedExecutionException;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.apache.lucene.analysis.util.ResourceLoader;
import org.apache.lucene.analysis.util.ResourceLoaderAware;
import org.apache.solr.api.Api;
import org.apache.solr.api.ApiBag;
import org.apache.solr.api.ApiSupport;
import org.apache.solr.common.ParWork;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.common.util.Utils;
import org.apache.solr.handler.RequestHandlerBase;
import org.apache.solr.handler.component.SearchComponent;
import org.apache.solr.pkg.PackagePluginHolder;
import org.apache.solr.request.SolrRequestHandler;
import org.apache.solr.util.plugin.NamedListInitializedPlugin;
import org.apache.solr.util.plugin.PluginInfoInitialized;
import org.apache.solr.util.plugin.SolrCoreAware;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Collections.singletonMap;
import static org.apache.solr.api.ApiBag.HANDLER_NAME;

/**
 * This manages the lifecycle of a set of plugin of the same type .
 */
public class PluginBag<T> implements AutoCloseable {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final Map<String, PluginHolder<T>> registry;

  private String def;
  @SuppressWarnings({"rawtypes"})
  private final Class klass;
  private SolrCore core;
  private final SolrConfig.SolrPluginInfo meta;
  private final ApiBag apiBag;

  /**
   * Pass needThreadSafety=true if plugins can be added and removed concurrently with lookups.
   */
  public PluginBag(Class<T> klass, SolrCore core) {
    this.apiBag = klass == SolrRequestHandler.class ? new ApiBag(core != null) : null;
    this.core = core;
    this.klass = klass;
    // TODO: since reads will dominate writes, we could also think about creating a new instance of a map each time it changes.
    // Not sure how much benefit this would have over ConcurrentHashMap though
    // We could also perhaps make this constructor into a factory method to return different implementations depending on thread safety needs.
    this.registry = new ConcurrentHashMap<>(32, 0.75f, 4);

    meta = SolrConfig.classVsSolrPluginInfo.get(klass.getName());
    if (meta == null) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Unknown Plugin : " + klass.getName());
    }
  }

  public static void initInstance(Object inst, PluginInfo info) {
    if (inst instanceof PluginInfoInitialized) {
      ((PluginInfoInitialized) inst).init(info);
    } else if (inst instanceof NamedListInitializedPlugin) {
      ((NamedListInitializedPlugin) inst).init(info.initArgs);
    } else if (inst instanceof SolrRequestHandler) {
      ((SolrRequestHandler) inst).init(info.initArgs);
    }
    if (inst instanceof SearchComponent) {
      ((SearchComponent) inst).setName(info.name);
    }
    if (inst instanceof RequestHandlerBase) {
      ((RequestHandlerBase) inst).setPluginInfo(info);
    }

  }

  /**
   * Check if any of the mentioned names are missing. If yes, return the Set of missing names
   */
  @SuppressWarnings({"unchecked"})
  public Set<String> checkContains(Collection<String> names) {
    if (names == null || names.isEmpty()) return Collections.EMPTY_SET;
    HashSet<String> result = new HashSet<>();
    for (String s : names) if (!this.registry.containsKey(s)) result.add(s);
    return result;
  }

  @SuppressWarnings({"unchecked"})
  public PluginHolder<T> createPlugin(PluginInfo info) {
    if ("lazy".equals(info.attributes.get("startup")) && meta.options.contains(SolrConfig.PluginOpts.LAZY)) {
      if (log.isDebugEnabled()) {
        log.debug("{} : '{}' created with startup=lazy ", meta.getCleanTag(), info.name);
      }
      return new LazyPluginHolder<T>(meta, info, core, core.getResourceLoader());
    } else {
      if (info.pkgName != null) {
        PackagePluginHolder<T> holder = new PackagePluginHolder<>(info, core, meta);
        return holder;
      } else {
        String packageName =  meta.clazz.getPackage().getName();
        T inst = SolrCore.createInstance(info.className, (Class<T>) meta.clazz, meta.getCleanTag(),
            null, core.getResourceLoader(info.pkgName), Utils.getSolrSubPackage(packageName));
        initInstance(inst, info);
        return new PluginHolder<>(info, inst);
      }
    }
  }

  private String getSolrSubPackage(String substring, String s) {
    return substring + s;
  }

  /**
   * make a plugin available in an alternate name. This is an internal API and not for public use
   *
   * @param src    key in which the plugin is already registered
   * @param target the new key in which the plugin should be aliased to. If target exists already, the alias fails
   * @return flag if the operation is successful or not
   */
  boolean alias(String src, String target) {
    if (src == null) return false;
    PluginHolder<T> a = registry.get(src);
    if (a == null) return false;
    PluginHolder<T> b = registry.get(target);
    if (b != null) return false;
    registry.put(target, a);
    return true;
  }

  /**
   * Get a plugin by name. If the plugin is not already instantiated, it is
   * done here
   */
  public T get(String name) {
    PluginHolder<T> result = registry.get(name);
    return result == null ? null : result.get();
  }

  /**
   * Fetches a plugin by name , or the default
   *
   * @param name       name using which it is registered
   * @param useDefault Return the default , if a plugin by that name does not exist
   */
  public T get(String name, boolean useDefault) {
    if (name == null) {
      return get(def);
    }
    T result = get(name);
    if (useDefault && result == null) return get(def);
    return result;
  }

  public Set<String> keySet() {
    return registry.keySet();
  }

  /**
   * register a plugin by a name
   */
  public T put(String name, T plugin) {
    if (plugin == null) return null;
    PluginHolder<T> pluginHolder = new PluginHolder<>(null, plugin);
    pluginHolder.registerAPI = false;
    PluginHolder<T> old = put(name, pluginHolder);
    return old == null ? null : old.get();
  }

  @SuppressWarnings({"unchecked"})
  public PluginHolder<T> put(String name, PluginHolder<T> plugin) {
    Boolean registerApi = null;
    Boolean disableHandler = null;
    if (plugin.pluginInfo != null) {
      String registerAt = plugin.pluginInfo.attributes.get("registerPath");
      if (registerAt != null) {
        List<String> strs = StrUtils.splitSmart(registerAt, ',');
        disableHandler = !strs.contains("/solr");
        registerApi = strs.contains("/v2");
      }
    }

    if (apiBag != null) {
      if (plugin.isLoaded()) {
        T inst = plugin.get();
        if (inst instanceof ApiSupport) {
          ApiSupport apiSupport = (ApiSupport) inst;
          if (registerApi == null) registerApi = apiSupport.registerV2();
          if (disableHandler == null) disableHandler = !apiSupport.registerV1();

          if(registerApi) {
            Collection<Api> apis = apiSupport.getApis();
            if (apis != null) {
              Map<String, String> nameSubstitutes = singletonMap(HANDLER_NAME, name);
              for (Api api : apis) {
                apiBag.register(api, nameSubstitutes);
              }
            }
          }

        }
      } else {
        if (registerApi != null && registerApi)
          apiBag.registerLazy((PluginHolder<SolrRequestHandler>) plugin, plugin.pluginInfo);
      }
    }
    if (disableHandler == null) disableHandler = Boolean.FALSE;
    PluginHolder<T> old = null;
    if (!disableHandler) old = registry.put(name, plugin);
    if (plugin.pluginInfo != null && plugin.pluginInfo.isDefault()) setDefault(name);
    if (plugin.isLoaded()) {
      if (core != null && core.getCoreContainer() != null) {
        try {
         ParWork.getRootSharedExecutor().submit(() -> registerMBean(plugin.get(), core, name));
        } catch (RejectedExecutionException e) {
          registerMBean(plugin.get(), core, name);
        }
      }
    }
    // old instance has been replaced - close it to prevent mem leaks
    if (old != null && old != plugin) {
      closeQuietly(old);
    }
    return old;
  }

  void setDefault(String def) {
    if (!registry.containsKey(def)) return;
    if (this.def != null) {
      log.warn("Multiple defaults for : {}", meta.getCleanTag());
    }
    this.def = def;
  }

  public Map<String, PluginHolder<T>> getRegistry() {
    return registry;
  }

  public boolean contains(String name) {
    return registry.containsKey(name);
  }

  String getDefault() {
    return def;
  }

  T remove(String name) {
    PluginHolder<T> removed = registry.remove(name);
    return removed == null ? null : removed.get();
  }

  void init(Map<String, T> defaults, SolrCore solrCore) {
    init(defaults, solrCore, solrCore.getSolrConfig().getPluginInfos(klass.getName()));
  }

  /**
   * Initializes the plugins after reading the meta data from {@link org.apache.solr.core.SolrConfig}.
   *
   * @param defaults These will be registered if not explicitly specified
   */
  void init(Map<String, T> defaults, SolrCore solrCore, Collection<PluginInfo> infos) {
    core = solrCore;
    try (ParWork parWork = new ParWork(this, false, false)) {
      for (PluginInfo info : infos) {
        parWork.collect("", new CreateAndPutRequestHandler(info));
      }
    }
    if (infos.size() > 0) { // Aggregate logging
      if (log.isDebugEnabled()) {
        log.debug("[{}] Initialized {} plugins of type {}: {}", solrCore.getName(), infos.size(), meta.getCleanTag(),
            infos.stream().map(i -> i.name).collect(Collectors.toList()));
      }
    }
    for (Map.Entry<String, T> e : defaults.entrySet()) {
      if (!contains(e.getKey())) {
        put(e.getKey(), new PluginHolder<T>(null, e.getValue()));
      }
    }
  }

  /**
   * To check if a plugin by a specified name is already loaded
   */
  public boolean isLoaded(String name) {
    PluginHolder<T> result = registry.get(name);
    if (result == null) return false;
    return result.isLoaded();
  }

  private static void registerMBean(Object inst, SolrCore core, String pluginKey) {
    if (core == null) return;
    if (inst instanceof SolrInfoBean) {
      try {
        SolrInfoBean mBean = (SolrInfoBean) inst;
        String name = (inst instanceof SolrRequestHandler) ? pluginKey : mBean.getName();
        core.registerInfoBean(name, mBean);
      } catch (Exception e) {
        log.error("registerMBean failed", e);
      }
    }
  }


  /**
   * Close this registry. This will in turn call a close on all the contained plugins
   */
  @Override
  public void close() {
    try (ParWork worker = new ParWork(this, false, false)) {
      registry.forEach((s, tPluginHolder) -> {
        worker.collect(tPluginHolder);
      });
    }
  }

  public static void closeQuietly(Object inst)  {
    try {
      if (inst != null && inst instanceof AutoCloseable) ((AutoCloseable) inst).close();
    } catch (Exception e) {
      ParWork.propagateInterrupt(e);
      log.error("Error closing {}", inst , e);
    }
  }

  /**
   * An indirect reference to a plugin. It just wraps a plugin instance.
   * subclasses may choose to lazily load the plugin
   */
  public static class PluginHolder<T> implements Supplier<T>,  AutoCloseable {
    protected T inst;
    protected final PluginInfo pluginInfo;
    boolean registerAPI = false;

    public PluginHolder(PluginInfo info) {
      this.pluginInfo = info;
    }

    public PluginHolder(PluginInfo info, T inst) {
      this.inst = inst;
      this.pluginInfo = info;
    }

    public T get() {
      return inst;
    }

    public boolean isLoaded() {
      return inst != null;
    }

    @Override
    public void close() {
      // TODO: there may be a race here.  One thread can be creating a plugin
      // and another thread can come along and close everything (missing the plugin
      // that is in the state of being created and will probably never have close() called on it).
      // can close() be called concurrently with other methods?
      if (isLoaded()) {
        T myInst = get();
        // N.B. instanceof returns false if myInst is null
        if (myInst instanceof AutoCloseable) {
          try {
            ((AutoCloseable) myInst).close();
          } catch (Exception e) {
            ParWork.propagateInterrupt(e);
            log.error("Error closing {}", inst , e);
          }
        }
      }
    }

    public String getClassName() {
      if (isLoaded()) return inst.getClass().getName();
      if (pluginInfo != null) return pluginInfo.className;
      return null;
    }

    public PluginInfo getPluginInfo() {
      return pluginInfo;
    }
  }

  /**
   * A class that loads plugins Lazily. When the get() method is invoked
   * the Plugin is initialized and returned.
   */
  public static class LazyPluginHolder<T> extends PluginHolder<T> {
    private volatile T lazyInst;
    private final SolrConfig.SolrPluginInfo pluginMeta;
    protected SolrException solrException;
    private final SolrCore core;
    protected ResourceLoader resourceLoader;


    LazyPluginHolder(SolrConfig.SolrPluginInfo pluginMeta, PluginInfo pluginInfo, SolrCore core, ResourceLoader loader) {
      super(pluginInfo);
      this.pluginMeta = pluginMeta;
      this.core = core;
      this.resourceLoader = loader;
    }

    @Override
    public boolean isLoaded() {
      return lazyInst != null;
    }

    @Override
    public T get() {
      if (lazyInst != null) return lazyInst;
      if (solrException != null) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,"Unrecoverable error", solrException);
      }
      if (createInst()) {
        // check if we created the instance to avoid registering it again
        registerMBean(lazyInst, core, pluginInfo.name);
      }
      return lazyInst;
    }

    private synchronized boolean createInst() {
      if (lazyInst != null) return false;
      if (log.isInfoEnabled()) {
        log.info("Going to create a new {} with {} ", pluginMeta.getCleanTag(), pluginInfo);
      }

      @SuppressWarnings({"unchecked"})
      Class<T> clazz = (Class<T>) pluginMeta.clazz;
      T localInst = null;
      try {
        localInst = SolrCore.createInstance(pluginInfo.className, clazz, pluginMeta.getCleanTag(), null, resourceLoader, Utils.getSolrSubPackage(clazz.getPackageName()));
      } catch (SolrException e) {
        throw e;
      }
      initInstance(localInst, pluginInfo);
      if (localInst instanceof SolrCoreAware) {
        SolrResourceLoader.assertAwareCompatibility(SolrCoreAware.class, localInst);
        ((SolrCoreAware) localInst).inform(core);
      }
      if (localInst instanceof ResourceLoaderAware) {
        SolrResourceLoader.assertAwareCompatibility(ResourceLoaderAware.class, localInst);
        try {
          ((ResourceLoaderAware) localInst).inform(core.getResourceLoader());
        } catch (IOException e) {
          throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "error initializing component", e);
        }
      }
      lazyInst = localInst;  // only assign the volatile until after the plugin is completely ready to use
      return true;
    }
  }

  public Api v2lookup(String path, String method, Map<String, String> parts) {
    if (apiBag == null) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "this should not happen, looking up for v2 API at the wrong place");
    }
    return apiBag.lookup(path, method, parts);
  }

  public ApiBag getApiBag() {
    return apiBag;
  }

  private class CreateAndPutRequestHandler implements Runnable {
    private final PluginInfo info;

    public CreateAndPutRequestHandler(PluginInfo info) {
      this.info = info;
    }

    @Override
    public void run() {
      PluginHolder<T> o = createPlugin(info);
      String name = info.name;
      if (meta.clazz.equals(SolrRequestHandler.class)) name = RequestHandlers.normalize(info.name);
      PluginHolder<T> old = put(name, o);
      if (old != null) {
        log.warn("Multiple entries of {} with name {}", meta.getCleanTag(), name);
      }
    }
  }
}

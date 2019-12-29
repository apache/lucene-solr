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

package org.apache.solr.pkg;

import java.lang.invoke.MethodHandles;
import java.lang.ref.Reference;
import java.lang.ref.SoftReference;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

import org.apache.solr.core.PluginInfo;
import org.apache.solr.core.SolrCore;
import org.apache.solr.logging.MDCLoggingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PackageListeners {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static final String PACKAGE_VERSIONS = "PKG_VERSIONS";
  private SolrCore core;

  public PackageListeners(SolrCore core) {
    this.core = core;
  }

  // this registry only keeps a soft reference because it does not want to
  // cause a memory leak if the listener forgets to unregister itself
  private final List<Reference<Listener>> listeners = new CopyOnWriteArrayList<>();

  public synchronized void addListener(Listener listener) {
    listeners.add(new SoftReference<>(listener));

  }

  public synchronized void removeListener(Listener listener) {
    Iterator<Reference<Listener>> it = listeners.iterator();
    while (it.hasNext()) {
      Reference<Listener> ref = it.next();
      Listener pkgListener = ref.get();
      if (pkgListener == null || pkgListener == listener) {
        it.remove();
      }

    }
  }

  synchronized void packagesUpdated(List<PackageLoader.Package> pkgs) {
    if(core != null) MDCLoggingContext.setCore(core);
    try {
      for (PackageLoader.Package pkgInfo : pkgs) {
        invokeListeners(pkgInfo);
      }
    } finally {
      if(core != null) MDCLoggingContext.clear();

    }
  }

  private void invokeListeners(PackageLoader.Package pkg) {
    Ctx ctx = new Ctx();
    try {
      forEachListener(listener -> {
        if (listener.packageName() == null ||
            listener.packageName().equals(pkg.name())) {
          listener.changed(pkg, ctx);
        }
      });
    } finally {
      if(ctx.postProcessors != null){
        for (Runnable value : ctx.postProcessors.values()) {
          value.run();
        }
      }
    }
  }

  public synchronized void forEachListener(Consumer<Listener> listenerConsumer) {
    for (Reference<Listener> ref : listeners) {
      Listener l = ref.get();
      if (l != null) {
        listenerConsumer.accept(l);
      }
    }
  }


  public class Ctx {
    private Map<String, Runnable > postProcessors;

    /**A post processor will be run after all the listeners are invoked. This is particularly helpful
     * if a group of plugins need to be reloaded all at once.  The case in point is schema plugins.
     * If there are multiple plugins loade from packages in a schema, we would like to reload it only once
     *
     */
    public void addPostProcessor(String name, Runnable runnable){
      if(postProcessors == null) postProcessors = new HashMap<>();
      postProcessors.put(name, runnable);
    }

    public Runnable getPostProcessor(String name){
      if(postProcessors == null) return null;
      return postProcessors.get(name);
    }


  }
  public interface Listener {
    /**Name of the package or null to listen to all package changes
     */
    String packageName();

    PluginInfo pluginInfo();

    void changed(PackageLoader.Package pkg, Ctx ctx);

    PackageAPI.PkgVersion  getPackageVersion();

  }
}

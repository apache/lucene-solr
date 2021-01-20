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
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

import org.apache.solr.common.MapWriter;
import org.apache.solr.core.PluginInfo;
import org.apache.solr.core.SolrCore;
import org.apache.solr.logging.MDCLoggingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PackageListeners {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static final String PACKAGE_VERSIONS = "PKG_VERSIONS";
  private final SolrCore core;

  public PackageListeners(SolrCore core) {
    this.core = core;
  }

  // this registry only keeps a weak reference because it does not want to
  // cause a memory leak if the listener forgets to unregister itself
  private List<Reference<Listener>> listeners = new CopyOnWriteArrayList<>();

  public synchronized void addListener(Listener listener) {
    listeners.add(new SoftReference<>(listener));
  }

  public synchronized void addListener(Listener listener, boolean addFirst) {
    if(addFirst) {
      listeners.add(0, new SoftReference<>(listener));
    } else {
      addListener(listener);
    }

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
    MDCLoggingContext.setCore(core);
    Listener.Ctx ctx = new Listener.Ctx();
    try {
      for (PackageLoader.Package pkgInfo : pkgs) {
        invokeListeners(pkgInfo, ctx);
      }
    } finally {
      ctx.runLaterTasks(r -> core.getCoreContainer().runAsync(r));
      MDCLoggingContext.clear();
    }
  }

  private synchronized void invokeListeners(PackageLoader.Package pkg, Listener.Ctx ctx) {
    for (Reference<Listener> ref : listeners) {
      Listener listener = ref.get();
      if(listener == null) continue;
      if (listener.packageName() == null || listener.packageName().equals(pkg.name())) {
        listener.changed(pkg, ctx);
      }
    }
  }

  public List<Listener> getListeners() {
    List<Listener> result = new ArrayList<>();
    for (Reference<Listener> ref : listeners) {
      Listener l = ref.get();
      if (l != null) {
        result.add(l);
      }
    }
    return result;
  }


  public interface Listener {
    /**Name of the package or null to listen to all package changes */
    String packageName();

    /** fetch the package versions of class names
     *
     */
    Map<String, PackageAPI.PkgVersion> packageDetails();

    /**A callback when the package is updated */
    void changed(PackageLoader.Package pkg, Ctx ctx);

    default MapWriter getPackageVersion(PluginInfo.ClassName cName) {
      return null;
    }
    class Ctx {
      private Map<String, Runnable> runLater;

      /**
       * If there are multiple packages to be updated and there are multiple listeners,
       * This is executed after all of the {@link Listener#changed(PackageLoader.Package, Ctx)}
       * calls are invoked. The name is a unique identifier that can be used by consumers to avoid duplicate
       * If no deduplication is required, use null as the name
       */
      public void runLater(String name, Runnable runnable) {
        if (runLater == null) runLater = new LinkedHashMap<>();
        if (name == null) {
          name = runnable.getClass().getSimpleName() + "@" + runnable.hashCode();
        }
        runLater.put(name, runnable);
      }

      private void runLaterTasks(Consumer<Runnable> runnableExecutor) {
        if (runLater == null) return;
        for (Runnable r : runLater.values()) {
          runnableExecutor.accept(r);
        }
      }
    }

  }
}

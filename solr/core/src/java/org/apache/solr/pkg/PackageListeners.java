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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

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

  // this registry only keeps a weak reference because it does not want to
  // cause a memory leak if the listener forgets to unregister itself
  private List<Reference<Listener>> listeners = new ArrayList<>();

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

  private synchronized void invokeListeners(PackageLoader.Package pkg) {
    for (Reference<Listener> ref : listeners) {
      Listener listener = ref.get();
      if(listener == null) continue;
      if (listener.packageName() == null || listener.packageName().equals(pkg.name())) {
        listener.changed(pkg);
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
    /**Name of the package or null to loisten to all package changes
     */
    String packageName();

    PluginInfo pluginInfo();

    void changed(PackageLoader.Package pkg);

    PackageLoader.Package.Version getPackageVersion();

  }
}

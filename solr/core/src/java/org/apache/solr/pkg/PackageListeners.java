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

import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.solr.core.PluginInfo;

public class PackageListeners {
  // this registry only keeps a weak reference because it does not want to
  // cause a memory leak if the listener forgets to unregister itself
  private List<WeakReference<Listener>> listeners = new ArrayList<>();

  public synchronized void addListener(Listener listener) {
    listeners.add(new WeakReference<>(listener));

  }

  public synchronized void removeListener(Listener listener) {
    Iterator<WeakReference<Listener>> it = listeners.iterator();
    while (it.hasNext()) {
      WeakReference<Listener> ref = it.next();
      Listener pkgListener = ref.get();
      if(pkgListener == null || pkgListener == listener){
        it.remove();
      }

    }

  }

  synchronized void packagesUpdated(List<PackageLoader.Package> pkgs){
    for (PackageLoader.Package pkgInfo : pkgs) {
      invokeListeners(pkgInfo);
    }
  }

  private synchronized void invokeListeners(PackageLoader.Package pkg) {
    for (WeakReference<Listener> ref : listeners) {
      Listener listener = ref.get();
      if (listener != null && listener.packageName().equals(pkg.name())) {
        listener.changed(pkg);
      }
    }
  }

  public List<Listener> getListeners(){
    List<Listener> result = new ArrayList<>();
    for (WeakReference<Listener> ref : listeners) {
      Listener l = ref.get();
      if(l != null){
        result.add(l);
      }

    }
    return result;
  }



  public interface Listener {
    String packageName();

    PluginInfo pluginInfo();

    void changed(PackageLoader.Package pkg);

    PackageLoader.Package.Version getPackageVersion();

  }
}

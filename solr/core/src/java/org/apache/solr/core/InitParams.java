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

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableSet;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.StrUtils;

import static org.apache.solr.common.params.CommonParams.NAME;
import static org.apache.solr.common.params.CommonParams.PATH;
import static org.apache.solr.core.PluginInfo.APPENDS;
import static org.apache.solr.core.PluginInfo.DEFAULTS;
import static org.apache.solr.core.PluginInfo.INVARIANTS;

/**
 * An Object which represents a {@code <initParams>} tag
 */
public class InitParams {
  public static final String TYPE = "initParams";
  public final String name;
  public final Set<String> paths;
  public final NamedList defaults, invariants, appends;
  private PluginInfo pluginInfo;
  private final Set<String> KNOWN_KEYS = ImmutableSet.of(DEFAULTS, INVARIANTS, APPENDS);

  public InitParams(PluginInfo p) {
    this.pluginInfo = p;
    this.name = p.attributes.get(NAME);
    Set<String> paths = null;
    String pathStr = p.attributes.get(PATH);
    if (pathStr != null) {
      paths = Set.copyOf(StrUtils.splitSmart(pathStr, ','));
    }
    this.paths = paths;
    NamedList nl = (NamedList) p.initArgs.get(DEFAULTS);
    defaults = nl == null ? null : nl.getImmutableCopy();
    nl = (NamedList) p.initArgs.get(INVARIANTS);
    invariants = nl == null ? null : nl.getImmutableCopy();
    nl = (NamedList) p.initArgs.get(APPENDS);
    appends = nl == null ? null : nl.getImmutableCopy();
  }

  public boolean matchPath(String name) {
    if (paths == null) return false;
    if (paths.contains(name)) return true;

    for (String path : paths) {
      if (matchPath(path, name)) return true;
    }

    return false;
  }

  private static boolean matchPath(String path, String name) {
    List<String> pathSplit = StrUtils.splitSmart(path, '/');
    List<String> nameSplit = StrUtils.splitSmart(name, '/');
    int i = 0;
    for (; i < nameSplit.size(); i++) {
      String s = nameSplit.get(i);
      String ps = pathSplit.size() > i ? pathSplit.get(i) : null;
      if (ps == null) return false;
      if (s.equals(ps)) continue;
      if ("*".equals(ps) && nameSplit.size() == i + 1) return true;
      if ("**".equals(ps)) return true;
      return false;
    }
    String ps = pathSplit.size() > i ? pathSplit.get(i) : null;
    return "*".equals(ps) || "**".equals(ps);

  }

  public void apply(PluginInfo info) {
    if (!info.isFromSolrConfig()) {
      //if this is a component implicitly defined in code it should be overridden by initPrams
      merge(defaults, (NamedList) info.initArgs.get(DEFAULTS), info.initArgs, DEFAULTS, false);
    } else {
      //if the args is initialized from solrconfig.xml inside the requestHandler it should be taking precedence over  initParams
      merge((NamedList) info.initArgs.get(DEFAULTS), defaults, info.initArgs, DEFAULTS, false);
    }
    merge((NamedList) info.initArgs.get(INVARIANTS), invariants, info.initArgs, INVARIANTS, false);
    merge((NamedList) info.initArgs.get(APPENDS), appends, info.initArgs, APPENDS, true);

    if (pluginInfo.initArgs != null) {
      for (int i = 0; i < pluginInfo.initArgs.size(); i++) {
        String name = pluginInfo.initArgs.getName(i);
        if (KNOWN_KEYS.contains(name)) continue;//already taken care of
        Object val = info.initArgs.get(name);
        if (val != null) continue; //this is explicitly specified in the reqhandler , ignore
        info.initArgs.add(name, pluginInfo.initArgs.getVal(i));
      }
    }
  }

  private static void merge(NamedList first, NamedList second, NamedList sink, String name, boolean appends) {
    if (first == null && second == null) return;
    if (first == null) first = new NamedList();
    NamedList nl = first.clone();
    if (appends) {
      if (second != null) nl.addAll(second);
    } else {
      Set<String> a = new HashSet<>();
      Set<String> b = new HashSet<>();
      for (Object o : first) {
        Map.Entry<String, Object> e = (Map.Entry) o;
        a.add(e.getKey());
      }
      if (second != null) {
        for (Object o : second) {
          Map.Entry<String, Object> e = (Map.Entry) o;
          b.add(e.getKey());
        }
      }
      for (String s : b) {
        if (a.contains(s)) continue;
        for (Object v : second.getAll(s)) nl.add(s, v);
      }
    }
    if (sink.indexOf(name, 0) > -1) {
      sink.setVal(sink.indexOf(name, 0), nl);
    } else {
      sink.add(name, nl);
    }
  }
}

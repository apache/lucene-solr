package org.apache.solr.core;

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

import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.StrUtils;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * An Object which represents a {@code <initParams>} tag
 */
public class InitParams {
  public static final String TYPE = "initParams";
  public final String name;
  public final Set<String> paths;
  public final NamedList defaults,invariants,appends;

  public InitParams(PluginInfo p) {
    this.name = p.attributes.get("name");
    Set<String> paths = null;
    String pathStr = p.attributes.get("path");
    if(pathStr!=null) {
      paths = Collections.unmodifiableSet(new HashSet<>(StrUtils.splitSmart(pathStr, ',')));
    }
    this.paths = paths;
    NamedList nl = (NamedList) p.initArgs.get(PluginInfo.DEFAULTS);
    defaults = nl == null ? null: nl.getImmutableCopy();
    nl = (NamedList) p.initArgs.get(PluginInfo.INVARIANTS);
    invariants = nl == null ? null: nl.getImmutableCopy();
    nl = (NamedList) p.initArgs.get(PluginInfo.APPENDS);
    appends = nl == null ? null: nl.getImmutableCopy();
  }

  public boolean matchPath(String name) {
    if(paths == null) return false;
    if(paths.contains(name)) return true;

    for (String path : paths) {
      if(matchPath(path,name)) return true;
    }

    return false;
  }

  private static boolean matchPath(String path, String name){
    List<String> pathSplit = StrUtils.splitSmart(path, '/');
    List<String> nameSplit = StrUtils.splitSmart(name, '/');
    for (int i = 0; i < nameSplit.size(); i++) {
      String s = nameSplit.get(i);
      String ps = pathSplit.size()>i ?  pathSplit.get(i) :null;
      if(ps == null) return false;
      if(s.equals(ps)) continue;
      if("*".equals(ps) && nameSplit.size()==i+1) return true;
      if("**".equals(ps)) return true;
      return false;
    }
    return true;

  }

  public void apply(NamedList initArgs) {
    merge(defaults, (NamedList) initArgs.get(PluginInfo.DEFAULTS), initArgs, PluginInfo.DEFAULTS, false);
    merge((NamedList) initArgs.get(PluginInfo.INVARIANTS), invariants, initArgs, PluginInfo.INVARIANTS, false);
    merge((NamedList) initArgs.get(PluginInfo.APPENDS), appends, initArgs, PluginInfo.APPENDS, true);
  }

  private static  void merge(NamedList first, NamedList second, NamedList sink, String name, boolean appends) {
    if(first == null && second == null) return;
    if(first == null) first = new NamedList();
    NamedList nl = first.clone();
    if(appends) {
      if(second!=null) nl.addAll(second);
    } else {
      Set<String> a = new HashSet<>();
      Set<String> b = new HashSet<>();
      for (Object o : first)    {
        Map.Entry<String,Object> e = (Map.Entry) o;
        a.add(e.getKey() );
      }
      if(second!=null) {
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
    if(sink.indexOf(name,0) >-1) {
      sink.setVal(sink.indexOf(name, 0), nl);
    } else {
      sink.add(name,nl);
    }
  }
}

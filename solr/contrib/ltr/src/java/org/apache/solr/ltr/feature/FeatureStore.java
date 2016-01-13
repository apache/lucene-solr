package org.apache.solr.ltr.feature;

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

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.solr.ltr.ranking.Feature;
import org.apache.solr.ltr.util.FeatureException;

public class FeatureStore {
  LinkedHashMap<String,Feature> store = new LinkedHashMap<>();
  String storeName;

  public FeatureStore(String storeName) {
    this.storeName = storeName;
  }

  public Feature get(String name) throws FeatureException {
    if (!store.containsKey(name)) {
      throw new FeatureException("missing feature " + name
          + ". Store name was: '" + storeName
          + "'. Possibly this feature exists in another context.");
    }
    return store.get(name);
  }

  public int size() {
    return store.size();
  }

  public boolean containsFeature(String name) {
    return store.containsKey(name);
  }

  public List<Object> featuresAsManagedResources() {
    List<Object> features = new ArrayList<Object>();
    for (Feature f : store.values()) {
      Map<String,Object> o = new LinkedHashMap<>();
      o.put("name", f.getName());
      o.put("type", f.getType());
      o.put("store", storeName);
      o.put("params", f.getParams());
      features.add(o);
    }
    return features;
  }

  public void add(Feature feature) {
    store.put(feature.getName(), feature);
  }

  public Collection<Feature> getFeatures() {
    return store.values();
  }

  public void clear() {
    store.clear();

  }

}

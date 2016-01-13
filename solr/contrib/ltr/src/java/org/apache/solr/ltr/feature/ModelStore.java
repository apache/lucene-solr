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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.solr.ltr.feature.norm.Normalizer;
import org.apache.solr.ltr.ranking.Feature;
import org.apache.solr.ltr.util.ModelException;
import org.apache.solr.ltr.util.NameValidator;

/**
 * Contains the model and features declared.
 */
public class ModelStore {

  private Map<String,ModelMetadata> availableModels;

  public ModelStore() {
    availableModels = new HashMap<>();
  }

  public synchronized ModelMetadata getModel(String name) throws ModelException {
    ModelMetadata model = availableModels.get(name);
    if (model == null) {
      throw new ModelException("cannot find model " + name);
    }
    return model;

  }

  public boolean containsModel(String modelName) {
    return availableModels.containsKey(modelName);
  }

  /**
   * Returns the available models as a list of Maps objects. After an update the
   * managed resources needs to return the resources in this format in order to
   * store in json somewhere (zookeeper, disk...)
   *
   * TODO investigate if it is possible to replace the managed resources' json
   * serializer/deserialiazer.
   *
   * @return the available models as a list of Maps objects
   */
  public List<Object> modelAsManagedResources() {
    List<Object> list = new ArrayList<>();
    for (ModelMetadata modelmeta : availableModels.values()) {
      Map<String,Object> modelMap = new HashMap<>();
      modelMap.put("name", modelmeta.getName());
      modelMap.put("type", modelmeta.getType());
      modelMap.put("store", modelmeta.getFeatureStoreName());
      List<Map<String,Object>> features = new ArrayList<>();
      for (Feature meta : modelmeta.getFeatures()) {
        Map<String,Object> map = new HashMap<String,Object>();
        map.put("name", meta.getName());

        Normalizer n = meta.getNorm();

        if (n != null) {
          Map<String,Object> normalizer = new HashMap<>();
          normalizer.put("type", n.getType());
          normalizer.put("params", n.getParams());
          map.put("norm", normalizer);
        }
        features.add(map);

      }
      modelMap.put("features", features);
      modelMap.put("params", modelmeta.getParams());

      list.add(modelMap);
    }
    return list;
  }

  public void clear() {
    availableModels.clear();

  }

  @Override
  public String toString() {
    return "ModelStore [availableModels=" + availableModels.keySet() + "]";
  }

  public void delete(String childId) {
    availableModels.remove(childId);

  }

  public synchronized void addModel(ModelMetadata modeldata)
      throws ModelException {
    String name = modeldata.getName();

    if (modeldata.getFeatures().isEmpty()) {
      throw new ModelException("no features declared for model "
          + modeldata.getName());
    }
    if (!NameValidator.check(name)) {
      throw new ModelException("invalid model name " + name);
    }

    if (containsModel(name)) {
      throw new ModelException("model '" + name
          + "' already exists. Please use a different name");
    }

    String type = modeldata.getType();
    try {
      Class.forName(type);
    } catch (ClassNotFoundException e) {
      throw new ModelException("cannot find class " + type
          + " implementing model " + name, e);
    }

    // checks for duplicates in the feature
    Set<String> names = new HashSet<>();
    for (Feature feature : modeldata.getFeatures()) {
      String fname = feature.getName();
      if (names.contains(fname)) {
        throw new ModelException("duplicated feature " + fname + " in model "
            + name);
      }

      names.add(fname);
    }

    availableModels.put(modeldata.getName(), modeldata);
  }

}

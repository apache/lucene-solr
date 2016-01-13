package org.apache.solr.ltr.rest;

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

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.ltr.feature.FeatureStore;
import org.apache.solr.ltr.feature.ModelMetadata;
import org.apache.solr.ltr.feature.ModelStore;
import org.apache.solr.ltr.feature.norm.Normalizer;
import org.apache.solr.ltr.feature.norm.impl.IdentityNormalizer;
import org.apache.solr.ltr.ranking.Feature;
import org.apache.solr.ltr.util.FeatureException;
import org.apache.solr.ltr.util.ModelException;
import org.apache.solr.ltr.util.NamedParams;
import org.apache.solr.ltr.util.NormalizerException;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.rest.BaseSolrResource;
import org.apache.solr.rest.ManagedResource;
import org.apache.solr.rest.ManagedResourceStorage.StorageIO;
import org.apache.solr.rest.RestManager;
import org.noggit.ObjectBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Menaged resource for storing a model
 */
public class ManagedModelStore extends ManagedResource implements
    ManagedResource.ChildResourceSupport {

  ModelStore store;
  private ManagedFeatureStore featureStores;

  private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final String MODELS_JSON_FIELD = "models";

  public ManagedModelStore(String resourceId, SolrResourceLoader loader,
      StorageIO storageIO) throws SolrException {
    super(resourceId, loader, storageIO);

    store = new ModelStore();

  }

  public void init(ManagedFeatureStore featureStores) {
    logger.info("INIT model store");
    this.featureStores = featureStores;
  }

  private Object managedData;

  @SuppressWarnings("unchecked")
  @Override
  protected void onManagedDataLoadedFromStorage(NamedList<?> managedInitArgs,
      Object managedData) throws SolrException {
    store.clear();
    // the managed models on the disk or on zookeeper will be loaded in a lazy
    // way, since we need to set the managed features first (unfortunately
    // managed resources do not
    // decouple the creation of a managed resource with the reading of the data
    // from the storage)
    this.managedData = managedData;

  }

  public void loadStoredModels() {
    logger.info("------ managed models ~ loading ------");

    if (managedData != null && managedData instanceof List) {
      List<Map<String,Object>> up = (List<Map<String,Object>>) managedData;
      for (Map<String,Object> u : up) {
        try {
          update(u);
        } catch (ModelException e) {
          throw new SolrException(ErrorCode.BAD_REQUEST, e);
        }
      }
    }
  }

  public static Normalizer getNormalizerInstance(String type, NamedParams params)
      throws NormalizerException {
    Normalizer f;
    Class<?> c;
    try {
      c = Class.forName(type);

      f = (Normalizer) c.newInstance();
    } catch (ClassNotFoundException | InstantiationException
        | IllegalAccessException e) {
      throw new NormalizerException("missing normalizer " + type, e);
    }
    f.setType(type);
    if (params == null) {
      params = NamedParams.EMPTY;
    }
    f.init(params);
    return f;

  }

  @SuppressWarnings("unchecked")
  private Feature parseFeature(Map<String,Object> featureMap,
      FeatureStore featureStore) throws NormalizerException, FeatureException,
      CloneNotSupportedException {
    // FIXME name shouldn't be be null, exception?
    String name = (String) featureMap.get("name");

    Normalizer norm = IdentityNormalizer.INSTANCE;
    if (featureMap.containsKey("norm")) {
      logger.info("adding normalizer {}", featureMap);
      Map<String,Object> normMap = (Map<String,Object>) featureMap.get("norm");
      // FIXME type shouldn't be be null, exception?
      String type = ((String) normMap.get("type"));
      NamedParams params = null;
      if (normMap.containsKey("params")) {
        Object paramsObj = normMap.get("params");
        if (paramsObj != null) {
          params = new NamedParams((Map<String,Object>) paramsObj);
        }
      }
      norm = getNormalizerInstance(type, params);
    }
    if (featureStores == null) {
      throw new FeatureException("missing feature store");
    }

    Feature meta = featureStore.get(name);
    meta = (Feature) meta.clone();
    meta.setNorm(norm);

    return meta;
  }

  @SuppressWarnings("unchecked")
  public ModelMetadata makeModelMetaData(String json) throws ModelException {
    Object parsedJson = null;
    try {
      parsedJson = ObjectBuilder.fromJSON(json);
    } catch (IOException ioExc) {
      throw new ModelException("ObjectBuilder failed parsing json", ioExc);
    }
    return makeModelMetaData((Map<String,Object>) parsedJson);
  }

  @SuppressWarnings("unchecked")
  public ModelMetadata makeModelMetaData(Map<String,Object> map)
      throws ModelException {
    String name = (String) map.get("name");
    Object o = map.get("store");
    String featureStoreName = (o == null) ? ManagedFeatureStore.DEFAULT_FSTORE
        : (String) o;
    NamedParams params = null;
    FeatureStore fstore = featureStores.getFeatureStore(featureStoreName);
    if (!map.containsKey("features")) {
      throw new SolrException(ErrorCode.BAD_REQUEST,
          "Missing mandatory field features");
    }
    List<Object> featureList = (List<Object>) map.get("features");

    List<Feature> features = new ArrayList<>();

    for (Object f : featureList) {
      try {
        Feature feature = parseFeature((Map<String,Object>) f, fstore);
        if (!fstore.containsFeature(feature.getName())) {
          throw new ModelException("missing feature " + feature.getName()
              + " in model " + name);
        }
        features.add(feature);
      } catch (NormalizerException | FeatureException e) {
        throw new SolrException(ErrorCode.BAD_REQUEST, e);
      } catch (CloneNotSupportedException e) {
        throw new SolrException(ErrorCode.BAD_REQUEST, e);
      }
    }

    if (map.containsKey("params")) {
      Map<String,Object> paramsMap = (Map<String,Object>) map.get("params");
      params = new NamedParams(paramsMap);
    }

    String type = (String) map.get("type");
    ModelMetadata meta = null;
    try {
      Class<?> cl = Class.forName(type);
      Constructor<?> cons = cl.getDeclaredConstructor(String.class,
          String.class, List.class, String.class, Collection.class,
          NamedParams.class);
      meta = (ModelMetadata) cons.newInstance(name, type, features,
          featureStoreName, fstore.getFeatures(), params);
    } catch (Exception e) {
      throw new ModelException("Model type does not exist " + type, e);
    }

    return meta;
  }

  @SuppressWarnings("unchecked")
  private void update(Map<String,Object> map) throws ModelException {

    ModelMetadata meta = makeModelMetaData(map);
    try {
      addMetadataModel(meta);
    } catch (ModelException e) {
      throw new SolrException(ErrorCode.BAD_REQUEST, e);
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  protected Object applyUpdatesToManagedData(Object updates) {
    if (updates instanceof List) {
      List<Map<String,Object>> up = (List<Map<String,Object>>) updates;
      for (Map<String,Object> u : up) {
        try {
          update(u);
        } catch (ModelException e) {
          throw new SolrException(ErrorCode.BAD_REQUEST, e);
        }
      }
    }

    if (updates instanceof Map) {
      Map<String,Object> map = (Map<String,Object>) updates;
      try {
        update(map);
      } catch (ModelException e) {
        throw new SolrException(ErrorCode.BAD_REQUEST, e);
      }
    }

    return store.modelAsManagedResources();
  }

  @Override
  public void doDeleteChild(BaseSolrResource endpoint, String childId) {
    if (childId.equals("*")) store.clear();
    if (store.containsModel(childId)) store.delete(childId);
  }

  /**
   * Called to retrieve a named part (the given childId) of the resource at the
   * given endpoint. Note: since we have a unique child managed store we ignore
   * the childId.
   */
  @Override
  public void doGet(BaseSolrResource endpoint, String childId) {

    SolrQueryResponse response = endpoint.getSolrResponse();
    response.add(MODELS_JSON_FIELD, store.modelAsManagedResources());

  }

  public synchronized void addMetadataModel(ModelMetadata modeldata)
      throws ModelException {
    logger.info("adding model {}", modeldata.getName());
    store.addModel(modeldata);
  }

  public ModelMetadata getModel(String modelName) throws ModelException {
    // this function replicates getModelStore().getModel(modelName), but
    // it simplifies the testing (we can avoid to mock also a ModelStore).
    return store.getModel(modelName);
  }

  public ModelStore getModelStore() {
    return store;
  }

  @Override
  public String toString() {
    return "ManagedModelStore [store=" + store + ", featureStores="
        + featureStores + "]";
  }

}

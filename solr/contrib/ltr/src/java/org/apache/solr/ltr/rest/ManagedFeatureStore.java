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

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.ltr.feature.FeatureStore;
import org.apache.solr.ltr.ranking.Feature;
import org.apache.solr.ltr.util.FeatureException;
import org.apache.solr.ltr.util.InvalidFeatureNameException;
import org.apache.solr.ltr.util.NameValidator;
import org.apache.solr.ltr.util.NamedParams;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.rest.BaseSolrResource;
import org.apache.solr.rest.ManagedResource;
import org.apache.solr.rest.ManagedResourceStorage.StorageIO;
import org.apache.solr.rest.RestManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Managed resource for a storing a feature.
 */
public class ManagedFeatureStore extends ManagedResource implements
    ManagedResource.ChildResourceSupport {

  private Map<String,FeatureStore> stores = new HashMap<>();

  private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static final String FEATURES_JSON_FIELD = "features";
  public static final String FEATURE_STORE_JSON_FIELD = "featureStores";
  public static final String DEFAULT_FSTORE = "_DEFAULT_";

  public ManagedFeatureStore(String resourceId, SolrResourceLoader loader,
      StorageIO storageIO) throws SolrException {
    super(resourceId, loader, storageIO);

  }

  public synchronized FeatureStore getFeatureStore(String name) {
    if (name == null) {
      name = DEFAULT_FSTORE;
    }
    if (!stores.containsKey(name)) {
      stores.put(name, new FeatureStore(name));
    }
    return stores.get(name);
  }

  @Override
  protected void onManagedDataLoadedFromStorage(NamedList<?> managedInitArgs,
      Object managedData) throws SolrException {

    stores.clear();
    logger.info("------ managed feature ~ loading ------");
    if (managedData instanceof List) {
      @SuppressWarnings("unchecked")
      List<Map<String,Object>> up = (List<Map<String,Object>>) managedData;
      for (Map<String,Object> u : up) {
        update(u);
      }
    }
  }

  public void update(Map<String,Object> map) {
    String name = (String) map.get("name");
    String type = (String) map.get("type");
    String store = (String) map.get("store");

    NamedParams params = null;

    if (map.containsKey("params")) {
      @SuppressWarnings("unchecked")
      Map<String,Object> np = (Map<String,Object>) map.get("params");
      params = new NamedParams(np);
    }

    try {

      addFeature(name, type, store, params);
    } catch (InvalidFeatureNameException e) {
      throw new SolrException(ErrorCode.BAD_REQUEST, e);
    } catch (FeatureException e) {
      logger.error(e.getMessage());
      e.printStackTrace();
      throw new SolrException(ErrorCode.BAD_REQUEST, e);
    }
  }

  public synchronized void addFeature(String name, String type,
      String featureStore, NamedParams params)
      throws InvalidFeatureNameException, FeatureException {
    if (featureStore == null) {
      featureStore = DEFAULT_FSTORE;
    }

    logger.info("register feature {} -> {} in store [" + featureStore + "]",
        name, type);
    if (!NameValidator.check(name)) {
      throw new InvalidFeatureNameException(name);
    }

    FeatureStore fstore = getFeatureStore(featureStore);

    if (fstore.containsFeature(name)) {
      logger.error(
          "feature {} yet contained in the store, please use a different name",
          name);
      throw new InvalidFeatureNameException(name
          + " yet contained in the store");
    }

    if (params == null) {
      params = NamedParams.EMPTY;
    }

    Feature feature = createFeature(name, type, params, fstore.size());

    fstore.add(feature);
  }

  /**
   * generates an instance this feature.
   */
  private Feature createFeature(String name, String type, NamedParams params,
      int id) throws FeatureException {
    try {
      Class<?> c = Class.forName(type);

      Feature f = (Feature) c.newInstance();
      f.init(name, params, id);
      return f;

    } catch (Exception e) {
      throw new FeatureException(e.getMessage(), e);
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public Object applyUpdatesToManagedData(Object updates) {
    if (updates instanceof List) {
      List<Map<String,Object>> up = (List<Map<String,Object>>) updates;
      for (Map<String,Object> u : up) {
        update(u);
      }
    }

    if (updates instanceof Map) {
      // a unique feature
      update((Map<String,Object>) updates);
    }

    // logger.info("fstore updated, features: ");
    // for (String s : store.getFeatureNames()) {
    // logger.info("  - {}", s);
    //
    // }
    List<Object> features = new ArrayList<>();
    for (FeatureStore fs : stores.values()) {
      features.addAll(fs.featuresAsManagedResources());
    }
    return features;
  }

  @Override
  public void doDeleteChild(BaseSolrResource endpoint, String childId) {
    if (childId.equals("*")) {
      stores.clear();
      return;
    }
    if (stores.containsKey(childId)) {
      stores.remove(childId);
    }
  }

  /**
   * Called to retrieve a named part (the given childId) of the resource at the
   * given endpoint. Note: since we have a unique child feature store we ignore
   * the childId.
   */
  @Override
  public void doGet(BaseSolrResource endpoint, String childId) {
    SolrQueryResponse response = endpoint.getSolrResponse();

    // If no feature store specified, show all the feature stores available
    if (childId == null) {
      response.add(FEATURE_STORE_JSON_FIELD, stores.keySet());
    } else {
      FeatureStore store = getFeatureStore(childId);
      if (store == null) {
        throw new SolrException(ErrorCode.BAD_REQUEST,
            "missing feature store [" + childId + "]");
      }
      response.add(FEATURES_JSON_FIELD, store.featuresAsManagedResources());
    }
  }

}

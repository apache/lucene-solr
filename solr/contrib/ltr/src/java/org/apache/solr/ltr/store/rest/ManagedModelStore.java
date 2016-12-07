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
package org.apache.solr.ltr.store.rest;

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.ltr.feature.Feature;
import org.apache.solr.ltr.model.LTRScoringModel;
import org.apache.solr.ltr.model.ModelException;
import org.apache.solr.ltr.norm.IdentityNormalizer;
import org.apache.solr.ltr.norm.Normalizer;
import org.apache.solr.ltr.store.FeatureStore;
import org.apache.solr.ltr.store.ModelStore;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.rest.BaseSolrResource;
import org.apache.solr.rest.ManagedResource;
import org.apache.solr.rest.ManagedResourceObserver;
import org.apache.solr.rest.ManagedResourceStorage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Menaged resource for storing a model
 */
public class ManagedModelStore extends ManagedResource implements ManagedResource.ChildResourceSupport {

  public static void registerManagedModelStore(SolrResourceLoader solrResourceLoader,
      ManagedResourceObserver managedResourceObserver) {
    solrResourceLoader.getManagedResourceRegistry().registerManagedResource(
        REST_END_POINT,
        ManagedModelStore.class,
        managedResourceObserver);
  }

  public static ManagedModelStore getManagedModelStore(SolrCore core) {
    return (ManagedModelStore) core.getRestManager()
        .getManagedResource(REST_END_POINT);
  }

  /** the model store rest endpoint **/
  public static final String REST_END_POINT = "/schema/model-store";

  /**
   * Managed model store: the name of the attribute containing all the models of
   * a model store
   **/
  private static final String MODELS_JSON_FIELD = "models";

  /** name of the attribute containing a class **/
  static final String CLASS_KEY = "class";
  /** name of the attribute containing the features **/
  static final String FEATURES_KEY = "features";
  /** name of the attribute containing a name **/
  static final String NAME_KEY = "name";
  /** name of the attribute containing a normalizer **/
  static final String NORM_KEY = "norm";
  /** name of the attribute containing parameters **/
  static final String PARAMS_KEY = "params";
  /** name of the attribute containing a store **/
  static final String STORE_KEY = "store";

  private final ModelStore store;
  private ManagedFeatureStore managedFeatureStore;

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public ManagedModelStore(String resourceId, SolrResourceLoader loader,
      ManagedResourceStorage.StorageIO storageIO) throws SolrException {
    super(resourceId, loader, storageIO);
    store = new ModelStore();
  }

  public void setManagedFeatureStore(ManagedFeatureStore managedFeatureStore) {
    log.info("INIT model store");
    this.managedFeatureStore = managedFeatureStore;
  }

  public ManagedFeatureStore getManagedFeatureStore() {
    return managedFeatureStore;
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
    log.info("------ managed models ~ loading ------");

    if ((managedData != null) && (managedData instanceof List)) {
      final List<Map<String,Object>> up = (List<Map<String,Object>>) managedData;
      for (final Map<String,Object> u : up) {
        addModelFromMap(u);
      }
    }
  }

  private void addModelFromMap(Map<String,Object> modelMap) {
    try {
      final LTRScoringModel algo = fromLTRScoringModelMap(solrResourceLoader, modelMap, managedFeatureStore);
      addModel(algo);
    } catch (final ModelException e) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, e);
    }
  }

  public synchronized void addModel(LTRScoringModel ltrScoringModel) throws ModelException {
    try {
      log.info("adding model {}", ltrScoringModel.getName());
      store.addModel(ltrScoringModel);
    } catch (final ModelException e) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, e);
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  protected Object applyUpdatesToManagedData(Object updates) {

    if (updates instanceof List) {
      final List<Map<String,Object>> up = (List<Map<String,Object>>) updates;
      for (final Map<String,Object> u : up) {
        addModelFromMap(u);
      }
    }

    if (updates instanceof Map) {
      final Map<String,Object> map = (Map<String,Object>) updates;
      addModelFromMap(map);
    }

    return modelsAsManagedResources(store.getModels());
  }

  @Override
  public synchronized void doDeleteChild(BaseSolrResource endpoint, String childId) {
    if (childId.equals("*")) {
      store.clear();
    } else {
      store.delete(childId);
    }
    storeManagedData(applyUpdatesToManagedData(null));
  }

  /**
   * Called to retrieve a named part (the given childId) of the resource at the
   * given endpoint. Note: since we have a unique child managed store we ignore
   * the childId.
   */
  @Override
  public void doGet(BaseSolrResource endpoint, String childId) {

    final SolrQueryResponse response = endpoint.getSolrResponse();
    response.add(MODELS_JSON_FIELD,
        modelsAsManagedResources(store.getModels()));
  }

  public LTRScoringModel getModel(String modelName) {
    // this function replicates getModelStore().getModel(modelName), but
    // it simplifies the testing (we can avoid to mock also a ModelStore).
    return store.getModel(modelName);
  }

  @Override
  public String toString() {
    return "ManagedModelStore [store=" + store + ", featureStores="
        + managedFeatureStore + "]";
  }

  /**
   * Returns the available models as a list of Maps objects. After an update the
   * managed resources needs to return the resources in this format in order to
   * store in json somewhere (zookeeper, disk...)
   *
   *
   * @return the available models as a list of Maps objects
   */
  private static List<Object> modelsAsManagedResources(List<LTRScoringModel> models) {
    final List<Object> list = new ArrayList<>(models.size());
    for (final LTRScoringModel model : models) {
      list.add(toLTRScoringModelMap(model));
    }
    return list;
  }

  @SuppressWarnings("unchecked")
  public static LTRScoringModel fromLTRScoringModelMap(SolrResourceLoader solrResourceLoader,
      Map<String,Object> modelMap, ManagedFeatureStore managedFeatureStore) {

    final FeatureStore featureStore =
        managedFeatureStore.getFeatureStore((String) modelMap.get(STORE_KEY));

    final List<Feature> features = new ArrayList<>();
    final List<Normalizer> norms = new ArrayList<>();

    final List<Object> featureList = (List<Object>) modelMap.get(FEATURES_KEY);
    if (featureList != null) {
      for (final Object feature : featureList) {
        final Map<String,Object> featureMap = (Map<String,Object>) feature;
        features.add(lookupFeatureFromFeatureMap(featureMap, featureStore));
        norms.add(createNormalizerFromFeatureMap(solrResourceLoader, featureMap));
      }
    }

    return LTRScoringModel.getInstance(solrResourceLoader,
        (String) modelMap.get(CLASS_KEY), // modelClassName
        (String) modelMap.get(NAME_KEY), // modelName
        features,
        norms,
        featureStore.getName(),
        featureStore.getFeatures(),
        (Map<String,Object>) modelMap.get(PARAMS_KEY));
  }

  private static LinkedHashMap<String,Object> toLTRScoringModelMap(LTRScoringModel model) {
    final LinkedHashMap<String,Object> modelMap = new LinkedHashMap<>(5, 1.0f);

    modelMap.put(NAME_KEY, model.getName());
    modelMap.put(CLASS_KEY, model.getClass().getCanonicalName());
    modelMap.put(STORE_KEY, model.getFeatureStoreName());

    final List<Map<String,Object>> features = new ArrayList<>();
    final List<Feature> featuresList = model.getFeatures();
    final List<Normalizer> normsList = model.getNorms();
    for (int ii=0; ii<featuresList.size(); ++ii) {
      features.add(toFeatureMap(featuresList.get(ii), normsList.get(ii)));
    }
    modelMap.put(FEATURES_KEY, features);

    modelMap.put(PARAMS_KEY, model.getParams());

    return modelMap;
  }

  private static Feature lookupFeatureFromFeatureMap(Map<String,Object> featureMap,
      FeatureStore featureStore) {
    final String featureName = (String)featureMap.get(NAME_KEY);
    return (featureName == null ? null
        : featureStore.get(featureName));
  }

  @SuppressWarnings("unchecked")
  private static Normalizer createNormalizerFromFeatureMap(SolrResourceLoader solrResourceLoader,
      Map<String,Object> featureMap) {
    final Map<String,Object> normMap = (Map<String,Object>)featureMap.get(NORM_KEY);
    return  (normMap == null ? IdentityNormalizer.INSTANCE
        : fromNormalizerMap(solrResourceLoader, normMap));
  }

  private static LinkedHashMap<String,Object> toFeatureMap(Feature feature, Normalizer norm) {
    final LinkedHashMap<String,Object> map = new LinkedHashMap<String,Object>(2, 1.0f);
    map.put(NAME_KEY,  feature.getName());
    map.put(NORM_KEY, toNormalizerMap(norm));
    return map;
  }

  private static Normalizer fromNormalizerMap(SolrResourceLoader solrResourceLoader,
      Map<String,Object> normMap) {
    final String className = (String) normMap.get(CLASS_KEY);

    @SuppressWarnings("unchecked")
    final Map<String,Object> params = (Map<String,Object>) normMap.get(PARAMS_KEY);

    return Normalizer.getInstance(solrResourceLoader, className, params);
  }

  private static LinkedHashMap<String,Object> toNormalizerMap(Normalizer norm) {
    final LinkedHashMap<String,Object> normalizer = new LinkedHashMap<>(2, 1.0f);

    normalizer.put(CLASS_KEY, norm.getClass().getCanonicalName());

    final LinkedHashMap<String,Object> params = norm.paramsToMap();
    if (params != null) {
      normalizer.put(PARAMS_KEY, params);
    }

    return normalizer;
  }

}

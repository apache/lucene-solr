package org.apache.solr.ltr.ranking;

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

import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.component.ResponseBuilder;
import org.apache.solr.handler.component.SearchComponent;
import org.apache.solr.ltr.rest.ManagedFeatureStore;
import org.apache.solr.ltr.rest.ManagedModelStore;
import org.apache.solr.rest.ManagedResource;
import org.apache.solr.rest.ManagedResourceObserver;
import org.apache.solr.util.plugin.SolrCoreAware;

/**
 * The FeatureVectorComponent is intended to be used for offline training of
 * your model in order to fetch the feature vectors of the top matching
 * documents.
 */
public class LTRComponent extends SearchComponent implements SolrCoreAware,
    ManagedResourceObserver {

  // TODO: This is the Solr way, move these to LTRParams in solr.common.params
  public interface LTRParams {
    // Set to true to turn on feature vectors in the LTRComponent
    public static final String FV = "fv";
    public static final String FV_RESPONSE_WRITER = "fvwt";
    public static final String FSTORE_END_POINT = "/schema/fstore";
    public static final String MSTORE_END_POINT = "/schema/mstore";

  }

  public static final String LOGGER_NAME = "solr-feature-logger";
  public static final String FEATURE_PARAM = "featureVectors";

  @SuppressWarnings("rawtypes")
  @Override
  public void init(NamedList args) {}

  @Override
  public void prepare(ResponseBuilder rb) throws IOException {}

  @Override
  public void process(ResponseBuilder rb) throws IOException {}

  @Override
  public String getDescription() {
    return "Manages models and features in Solr";
  }

  @Override
  public void onManagedResourceInitialized(NamedList<?> args,
      ManagedResource res) throws SolrException {
    // FIXME do we need this?
  }

  @Override
  public void inform(SolrCore core) {
    core.getRestManager().addManagedResource(LTRParams.FSTORE_END_POINT,
        ManagedFeatureStore.class);
    ManagedFeatureStore fr = (ManagedFeatureStore) core.getRestManager()
        .getManagedResource(LTRParams.FSTORE_END_POINT);
    core.getRestManager().addManagedResource(LTRParams.MSTORE_END_POINT,
        ManagedModelStore.class);

    ManagedModelStore mr = (ManagedModelStore) core.getRestManager()
        .getManagedResource(LTRParams.MSTORE_END_POINT);
    // core.getResourceLoader().getManagedResourceRegistry().registerManagedResource(LTRParams.FSTORE_END_POINT,
    // , observer);
    mr.init(fr);
    // now we can safely load the models
    mr.loadStoredModels();
  }
}

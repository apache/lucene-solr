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

package org.apache.solr.ltr.model;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.ltr.feature.Feature;
import org.apache.solr.ltr.norm.Normalizer;
import org.noggit.JSONParser;
import org.noggit.ObjectBuilder;

/**
 * A scoring model that fetches the wrapped model from {@link SolrResourceLoader}.
 *
 * <p>This model uses {@link SolrResourceLoader#openResource(String)} for fetching the wrapped model.
 * If you give a relative path for {@code params/resource}, this model will try to load the wrapped model from
 * the instance directory (i.e. ${solr.solr.home}). Otherwise, seek through classpaths.
 *
 * <p>Example configuration:
 * <pre>{
  "class": "org.apache.solr.ltr.model.DefaultWrapperModel",
  "name": "myWrapperModelName",
  "params": {
    "resource": "models/myModel.json"
  }
}</pre>
 *
 * @see SolrResourceLoader#openResource(String)
 */
public class DefaultWrapperModel extends WrapperModel {

  /**
   * resource is part of the LTRScoringModel params map
   * and therefore here it does not individually
   * influence the class hashCode, equals, etc.
   */
  private String resource;

  public DefaultWrapperModel(String name, List<Feature> features, List<Normalizer> norms, String featureStoreName,
      List<Feature> allFeatures, Map<String, Object> params) {
    super(name, features, norms, featureStoreName, allFeatures, params);
  }

  public void setResource(String resource) {
    this.resource = resource;
  }

  @Override
  protected void validate() throws ModelException {
    super.validate();
    if (resource == null) {
      throw new ModelException("no resource configured for model "+name);
    }
  }

  @Override
  public Map<String, Object> fetchModelMap() throws ModelException {
    Map<String, Object> modelMapObj;
    try (InputStream in = openInputStream()) {
      modelMapObj = parseInputStream(in);
    } catch (IOException e) {
      throw new ModelException("Failed to fetch the wrapper model from given resource (" + resource + ")", e);
    }
    return modelMapObj;
  }

  protected InputStream openInputStream() throws IOException {
    return solrResourceLoader.openResource(resource);
  }

  @SuppressWarnings("unchecked")
  protected Map<String, Object> parseInputStream(InputStream in) throws IOException {
    try (Reader reader = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {
      return (Map<String, Object>) new ObjectBuilder(new JSONParser(reader)).getValStrict();
    }
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder(getClass().getSimpleName());
    sb.append("(name=").append(getName());
    sb.append(",resource=").append(resource);
    sb.append(",model=(").append(model.toString()).append(")");

    return sb.toString();
  }
}

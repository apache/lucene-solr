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
package org.apache.solr.ltr.norm;

import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.lucene.search.Explanation;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.util.SolrPluginUtils;

/**
 * A normalizer normalizes the value of a feature. After the feature values
 * have been computed, the {@link Normalizer#normalize(float)} methods will
 * be called and the resulting values will be used by the model.
 */
public abstract class Normalizer {


  public abstract float normalize(float value);

  public abstract LinkedHashMap<String,Object> paramsToMap();

  public Explanation explain(Explanation explain) {
    final float normalized = normalize(explain.getValue().floatValue());
    final String explainDesc = "normalized using " + toString();

    return Explanation.match(normalized, explainDesc, explain);
  }

  public static Normalizer getInstance(SolrResourceLoader solrResourceLoader,
      String className, Map<String,Object> params) {
    final Normalizer f = solrResourceLoader.newInstance(className, Normalizer.class);
    if (params != null) {
      SolrPluginUtils.invokeSetters(f, params.entrySet());
    }
    f.validate();
    return f;
  }

  /**
   * As part of creation of a normalizer instance, this function confirms
   * that the normalizer parameters are valid.
   *
   * @throws NormalizerException
   *             Normalizer Exception
   */
  protected abstract void validate() throws NormalizerException;

}

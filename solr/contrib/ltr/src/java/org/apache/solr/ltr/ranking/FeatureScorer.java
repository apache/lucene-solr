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
import java.util.HashMap;

import org.apache.lucene.search.Scorer;

/**
 * A 'recipe' for computing a feature
 */
public abstract class FeatureScorer extends Scorer {

  protected String name;
  private HashMap<String,Object> docInfo;

  public FeatureScorer(FeatureWeight weight) {
    super(weight);
    this.name = weight.getName();
  }

  @Override
  public abstract float score() throws IOException;

  /**
   * Used in the FeatureWeight's explain. Each feature should implement this
   * returning properties of the specific scorer useful for an explain. For
   * example "MyCustomClassFeature [name=" + name + "myVariable:" + myVariable +
   * "]";
   */
  @Override
  public abstract String toString();

  /**
   * Used to provide context from initial score steps to later reranking steps.
   */
  public void setDocInfo(HashMap<String,Object> iDocInfo) {
    docInfo = iDocInfo;
  }

  public Object getDocParam(String key) {
    return docInfo.get(key);
  }

  public boolean hasDocParam(String key) {
    if (docInfo != null) return docInfo.containsKey(key);
    else return false;
  }

  @Override
  public int freq() throws IOException {
    throw new UnsupportedOperationException();
  }
}

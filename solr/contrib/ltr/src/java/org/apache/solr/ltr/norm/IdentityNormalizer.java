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

/**
 * A Normalizer that normalizes a feature value to itself. This is the
 * default normalizer class, if no normalizer is configured then the
 * IdentityNormalizer will be used.
 */
public class IdentityNormalizer extends Normalizer {

  public static final IdentityNormalizer INSTANCE = new IdentityNormalizer();

  public IdentityNormalizer() {

  }

  @Override
  public float normalize(float value) {
    return value;
  }

  @Override
  public LinkedHashMap<String,Object> paramsToMap() {
    return null;
  }

  @Override
  protected void validate() throws NormalizerException {
  }

  @Override
  public String toString() {
    return getClass().getSimpleName();
  }

}

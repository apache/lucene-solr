package org.apache.solr.ltr.feature.norm.impl;

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

import org.apache.solr.ltr.feature.norm.Normalizer;
import org.apache.solr.ltr.util.NamedParams;
import org.apache.solr.ltr.util.NormalizerException;

public class StandardNormalizer extends Normalizer {

  private float avg;
  private float std;

  public void init(NamedParams params) throws NormalizerException {
    super.init(params);
    if (!params.containsKey("avg")) {
      throw new NormalizerException("missing param avg");
    }
    if (!params.containsKey("std")) {
      throw new NormalizerException("missing param std");
    }
    avg = params.getFloat("avg", 0);
    std = params.getFloat("std", 1);
    if (std <= 0) throw new NormalizerException("std must be > 0");
  }

  @Override
  public float normalize(float value) {
    return (value - avg) / std;
  }

}

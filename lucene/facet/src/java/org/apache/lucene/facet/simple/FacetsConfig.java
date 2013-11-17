package org.apache.lucene.facet.simple;

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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/** By default a dimension is flat and single valued; use
 *  the setters in this class to change that for any dims */
public class FacetsConfig {

  public static final String DEFAULT_INDEXED_FIELD_NAME = "$facets";

  // nocommit pull the delim char into there?
  // nocommit pull DimType into here (shai?)

  private final Map<String,DimConfig> fieldTypes = new ConcurrentHashMap<String,DimConfig>();

  /** @lucene.internal */
  // nocommit expose this to the user, vs the setters?
  public static final class DimConfig {
    boolean hierarchical;
    boolean multiValued;

    /** Actual field where this dimension's facet labels
     *  should be indexed */
    String indexedFieldName = DEFAULT_INDEXED_FIELD_NAME;
  }

  public final static DimConfig DEFAULT_DIM_CONFIG = new DimConfig();

  public DimConfig getDimConfig(String dimName) {
    DimConfig ft = fieldTypes.get(dimName);
    if (ft == null) {
      ft = DEFAULT_DIM_CONFIG;
    }
    return ft;
  }

  // nocommit maybe setDimConfig instead?
  public synchronized void setHierarchical(String dimName) {
    DimConfig ft = fieldTypes.get(dimName);
    if (ft == null) {
      ft = new DimConfig();
      fieldTypes.put(dimName, ft);
    }
    ft.hierarchical = true;
  }

  public synchronized void setMultiValued(String dimName) {
    DimConfig ft = fieldTypes.get(dimName);
    if (ft == null) {
      ft = new DimConfig();
      fieldTypes.put(dimName, ft);
    }
    ft.multiValued = true;
  }

  public synchronized void setIndexedFieldName(String dimName, String indexedFieldName) {
    DimConfig ft = fieldTypes.get(dimName);
    if (ft == null) {
      ft = new DimConfig();
      fieldTypes.put(dimName, ft);
    }
    ft.indexedFieldName = indexedFieldName;
  }

  Map<String,DimConfig> getDimConfigs() {
    return fieldTypes;
  }
}

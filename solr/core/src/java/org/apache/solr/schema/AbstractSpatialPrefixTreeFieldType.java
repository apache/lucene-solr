package org.apache.solr.schema;

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

import org.apache.lucene.spatial.prefix.PrefixTreeStrategy;
import org.apache.lucene.spatial.prefix.tree.SpatialPrefixTree;
import org.apache.lucene.spatial.prefix.tree.SpatialPrefixTreeFactory;
import org.apache.lucene.spatial.query.SpatialArgsParser;
import org.apache.solr.util.MapListener;

import java.util.Map;

/**
 * @see PrefixTreeStrategy
 * @lucene.experimental
 */
public abstract class AbstractSpatialPrefixTreeFieldType<T extends PrefixTreeStrategy> extends AbstractSpatialFieldType<T> {

  /** @see org.apache.lucene.spatial.prefix.RecursivePrefixTreeStrategy#setDefaultFieldValuesArrayLen(int)  */
  public static final String DEFAULT_FIELD_VALUES_ARRAY_LEN = "defaultFieldValuesArrayLen";

  protected SpatialPrefixTree grid;
  private Double distErrPct;
  private Integer defaultFieldValuesArrayLen;

  @Override
  protected void init(IndexSchema schema, Map<String, String> args) {
    super.init(schema, args);

    //Solr expects us to remove the parameters we've used.
    MapListener<String, String> argsWrap = new MapListener<String, String>(args);
    grid = SpatialPrefixTreeFactory.makeSPT(argsWrap, schema.getResourceLoader().getClassLoader(), ctx);
    args.keySet().removeAll(argsWrap.getSeenKeys());

    String v = args.remove(SpatialArgsParser.DIST_ERR_PCT);
    if (v != null)
      distErrPct = Double.valueOf(v);

    v = args.remove(DEFAULT_FIELD_VALUES_ARRAY_LEN);
    if (v != null)
      defaultFieldValuesArrayLen = Integer.valueOf(v);
  }


  @Override
  protected T newSpatialStrategy(String fieldName) {
    T strat = newPrefixTreeStrategy(fieldName);

    if (distErrPct != null)
      strat.setDistErrPct(distErrPct);
    if (defaultFieldValuesArrayLen != null)
      strat.setDefaultFieldValuesArrayLen(defaultFieldValuesArrayLen);

    log.info(this.toString()+" strat: "+strat+" maxLevels: "+ grid.getMaxLevels());//TODO output maxDetailKm
    return strat;
  }

  protected abstract T newPrefixTreeStrategy(String fieldName);

}

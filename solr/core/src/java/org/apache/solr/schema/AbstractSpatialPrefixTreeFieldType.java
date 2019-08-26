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
package org.apache.solr.schema;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.spatial.prefix.PrefixTreeStrategy;
import org.apache.lucene.spatial.prefix.tree.SpatialPrefixTree;
import org.apache.lucene.spatial.prefix.tree.SpatialPrefixTreeFactory;
import org.apache.lucene.spatial.query.SpatialArgsParser;
import org.apache.solr.util.MapListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @Override
  protected void init(IndexSchema schema, Map<String, String> args) {
    super.init(schema, args);

    args.putIfAbsent(SpatialPrefixTreeFactory.VERSION, schema.getDefaultLuceneMatchVersion().toString());

    // Convert the maxDistErr to degrees (based on distanceUnits) since Lucene spatial layer depends on degrees
    if(args.containsKey(SpatialPrefixTreeFactory.MAX_DIST_ERR)) {
      double maxDistErrOriginal = Double.parseDouble(args.get(SpatialPrefixTreeFactory.MAX_DIST_ERR));
      args.put(SpatialPrefixTreeFactory.MAX_DIST_ERR, 
          Double.toString(maxDistErrOriginal * distanceUnits.multiplierFromThisUnitToDegrees()));
    }

    //Solr expects us to remove the parameters we've used.
    MapListener<String, String> argsWrap = new MapListener<>(args);
    grid = SpatialPrefixTreeFactory.makeSPT(argsWrap, schema.getResourceLoader().getClassLoader(), ctx);
    args.keySet().removeAll(argsWrap.getSeenKeys());

    String v = args.remove(SpatialArgsParser.DIST_ERR_PCT);
    if (v != null)
      distErrPct = Double.valueOf(v);

    v = args.remove(DEFAULT_FIELD_VALUES_ARRAY_LEN);
    if (v != null)
      defaultFieldValuesArrayLen = Integer.valueOf(v);
  }
  
  /**
   * This analyzer is not actually used for indexing.  It is implemented here
   * so that the analysis UI will show reasonable tokens.
   */
  @Override
  public Analyzer getIndexAnalyzer() {
    return new Analyzer() {
      @Override
      protected TokenStreamComponents createComponents(String fieldName) {
        PrefixTreeStrategy s = newSpatialStrategy(fieldName == null ? getTypeName() : fieldName);
        PrefixTreeStrategy.ShapeTokenStream ts = s.tokenStream();
        return new TokenStreamComponents(r -> {
          try {
            ts.setShape(parseShape(IOUtils.toString(r)));
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        }, ts);
      }
    };
  }
  
  @Override
  public Analyzer getQueryAnalyzer()
  {
    return getIndexAnalyzer();
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

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
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.spatial.prefix.PrefixTreeStrategy;
import org.apache.lucene.spatial.prefix.tree.SpatialPrefixTree;
import org.apache.lucene.spatial.prefix.tree.SpatialPrefixTreeFactory;
import org.apache.lucene.spatial.query.SpatialArgsParser;
import org.apache.solr.common.SolrException;
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

  /*
   * A list of the properties hardcoded within PrefixTreeStrategy.FIELD_TYPE.
   *
   * Used primarily for validating that user-provided configurations don't disagree with these invariants.  Intentionally
   * left out of this map is 'tokenized' which is hardcoded to 'true' in PrefixTreeStrategy.FIELD_TYPE, but triggers
   * unwanted tokenization logic in Solr query parsing.
   *
   * @see PrefixTreeStrategy#FIELD_TYPE
   */
  public static final Map<String, String> FIELD_TYPE_INVARIANTS = new HashMap<>();
  static {
    FIELD_TYPE_INVARIANTS.put("omitNorms", "true");
    FIELD_TYPE_INVARIANTS.put("termPositions", "false");
    FIELD_TYPE_INVARIANTS.put("termOffsets", "false");
    FIELD_TYPE_INVARIANTS.put("omitTermFreqAndPositions", "true");
    FIELD_TYPE_INVARIANTS.put("omitPositions", "true");
  }

  protected SpatialPrefixTree grid;
  private Double distErrPct;
  private Integer defaultFieldValuesArrayLen;

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @Override
  public void setArgs(IndexSchema schema, Map<String, String> args) {
    for (Map.Entry<String, String> entry : FIELD_TYPE_INVARIANTS.entrySet()) {
      final String key = entry.getKey();
      final String hardcodedValue = entry.getValue();
      final String userConfiguredValue = args.get(entry.getKey());

      if (args.containsKey(key)) {
        if (userConfiguredValue.equals(hardcodedValue)) {
          log.warn("FieldType {} does not allow {} to be specified in schema, hardcoded behavior is {}={}",
                  getClass().getSimpleName(), key, key, hardcodedValue);
        } else {
          final String message = String.format(Locale.ROOT, "FieldType %s is incompatible with %s=%s; hardcoded " +
                          "behavior is %s=%s.  Remove specification in schema",
                  getClass().getSimpleName(), key, userConfiguredValue, key, hardcodedValue);
          throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, message);
        }
      }
    }
    args.putAll(FIELD_TYPE_INVARIANTS);

    super.setArgs(schema, args);
  }

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
   *
   * @see #FIELD_TYPE_INVARIANTS
   */
  @Override
  public void checkSchemaField(final SchemaField field) {
    super.checkSchemaField(field);

    if (! field.omitNorms()) {
      final String message = String.format(Locale.ROOT, "%s of type %s is incompatible with omitNorms=false; hardcoded " +
                      "behavior is omitNorms=true.  Remove specification in schema", field.getName(), getClass().getSimpleName());
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, message);
    }
    if (field.indexOptions() != IndexOptions.DOCS) {
      final String message = String.format(Locale.ROOT,
              "%s of type %s is incompatible with termFreq or position storage.  Remove specification in schema.",
              field.getName(), getClass().getSimpleName());
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, message);
    }
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

    if (log.isInfoEnabled()) {
      log.info("{} strat: {} maxLevels: {}", this, strat, grid.getMaxLevels());//TODO output maxDetailKm
    }
    return strat;
  }

  protected abstract T newPrefixTreeStrategy(String fieldName);

}

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
package org.apache.lucene.queryparser.flexible.standard.config;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.DateTools;
import org.apache.lucene.document.DateTools.Resolution;
import org.apache.lucene.queryparser.flexible.core.config.ConfigurationKey;
import org.apache.lucene.queryparser.flexible.core.config.FieldConfig;
import org.apache.lucene.queryparser.flexible.core.config.QueryConfigHandler;
import org.apache.lucene.queryparser.flexible.standard.StandardQueryParser;
import org.apache.lucene.queryparser.flexible.standard.processors.StandardQueryNodeProcessorPipeline;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.MultiTermQuery.RewriteMethod;

/**
 * This query configuration handler is used for almost every processor defined
 * in the {@link StandardQueryNodeProcessorPipeline} processor pipeline. It holds
 * configuration methods that reproduce the configuration methods that could be set on the old
 * lucene 2.4 QueryParser class.
 * 
 * @see StandardQueryNodeProcessorPipeline
 */
public class StandardQueryConfigHandler extends QueryConfigHandler {

  /**
   * Class holding keys for StandardQueryNodeProcessorPipeline options.
   */
  final public static class ConfigurationKeys  {
    
    /**
     * Key used to set whether position increments is enabled
     * 
     * @see StandardQueryParser#setEnablePositionIncrements(boolean)
     * @see StandardQueryParser#getEnablePositionIncrements()
     */
    final public static ConfigurationKey<Boolean> ENABLE_POSITION_INCREMENTS = ConfigurationKey.newInstance();

    /**
     * Key used to set whether leading wildcards are supported
     * 
     * @see StandardQueryParser#setAllowLeadingWildcard(boolean)
     * @see StandardQueryParser#getAllowLeadingWildcard()
     */
    final public static ConfigurationKey<Boolean> ALLOW_LEADING_WILDCARD = ConfigurationKey.newInstance();
    
    /**
     * Key used to set the {@link Analyzer} used for terms found in the query
     * 
     * @see StandardQueryParser#setAnalyzer(Analyzer)
     * @see StandardQueryParser#getAnalyzer()
     */
    final public static ConfigurationKey<Analyzer> ANALYZER = ConfigurationKey.newInstance();
    
    /**
     * Key used to set the default boolean operator
     * 
     * @see StandardQueryParser#setDefaultOperator(org.apache.lucene.queryparser.flexible.standard.config.StandardQueryConfigHandler.Operator)
     * @see StandardQueryParser#getDefaultOperator()
     */
    final public static ConfigurationKey<Operator> DEFAULT_OPERATOR = ConfigurationKey.newInstance();
    
    /**
     * Key used to set the default phrase slop
     * 
     * @see StandardQueryParser#setPhraseSlop(int)
     * @see StandardQueryParser#getPhraseSlop()
     */
    final public static ConfigurationKey<Integer> PHRASE_SLOP = ConfigurationKey.newInstance();
    
    /**
     * Key used to set the {@link Locale} used when parsing the query
     * 
     * @see StandardQueryParser#setLocale(Locale)
     * @see StandardQueryParser#getLocale()
     */
    final public static ConfigurationKey<Locale> LOCALE = ConfigurationKey.newInstance();
    
    final public static ConfigurationKey<TimeZone> TIMEZONE = ConfigurationKey.newInstance();
    
    /**
     * Key used to set the {@link RewriteMethod} used when creating queries
     * 
     * @see StandardQueryParser#setMultiTermRewriteMethod(org.apache.lucene.search.MultiTermQuery.RewriteMethod)
     * @see StandardQueryParser#getMultiTermRewriteMethod()
     */
    final public static ConfigurationKey<MultiTermQuery.RewriteMethod> MULTI_TERM_REWRITE_METHOD = ConfigurationKey.newInstance();

    /**
     * Key used to set the fields a query should be expanded to when the field
     * is <code>null</code>
     * 
     * @see StandardQueryParser#setMultiFields(CharSequence[])
     * @see StandardQueryParser#getMultiFields()
     */
    final public static ConfigurationKey<CharSequence[]> MULTI_FIELDS = ConfigurationKey.newInstance();
    
    /**
     * Key used to set a field to boost map that is used to set the boost for each field
     * 
     * @see StandardQueryParser#setFieldsBoost(Map)
     * @see StandardQueryParser#getFieldsBoost()
     */
    final public static ConfigurationKey<Map<String,Float>> FIELD_BOOST_MAP = ConfigurationKey.newInstance();

    /**
     * Key used to set a field to {@link Resolution} map that is used
     * to normalize each date field value.
     * 
     * @see StandardQueryParser#setDateResolutionMap(Map)
     * @see StandardQueryParser#getDateResolutionMap()
     */
    final public static ConfigurationKey<Map<CharSequence, DateTools.Resolution>> FIELD_DATE_RESOLUTION_MAP = ConfigurationKey.newInstance();
    
    /**
     * Key used to set the {@link FuzzyConfig} used to create fuzzy queries.
     * 
     * @see StandardQueryParser#setFuzzyMinSim(float)
     * @see StandardQueryParser#setFuzzyPrefixLength(int)
     * @see StandardQueryParser#getFuzzyMinSim()
     * @see StandardQueryParser#getFuzzyPrefixLength()
     */
    final public static ConfigurationKey<FuzzyConfig> FUZZY_CONFIG = ConfigurationKey.newInstance();
    
    /**
     * Key used to set default {@link Resolution}.
     * 
     * @see StandardQueryParser#setDateResolution(org.apache.lucene.document.DateTools.Resolution)
     * @see StandardQueryParser#getDateResolution()
     */
    final public static ConfigurationKey<DateTools.Resolution> DATE_RESOLUTION = ConfigurationKey.newInstance();
    
    /**
     * Key used to set the boost value in {@link FieldConfig} objects.
     * 
     * @see StandardQueryParser#setFieldsBoost(Map)
     * @see StandardQueryParser#getFieldsBoost()
     */
    final public static ConfigurationKey<Float> BOOST = ConfigurationKey.newInstance();
    
    /**
     * Key used to set a field to its {@link PointsConfig}.
     * 
     * @see StandardQueryParser#setPointsConfigMap(Map)
     * @see StandardQueryParser#getPointsConfigMap()
     */
    final public static ConfigurationKey<PointsConfig> POINTS_CONFIG = ConfigurationKey.newInstance();

    /**
     * Key used to set the {@link PointsConfig} in {@link FieldConfig} for point fields.
     * 
     * @see StandardQueryParser#setPointsConfigMap(Map)
     * @see StandardQueryParser#getPointsConfigMap()
     */
    final public static ConfigurationKey<Map<String,PointsConfig>> POINTS_CONFIG_MAP = ConfigurationKey.newInstance();

  }
  
  /**
   * Boolean Operator: AND or OR
   */
  public static enum Operator {
    AND, OR;
  }

  public StandardQueryConfigHandler() {
    // Add listener that will build the FieldConfig.
    addFieldConfigListener(new FieldBoostMapFCListener(this));
    addFieldConfigListener(new FieldDateResolutionFCListener(this));
    addFieldConfigListener(new PointsConfigListener(this));
    
    // Default Values
    set(ConfigurationKeys.ALLOW_LEADING_WILDCARD, false); // default in 2.9
    set(ConfigurationKeys.ANALYZER, null); //default value 2.4
    set(ConfigurationKeys.DEFAULT_OPERATOR, Operator.OR);
    set(ConfigurationKeys.PHRASE_SLOP, 0); //default value 2.4
    set(ConfigurationKeys.ENABLE_POSITION_INCREMENTS, false); //default value 2.4
    set(ConfigurationKeys.FIELD_BOOST_MAP, new LinkedHashMap<String, Float>());
    set(ConfigurationKeys.FUZZY_CONFIG, new FuzzyConfig());
    set(ConfigurationKeys.LOCALE, Locale.getDefault());
    set(ConfigurationKeys.MULTI_TERM_REWRITE_METHOD, MultiTermQuery.CONSTANT_SCORE_REWRITE);
    set(ConfigurationKeys.FIELD_DATE_RESOLUTION_MAP, new HashMap<CharSequence, DateTools.Resolution>());
    
  }

}

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
package org.apache.solr.handler.clustering.carrot2;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * Carrot2 parameter mapping (recognized and mapped if passed via Solr configuration).
 * @lucene.experimental
 */
public final class CarrotParams {

  private static String CARROT_PREFIX = "carrot.";

  public static String ALGORITHM = CARROT_PREFIX + "algorithm";
  
  public static String TITLE_FIELD_NAME = CARROT_PREFIX + "title";
  public static String URL_FIELD_NAME = CARROT_PREFIX + "url";
  public static String SNIPPET_FIELD_NAME = CARROT_PREFIX + "snippet";
  public static String LANGUAGE_FIELD_NAME = CARROT_PREFIX + "lang";
  public static String CUSTOM_FIELD_NAME = CARROT_PREFIX + "custom";
  
  public static String PRODUCE_SUMMARY = CARROT_PREFIX + "produceSummary";
  public static String SUMMARY_FRAGSIZE = CARROT_PREFIX + "fragSize";
  public static String SUMMARY_SNIPPETS = CARROT_PREFIX + "summarySnippets";

  public static String NUM_DESCRIPTIONS = CARROT_PREFIX + "numDescriptions";
  public static String OUTPUT_SUB_CLUSTERS = CARROT_PREFIX + "outputSubClusters";

  public static String LANGUAGE_CODE_MAP = CARROT_PREFIX + "lcmap";

  /**
   * Points to Carrot<sup>2</sup> resources
   */
  public static String RESOURCES_DIR = CARROT_PREFIX + "resourcesDir";

  static final Set<String> CARROT_PARAM_NAMES = new HashSet<>(Arrays.asList(
          ALGORITHM, 
          
          TITLE_FIELD_NAME, 
          URL_FIELD_NAME, 
          SNIPPET_FIELD_NAME, 
          LANGUAGE_FIELD_NAME,
          CUSTOM_FIELD_NAME,
          
          PRODUCE_SUMMARY, 
          SUMMARY_FRAGSIZE, 
          SUMMARY_SNIPPETS, 
          
          NUM_DESCRIPTIONS, 
          OUTPUT_SUB_CLUSTERS, 
          RESOURCES_DIR,
          LANGUAGE_CODE_MAP));

  /** No instances. */
  private CarrotParams() {}
}

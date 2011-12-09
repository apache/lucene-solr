package org.apache.solr.handler.clustering.carrot2;

import java.util.Set;

import com.google.common.collect.ImmutableSet;

/**
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


public interface CarrotParams {

  String CARROT_PREFIX = "carrot.";

  String ALGORITHM = CARROT_PREFIX + "algorithm";
  String TITLE_FIELD_NAME = CARROT_PREFIX + "title";
  String URL_FIELD_NAME = CARROT_PREFIX + "url";
  String SNIPPET_FIELD_NAME = CARROT_PREFIX + "snippet";
  String PRODUCE_SUMMARY = CARROT_PREFIX + "produceSummary";
  String NUM_DESCRIPTIONS = CARROT_PREFIX + "numDescriptions";
  String OUTPUT_SUB_CLUSTERS = CARROT_PREFIX + "outputSubClusters";
  String SUMMARY_FRAGSIZE = CARROT_PREFIX + "fragSize";
  String SUMMARY_SNIPPETS = CARROT_PREFIX + "summarySnippets";

  String LEXICAL_RESOURCES_DIR = CARROT_PREFIX + "lexicalResourcesDir";

  public static final Set<String> CARROT_PARAM_NAMES = ImmutableSet.of(
          ALGORITHM, TITLE_FIELD_NAME, URL_FIELD_NAME, SNIPPET_FIELD_NAME,
          PRODUCE_SUMMARY, NUM_DESCRIPTIONS, OUTPUT_SUB_CLUSTERS, SUMMARY_FRAGSIZE);
}

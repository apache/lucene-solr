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
package org.apache.solr.ltr;

import org.apache.solr.request.SolrQueryRequest;

public class SolrQueryRequestContextUtils {

  /** key prefix to reduce possibility of clash with other code's key choices **/
  private static final String LTR_PREFIX = "ltr.";

  /** key of the feature logger in the request context **/
  private static final String FEATURE_LOGGER = LTR_PREFIX + "feature_logger";

  /** key of the scoring queries in the request context **/
  private static final String SCORING_QUERIES = LTR_PREFIX + "scoring_queries";

  /** key of the isExtractingFeatures flag in the request context **/
  private static final String IS_EXTRACTING_FEATURES = LTR_PREFIX + "isExtractingFeatures";

  /** key of the feature vector store name in the request context **/
  private static final String STORE = LTR_PREFIX + "store";

  /** feature logger accessors **/

  public static void setFeatureLogger(SolrQueryRequest req, FeatureLogger featureLogger) {
    req.getContext().put(FEATURE_LOGGER, featureLogger);
  }

  public static FeatureLogger getFeatureLogger(SolrQueryRequest req) {
    return (FeatureLogger) req.getContext().get(FEATURE_LOGGER);
  }

  /** scoring query accessors **/

  public static void setScoringQueries(SolrQueryRequest req, LTRScoringQuery[] scoringQueries) {
    req.getContext().put(SCORING_QUERIES, scoringQueries);
  }

  public static LTRScoringQuery[] getScoringQueries(SolrQueryRequest req) {
    return (LTRScoringQuery[]) req.getContext().get(SCORING_QUERIES);
  }

  /** isExtractingFeatures flag accessors **/

  public static void setIsExtractingFeatures(SolrQueryRequest req) {
    req.getContext().put(IS_EXTRACTING_FEATURES, Boolean.TRUE);
  }

  public static void clearIsExtractingFeatures(SolrQueryRequest req) {
    req.getContext().put(IS_EXTRACTING_FEATURES, Boolean.FALSE);
  }

  public static boolean isExtractingFeatures(SolrQueryRequest req) {
    return Boolean.TRUE.equals(req.getContext().get(IS_EXTRACTING_FEATURES));
  }

  /** feature vector store name accessors **/

  public static void setFvStoreName(SolrQueryRequest req, String fvStoreName) {
    req.getContext().put(STORE, fvStoreName);
  }

  public static String getFvStoreName(SolrQueryRequest req) {
    return (String) req.getContext().get(STORE);
  }

}


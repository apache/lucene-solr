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
package org.apache.solr.common.params;

/**
 * Defines the request parameters used by all analysis request handlers.
 *
 *
 * @since solr 1.4
 */
public interface AnalysisParams {

  /**
   * The prefix for all parameters.
   */
  static final String PREFIX = "analysis";

  /**
   * Holds the query to be analyzed.
   */
  static final String QUERY = PREFIX + ".query";

  /**
   * Set to {@code true} to indicate that the index tokens that match query tokens should be marked as "matched".
   */
  static final String SHOW_MATCH = PREFIX + ".showmatch";


  //===================================== FieldAnalysisRequestHandler Params =========================================

  /**
   * Holds the value of the field which should be analyzed.
   */
  static final String FIELD_NAME = PREFIX + ".fieldname";

  /**
   * Holds a comma-separated list of field types that the analysis should be peformed for.
   */
  static final String FIELD_TYPE = PREFIX + ".fieldtype";

  /**
   * Hodls a comma-separated list of field named that the analysis should be performed for.
   */
  static final String FIELD_VALUE = PREFIX + ".fieldvalue";
}

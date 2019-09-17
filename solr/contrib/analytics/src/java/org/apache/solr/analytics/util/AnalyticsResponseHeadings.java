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
package org.apache.solr.analytics.util;

/**
 * Holds the headers for analytics responses.
 */
public class AnalyticsResponseHeadings {

  public static final String COMPLETED_HEADER = "analytics_response";
  public static final String RESULTS = "results";
  public static final String GROUPINGS = "groupings";
  public static final String FACET_VALUE = "value";
  public static final String PIVOT_NAME = "pivot";
  public static final String PIVOT_CHILDREN = "children";

  // Old Olap-style
  public static final String COMPLETED_OLD_HEADER = "stats";
  public static final String FIELD_FACETS = "fieldFacets";
  public static final String RANGE_FACETS = "rangeFacets";
  public static final String QUERY_FACETS = "queryFacets";
}

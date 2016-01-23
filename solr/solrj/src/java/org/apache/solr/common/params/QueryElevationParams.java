package org.apache.solr.common.params;

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


/**
 * Parameters used with the QueryElevationComponent
 *
 **/
public interface QueryElevationParams {

  String ENABLE = "enableElevation";
  String EXCLUSIVE = "exclusive";
  String FORCE_ELEVATION = "forceElevation";
  String IDS = "elevateIds";
  String EXCLUDE = "excludeIds";
  /**
   * The name of the field that editorial results will be written out as when using the QueryElevationComponent, which
   * automatically configures the EditorialMarkerFactory.  The default name is "elevated"
   * <br>
   * See http://wiki.apache.org/solr/DocTransformers
   */
  String EDITORIAL_MARKER_FIELD_NAME = "editorialMarkerFieldName";
  /**
   * The name of the field that excluded editorial results will be written out as when using the QueryElevationComponent, which
   * automatically configures the EditorialMarkerFactory.  The default name is "excluded".  This is only used
   * when {@link #MARK_EXCLUDES} is set to true at query time.
   * <br>
   * See http://wiki.apache.org/solr/DocTransformers
   */
  String EXCLUDE_MARKER_FIELD_NAME = "excludeMarkerFieldName";

  /**
   * Instead of removing excluded items from the results, passing in this parameter allows you to get back the excluded items, but to mark them
   * as excluded.
   */
  String MARK_EXCLUDES = "markExcludes";
}

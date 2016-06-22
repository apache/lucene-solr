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
 * 
 */
package org.apache.solr.common.params;

/**
 * Parameters used when dealing with Search queries.
 */
public interface SearchParams {
  
  /** The range check. */
  String RANGE_CHECK = "rangeCheck";

  /** The range check exact. */
  String RANGE_CHECK_EXACT = "exact";
  
  /** The range check single. */
  String RANGE_CHECK_SINGLE = "single";
}

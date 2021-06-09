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

package org.apache.lucene.sandbox.search;

import java.util.Locale;

/** This enum breaks down the query into different sections to describe what was timed. */
public enum QueryProfilerTimingType {
  CREATE_WEIGHT,
  BUILD_SCORER,
  NEXT_DOC,
  ADVANCE,
  MATCH,
  SCORE,
  SHALLOW_ADVANCE,
  COMPUTE_MAX_SCORE,
  SET_MIN_COMPETITIVE_SCORE;

  @Override
  public String toString() {
    return name().toLowerCase(Locale.ROOT);
  }
}

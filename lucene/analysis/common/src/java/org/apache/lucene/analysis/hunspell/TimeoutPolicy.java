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
package org.apache.lucene.analysis.hunspell;

/** A strategy determining what to do when Hunspell API calls take too much time */
public enum TimeoutPolicy {
  /** Let the computation complete even if it takes ages */
  NO_TIMEOUT,

  /** Just stop the calculation and return whatever has been computed so far */
  RETURN_PARTIAL_RESULT,

  /**
   * Throw an exception (e.g. {@link SuggestionTimeoutException}) to make it more clear to the
   * caller that the timeout happened and the returned results might not be reliable or
   * reproducible.
   */
  THROW_EXCEPTION
}

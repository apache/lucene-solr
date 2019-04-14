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

package org.apache.lucene.luwak;

/**
 * Class representing an error that occurred during a matcher run
 */
public class MatchError {

  /**
   * The query running when the exception happened
   */
  public final String queryId;

  /**
   * The exception
   */
  public final Exception error;

  /**
   * Create a new MatchError
   *
   * @param queryId the query id
   * @param error   the error
   */
  public MatchError(String queryId, Exception error) {
    this.queryId = queryId;
    this.error = error;
  }

  @Override
  public String toString() {
    return queryId + ":" + error.getMessage();
  }
}

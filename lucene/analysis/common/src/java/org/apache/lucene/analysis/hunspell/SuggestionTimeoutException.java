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

import java.util.Collections;
import java.util.List;

/**
 * An exception thrown when {@link Hunspell#suggest} call takes too long, if {@link
 * TimeoutPolicy#THROW_EXCEPTION} is used.
 */
public class SuggestionTimeoutException extends RuntimeException {
  private final List<String> partialResult;

  SuggestionTimeoutException(String message, List<String> partialResult) {
    super(message);
    this.partialResult = partialResult == null ? null : Collections.unmodifiableList(partialResult);
  }

  /**
   * @return partial result calculated by {@link Hunspell#suggest} before the time limit was
   *     exceeded
   */
  public List<String> getPartialResult() {
    return partialResult;
  }
}

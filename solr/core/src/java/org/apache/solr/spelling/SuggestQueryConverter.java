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
package org.apache.solr.spelling;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

/**
 * Passes the entire query string to the configured analyzer as-is.
 **/
public class SuggestQueryConverter extends SpellingQueryConverter {

  @Override
  public Collection<Token> convert(String original) {
    if (original == null) { // this can happen with q.alt = and no query
      return Collections.emptyList();
    }

    Collection<Token> result = new ArrayList<>();
    try {
      analyze(result, original, 0, 0);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return result;
  }
}

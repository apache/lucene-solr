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
package org.apache.lucene.analysis.util;

import org.apache.lucene.analysis.TokenStream;

/**
 * Abstract base class for TokenFilters that may remove tokens.
 * You have to implement {@link #accept} and return a boolean if the current
 * token should be preserved. {@link #incrementToken} uses this method
 * to decide if a token should be passed to the caller.
 * @deprecated This class moved to Lucene-Core module:
 *  {@link org.apache.lucene.analysis.FilteringTokenFilter}
 */
@Deprecated
public abstract class FilteringTokenFilter extends org.apache.lucene.analysis.FilteringTokenFilter {

  /**
   * Create a new {@link FilteringTokenFilter}.
   * @param in      the {@link TokenStream} to consume
   */
  public FilteringTokenFilter(TokenStream in) {
    super(in);
  }

}

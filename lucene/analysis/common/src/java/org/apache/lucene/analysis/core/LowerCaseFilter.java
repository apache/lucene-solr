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
package org.apache.lucene.analysis.core;


import org.apache.lucene.analysis.TokenStream;

/**
 * Normalizes token text to lower case.
 * <p>
 * This class moved to Lucene Core, but a reference in the {@code analysis/common} module
 * is preserved for documentation purposes and consistency with filter factory.
 * @see org.apache.lucene.analysis.LowerCaseFilter
 * @see LowerCaseFilterFactory
 */
public final class LowerCaseFilter extends org.apache.lucene.analysis.LowerCaseFilter {
  
  /**
   * Create a new LowerCaseFilter, that normalizes token text to lower case.
   * 
   * @param in TokenStream to filter
   */
  public LowerCaseFilter(TokenStream in) {
    super(in);
  }
  
}

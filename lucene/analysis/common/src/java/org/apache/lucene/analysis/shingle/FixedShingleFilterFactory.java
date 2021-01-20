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

package org.apache.lucene.analysis.shingle;

import java.util.Map;
import org.apache.lucene.analysis.TokenFilterFactory;
import org.apache.lucene.analysis.TokenStream;

/**
 * Factory for {@link FixedShingleFilter}
 *
 * <p>Parameters are:
 *
 * <ul>
 *   <li>shingleSize - how many tokens should be combined into each shingle (default: 2)
 *   <li>tokenSeparator - how tokens should be joined together in the shingle (default: space)
 *   <li>fillerToken - what should be added in place of stop words (default: _ )
 * </ul>
 *
 * @since 7.4.0
 * @lucene.spi {@value #NAME}
 */
public class FixedShingleFilterFactory extends TokenFilterFactory {

  /** SPI name */
  public static final String NAME = "fixedShingle";

  private final int shingleSize;
  private final String tokenSeparator;
  private final String fillerToken;

  public FixedShingleFilterFactory(Map<String, String> args) {
    super(args);
    this.shingleSize = getInt(args, "shingleSize", 2);
    this.tokenSeparator = get(args, "tokenSeparator", " ");
    this.fillerToken = get(args, "fillerToken", "_");
  }

  /** Default ctor for compatibility with SPI */
  public FixedShingleFilterFactory() {
    throw defaultCtorException();
  }

  @Override
  public TokenStream create(TokenStream input) {
    return new FixedShingleFilter(input, shingleSize, tokenSeparator, fillerToken);
  }
}

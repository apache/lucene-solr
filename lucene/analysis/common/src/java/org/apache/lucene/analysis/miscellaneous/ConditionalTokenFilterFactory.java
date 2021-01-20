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

package org.apache.lucene.analysis.miscellaneous;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenFilterFactory;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.util.ResourceLoader;
import org.apache.lucene.util.ResourceLoaderAware;

/**
 * Abstract parent class for analysis factories that create {@link ConditionalTokenFilter} instances
 *
 * @since 7.4.0
 * @lucene.spi {@value #NAME}
 */
public abstract class ConditionalTokenFilterFactory extends TokenFilterFactory
    implements ResourceLoaderAware {

  /** SPI name */
  public static final String NAME = "conditional";

  private List<TokenFilterFactory> innerFilters;

  protected ConditionalTokenFilterFactory(Map<String, String> args) {
    super(args);
  }

  /** Default ctor for compatibility with SPI */
  public ConditionalTokenFilterFactory() {
    throw defaultCtorException();
  }

  /**
   * Set the inner filter factories to produce the {@link TokenFilter}s that will be wrapped by the
   * {@link ConditionalTokenFilter}
   */
  public void setInnerFilters(List<TokenFilterFactory> innerFilters) {
    this.innerFilters = innerFilters;
  }

  @Override
  public TokenStream create(TokenStream input) {
    if (innerFilters == null || innerFilters.size() == 0) {
      return input;
    }
    Function<TokenStream, TokenStream> innerStream =
        ts -> {
          for (TokenFilterFactory factory : innerFilters) {
            ts = factory.create(ts);
          }
          return ts;
        };
    return create(input, innerStream);
  }

  @Override
  public final void inform(ResourceLoader loader) throws IOException {
    if (innerFilters == null) return;
    for (TokenFilterFactory factory : innerFilters) {
      if (factory instanceof ResourceLoaderAware) {
        ((ResourceLoaderAware) factory).inform(loader);
      }
    }
    doInform(loader);
  }

  /** Initialises this component with the corresponding {@link ResourceLoader} */
  protected void doInform(ResourceLoader loader) throws IOException {}

  /** Modify the incoming {@link TokenStream} with a {@link ConditionalTokenFilter} */
  protected abstract ConditionalTokenFilter create(
      TokenStream input, Function<TokenStream, TokenStream> inner);
}

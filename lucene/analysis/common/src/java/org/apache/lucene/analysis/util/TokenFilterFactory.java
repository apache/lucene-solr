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


import java.util.Map;
import java.util.Set;

import org.apache.lucene.analysis.TokenStream;

/**
 * Abstract parent class for analysis factories that create {@link org.apache.lucene.analysis.TokenFilter}
 * instances.
 *
 * @since 3.1
 */
public abstract class TokenFilterFactory extends AbstractAnalysisFactory {

  private static final AnalysisSPILoader<TokenFilterFactory> loader =
      new AnalysisSPILoader<>(TokenFilterFactory.class,
          new String[] { "TokenFilterFactory", "FilterFactory" });

  /** looks up a tokenfilter by name from context classpath */
  public static TokenFilterFactory forName(String name, Map<String,String> args) {
    return loader.newInstance(name, args);
  }
  
  /** looks up a tokenfilter class by name from context classpath */
  public static Class<? extends TokenFilterFactory> lookupClass(String name) {
    return loader.lookupClass(name);
  }
  
  /** returns a list of all available tokenfilter names from context classpath */
  public static Set<String> availableTokenFilters() {
    return loader.availableServices();
  }

  /** looks up a SPI name for the specified token filter factory */
  public static String findSPIName(Class<? extends TokenFilterFactory> serviceClass) {
    try {
      return lookupSPIName(serviceClass);
    } catch (NoSuchFieldException | IllegalAccessException | IllegalStateException e) {
      throw new IllegalStateException(e);
    }
  }

  /** 
   * Reloads the factory list from the given {@link ClassLoader}.
   * Changes to the factories are visible after the method ends, all
   * iterators ({@link #availableTokenFilters()},...) stay consistent. 
   * 
   * <p><b>NOTE:</b> Only new factories are added, existing ones are
   * never removed or replaced.
   * 
   * <p><em>This method is expensive and should only be called for discovery
   * of new factories on the given classpath/classloader!</em>
   */
  public static void reloadTokenFilters(ClassLoader classloader) {
    loader.reload(classloader);
  }
  
  /**
   * Initialize this factory via a set of key-value pairs.
   */
  protected TokenFilterFactory(Map<String,String> args) {
    super(args);
  }

  /** Transform the specified input TokenStream */
  public abstract TokenStream create(TokenStream input);

  /**
   * Normalize the specified input TokenStream
   * While the default implementation returns input unchanged,
   * filters that should be applied at normalization time can delegate to {@code create} method.
   */
  public TokenStream normalize(TokenStream input) {
    return input;
  }
}

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


import java.io.Reader;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.analysis.CharFilter;

/**
 * Abstract parent class for analysis factories that create {@link CharFilter}
 * instances.
 *
 * @since 3.1
 */
public abstract class CharFilterFactory extends AbstractAnalysisFactory {

  private static final AnalysisSPILoader<CharFilterFactory> loader =
      new AnalysisSPILoader<>(CharFilterFactory.class);
  
  /** looks up a charfilter by name from context classpath */
  public static CharFilterFactory forName(String name, Map<String,String> args) {
    return loader.newInstance(name, args);
  }
  
  /** looks up a charfilter class by name from context classpath */
  public static Class<? extends CharFilterFactory> lookupClass(String name) {
    return loader.lookupClass(name);
  }
  
  /** returns a list of all available charfilter names */
  public static Set<String> availableCharFilters() {
    return loader.availableServices();
  }

  /** looks up a SPI name for the specified char filter factory */
  public static String findSPIName(Class<? extends CharFilterFactory> serviceClass) {
    try {
      return lookupSPIName(serviceClass);
    } catch (NoSuchFieldException | IllegalAccessException | IllegalStateException e) {
      throw new IllegalStateException(e);
    }
  }

  /** 
   * Reloads the factory list from the given {@link ClassLoader}.
   * Changes to the factories are visible after the method ends, all
   * iterators ({@link #availableCharFilters()},...) stay consistent. 
   * 
   * <p><b>NOTE:</b> Only new factories are added, existing ones are
   * never removed or replaced.
   * 
   * <p><em>This method is expensive and should only be called for discovery
   * of new factories on the given classpath/classloader!</em>
   */
  public static void reloadCharFilters(ClassLoader classloader) {
    loader.reload(classloader);
  }

  /**
   * Initialize this factory via a set of key-value pairs.
   */
  protected CharFilterFactory(Map<String,String> args) {
    super(args);
  }

  /** Wraps the given Reader with a CharFilter. */
  public abstract Reader create(Reader input);

  /**
   * Normalize the specified input Reader
   * While the default implementation returns input unchanged,
   * char filters that should be applied at normalization time can delegate to {@code create} method.
   */
  public Reader normalize(Reader input) {
    return input;
  }
}

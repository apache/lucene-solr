package org.apache.lucene.analysis.util;

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

import java.io.Reader;
import java.util.Set;

import org.apache.lucene.analysis.CharFilter;

/**
 * Abstract parent class for analysis factories that create {@link CharFilter}
 * instances.
 */
public abstract class CharFilterFactory extends AbstractAnalysisFactory {

  private static final AnalysisSPILoader<CharFilterFactory> loader =
      new AnalysisSPILoader<CharFilterFactory>(CharFilterFactory.class);
  
  /** looks up a charfilter by name */
  public static CharFilterFactory forName(String name) {
    return loader.newInstance(name);
  }
  
  /** returns a list of all available charfilter names */
  public static Set<String> availableCharFilters() {
    return loader.availableServices();
  }

  public abstract CharFilter create(Reader input);
}

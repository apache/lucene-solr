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

import org.apache.lucene.analysis.Tokenizer;

import java.io.Reader;
import java.util.Set;

/**
 * Abstract parent class for analysis factories that create {@link Tokenizer}
 * instances.
 */
public abstract class TokenizerFactory extends AbstractAnalysisFactory {

  private static final AnalysisSPILoader<TokenizerFactory> loader =
      getSPILoader(Thread.currentThread().getContextClassLoader());
  
  /**
   * Used by e.g. Apache Solr to get a correctly configured instance
   * of {@link AnalysisSPILoader} from Solr's classpath.
   * @lucene.internal
   */
  public static AnalysisSPILoader<TokenizerFactory> getSPILoader(ClassLoader classloader) {
    return new AnalysisSPILoader<TokenizerFactory>(TokenizerFactory.class, classloader);
  }
  
  /** looks up a tokenizer by name from context classpath */
  public static TokenizerFactory forName(String name) {
    return loader.newInstance(name);
  }
  
  /** looks up a tokenizer class by name from context classpath */
  public static Class<? extends TokenizerFactory> lookupClass(String name) {
    return loader.lookupClass(name);
  }
  
  /** returns a list of all available tokenizer names from context classpath */
  public static Set<String> availableTokenizers() {
    return loader.availableServices();
  }
  
  /** Creates a TokenStream of the specified input */
  public abstract Tokenizer create(Reader input);
}

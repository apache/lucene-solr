package org.apache.lucene.analysis.uima.ae;

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

import java.util.HashMap;
import java.util.Map;

/**
 * Singleton factory class responsible of {@link AEProvider}s' creation
 */
public class AEProviderFactory {

  private static final AEProviderFactory instance = new AEProviderFactory();

  private final Map<String, AEProvider> providerCache = new HashMap<String, AEProvider>();

  private AEProviderFactory() {
    // Singleton
  }

  public static AEProviderFactory getInstance() {
    return instance;
  }

  /**
   * @param keyPrefix         a prefix of the key used to cache the AEProvider
   * @param aePath            the AnalysisEngine descriptor path
   * @param runtimeParameters map of runtime parameters to configure inside the AnalysisEngine
   * @return AEProvider
   */
  public synchronized AEProvider getAEProvider(String keyPrefix, String aePath, Map<String, Object> runtimeParameters) {
    String key = new StringBuilder(keyPrefix != null ? keyPrefix : "").append(aePath).append(runtimeParameters != null ?
        runtimeParameters.toString() : "").toString();
    if (providerCache.get(key) == null) {
      AEProvider aeProvider;
      if (runtimeParameters != null)
        aeProvider = new OverridingParamsAEProvider(aePath, runtimeParameters);
      else
        aeProvider = new BasicAEProvider(aePath);
      providerCache.put(key, aeProvider);
    }
    return providerCache.get(key);
  }

  /**
   * @param aePath the AnalysisEngine descriptor path
   * @return AEProvider
   */
  public synchronized AEProvider getAEProvider(String aePath) {
    return getAEProvider(null, aePath, null);
  }

  /**
   * @param aePath            the AnalysisEngine descriptor path
   * @param runtimeParameters map of runtime parameters to configure inside the AnalysisEngine
   * @return AEProvider
   */
  public synchronized AEProvider getAEProvider(String aePath, Map<String, Object> runtimeParameters) {
    return getAEProvider(null, aePath, runtimeParameters);
  }
}

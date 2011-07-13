package org.apache.solr.uima.processor.ae;

/**
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
 * 
 * @version $Id$
 */
public class AEProviderFactory {

  private static AEProviderFactory instance;

  private Map<String, AEProvider> providerCache = new HashMap<String, AEProvider>();

  private AEProviderFactory() {
    // Singleton
  }

  public static AEProviderFactory getInstance() {
    if (instance == null) {
      instance = new AEProviderFactory();
    }
    return instance;
  }

  public synchronized AEProvider getAEProvider(String core, String aePath,
          Map<String, Object> runtimeParameters) {
    String key = new StringBuilder(core).append(aePath).toString();
    if (providerCache.get(key) == null) {
      providerCache.put(key, new OverridingParamsAEProvider(aePath, runtimeParameters));
    }
    return providerCache.get(key);
  }
}

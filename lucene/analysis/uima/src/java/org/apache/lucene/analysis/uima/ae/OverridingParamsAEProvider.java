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

import org.apache.uima.analysis_engine.AnalysisEngineDescription;

import java.util.Map;

/**
 * {@link AEProvider} implementation that creates an Aggregate AE from the given path, also
 * injecting runtime parameters defined in the solrconfig.xml Solr configuration file and assigning
 * them as overriding parameters in the aggregate AE
 */
public class OverridingParamsAEProvider extends BasicAEProvider {

  private final Map<String, Object> runtimeParameters;

  public OverridingParamsAEProvider(String aePath, Map<String, Object> runtimeParameters) {
    super(aePath);
    this.runtimeParameters = runtimeParameters;
  }
  
  @Override
  protected void configureDescription(AnalysisEngineDescription description) {
    for (String attributeName : runtimeParameters.keySet()) {
      Object val = getRuntimeValue(description, attributeName);
      description.getAnalysisEngineMetaData().getConfigurationParameterSettings().setParameterValue(
          attributeName, val);
    }
  }

  /* create the value to inject in the runtime parameter depending on its declared type */
  private Object getRuntimeValue(AnalysisEngineDescription desc, String attributeName) {
    String type = desc.getAnalysisEngineMetaData().getConfigurationParameterDeclarations().
        getConfigurationParameter(null, attributeName).getType();
    // TODO : do it via reflection ? i.e. Class paramType = Class.forName(type)...
    Object val = null;
    Object runtimeValue = runtimeParameters.get(attributeName);
    if (runtimeValue != null) {
      if ("String".equals(type)) {
        val = String.valueOf(runtimeValue);
      } else if ("Integer".equals(type)) {
        val = Integer.valueOf(runtimeValue.toString());
      } else if ("Boolean".equals(type)) {
        val = Boolean.valueOf(runtimeValue.toString());
      } else if ("Float".equals(type)) {
        val = Float.valueOf(runtimeValue.toString());
      }
    }

    return val;
  }

}
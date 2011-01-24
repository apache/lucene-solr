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

import java.net.URL;
import java.util.Map;

import org.apache.uima.UIMAFramework;
import org.apache.uima.analysis_engine.AnalysisEngine;
import org.apache.uima.analysis_engine.AnalysisEngineDescription;
import org.apache.uima.resource.ResourceInitializationException;
import org.apache.uima.util.XMLInputSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link AEProvider} implementation that creates an Aggregate AE from the given path, also
 * injecting runtime parameters defined in the solrconfig.xml Solr configuration file and assigning
 * them as overriding parameters in the aggregate AE
 * 
 * @version $Id$
 */
public class OverridingParamsAEProvider implements AEProvider {

  private static Logger log = LoggerFactory.getLogger(OverridingParamsAEProvider.class);

  private String aeFilePath;

  private AnalysisEngine cachedAE;

  private Map<String, String> runtimeParameters;

  public OverridingParamsAEProvider(String aeFilePath, Map<String, String> runtimeParameters) {
    this.aeFilePath = aeFilePath;
    this.runtimeParameters = runtimeParameters;
  }

  public synchronized AnalysisEngine getAE() throws ResourceInitializationException {
    try {
      if (cachedAE == null) {
        // get Resource Specifier from XML file
        URL url = this.getClass().getResource(aeFilePath);
        XMLInputSource in = new XMLInputSource(url);

        // get AE description
        AnalysisEngineDescription desc = UIMAFramework.getXMLParser()
                .parseAnalysisEngineDescription(in);

        /* iterate over each AE (to set runtime parameters) */
        for (String attributeName : runtimeParameters.keySet()) {
          desc.getAnalysisEngineMetaData().getConfigurationParameterSettings().setParameterValue(
                  attributeName, runtimeParameters.get(attributeName));
          log.info(new StringBuilder("setting ").append(attributeName).append(" : ").append(
                  runtimeParameters.get(attributeName)).toString());
        }
        // create AE here
        cachedAE = UIMAFramework.produceAnalysisEngine(desc);
        if (log.isDebugEnabled())
          log.debug(new StringBuilder("AE ").append(cachedAE.getAnalysisEngineMetaData().getName())
                  .append(" created from descriptor ").append(aeFilePath).toString());
      } else {
        cachedAE.reconfigure();
        if (log.isDebugEnabled())
          log.debug(new StringBuilder("AE ").append(cachedAE.getAnalysisEngineMetaData().getName())
                  .append(" at path ").append(aeFilePath).append(" reconfigured ").toString());
      }
    } catch (Exception e) {
      cachedAE = null;
      throw new ResourceInitializationException(e);
    }
    return cachedAE;
  }

}
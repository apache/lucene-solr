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

import org.apache.uima.UIMAFramework;
import org.apache.uima.analysis_engine.AnalysisEngine;
import org.apache.uima.analysis_engine.AnalysisEngineDescription;
import org.apache.uima.resource.ResourceInitializationException;
import org.apache.uima.util.XMLInputSource;

/**
 * Basic {@link AEProvider} which just instantiates a UIMA {@link AnalysisEngine} with no additional metadata,
 * parameters or resources
 */
public class BasicAEProvider implements AEProvider {

  private final String aePath;
  private AnalysisEngine cachedAE;

  public BasicAEProvider(String aePath) {
    this.aePath = aePath;
  }

  @Override
  public synchronized AnalysisEngine getAE() throws ResourceInitializationException {
    try {
      if (cachedAE == null) {
        // get Resource Specifier from XML file

        XMLInputSource in;
        try {
          in = new XMLInputSource(aePath);
        } catch (Exception e) {
          in = new XMLInputSource(getClass().getResource(aePath));
        }

        // get AE description
        AnalysisEngineDescription desc = UIMAFramework.getXMLParser()
            .parseAnalysisEngineDescription(in);

        // create AE here
        cachedAE = UIMAFramework.produceAnalysisEngine(desc);
      } else {
        cachedAE.reconfigure();
      }
    } catch (Exception e) {
      cachedAE = null;
      throw new ResourceInitializationException(e);
    }
    return cachedAE;
  }
}

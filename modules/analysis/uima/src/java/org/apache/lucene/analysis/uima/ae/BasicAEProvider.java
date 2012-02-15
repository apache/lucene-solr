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

import java.io.IOException;

import org.apache.lucene.util.IOUtils;
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
  private AnalysisEngineDescription cachedDescription;

  public BasicAEProvider(String aePath) {
    this.aePath = aePath;
  }

  @Override
  public AnalysisEngine getAE() throws ResourceInitializationException {
    synchronized(this) {
      if (cachedDescription == null) {
        XMLInputSource in = null;
        boolean success = false;
        try {
          // get Resource Specifier from XML file
          in = getInputSource();

          // get AE description
          cachedDescription = UIMAFramework.getXMLParser()
              .parseAnalysisEngineDescription(in);
          configureDescription(cachedDescription);
          success = true;
        } catch (Exception e) {
            throw new ResourceInitializationException(e);
        } finally {
          if (success) {
            try {
              IOUtils.close(in.getInputStream());
            } catch (IOException e) {
              throw new ResourceInitializationException(e);
            }
          } else if (in != null) {
            IOUtils.closeWhileHandlingException(in.getInputStream());
          }
        }
      } 
    }

    return UIMAFramework.produceAnalysisEngine(cachedDescription);
  }
  
  protected void configureDescription(AnalysisEngineDescription description) {
    // no configuration
  }
  
  private XMLInputSource getInputSource() throws IOException {
    try {
      return new XMLInputSource(aePath);
    } catch (IOException e) {
      return new XMLInputSource(getClass().getResource(aePath));
    }
  }
}

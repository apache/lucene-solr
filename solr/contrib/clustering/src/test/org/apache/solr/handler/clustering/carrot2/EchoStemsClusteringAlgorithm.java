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
package org.apache.solr.handler.clustering.carrot2;

import java.util.ArrayList;
import java.util.List;

import org.carrot2.core.Cluster;
import org.carrot2.core.Document;
import org.carrot2.core.IClusteringAlgorithm;
import org.carrot2.core.LanguageCode;
import org.carrot2.core.ProcessingComponentBase;
import org.carrot2.core.ProcessingException;
import org.carrot2.core.attribute.AttributeNames;
import org.carrot2.core.attribute.Processing;
import org.carrot2.text.preprocessing.PreprocessingContext;
import org.carrot2.text.preprocessing.PreprocessingContext.AllStems;
import org.carrot2.text.preprocessing.PreprocessingContext.AllTokens;
import org.carrot2.text.preprocessing.PreprocessingContext.AllWords;
import org.carrot2.text.preprocessing.pipeline.BasicPreprocessingPipeline;
import org.carrot2.util.attribute.Attribute;
import org.carrot2.util.attribute.Bindable;
import org.carrot2.util.attribute.Input;
import org.carrot2.util.attribute.Output;

/**
 * A mock Carrot2 clustering algorithm that outputs stem of each token of each
 * document as a separate cluster. Useful only in tests.
 */
@Bindable(prefix = "EchoTokensClusteringAlgorithm")
public class EchoStemsClusteringAlgorithm extends ProcessingComponentBase
    implements IClusteringAlgorithm {
  @Input
  @Processing
  @Attribute(key = AttributeNames.DOCUMENTS)
  public List<Document> documents;
  
  @Output
  @Processing
  @Attribute(key = AttributeNames.CLUSTERS)
  public List<Cluster> clusters;

  public BasicPreprocessingPipeline preprocessing = new BasicPreprocessingPipeline();
  
  @Override
  public void process() throws ProcessingException {
    final PreprocessingContext preprocessingContext = preprocessing.preprocess(
        documents, "", LanguageCode.ENGLISH);
    final AllTokens allTokens = preprocessingContext.allTokens;
    final AllWords allWords = preprocessingContext.allWords;
    final AllStems allStems = preprocessingContext.allStems;
    clusters = new ArrayList<>();
    for (int i = 0; i < allTokens.image.length; i++) {
      if (allTokens.wordIndex[i] >= 0) {
        clusters.add(new Cluster(new String(
            allStems.image[allWords.stemIndex[allTokens.wordIndex[i]]])));
      }
    }
  }
}

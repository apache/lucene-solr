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
import org.carrot2.core.IClusteringAlgorithm;
import org.carrot2.core.LanguageCode;
import org.carrot2.core.ProcessingComponentBase;
import org.carrot2.core.ProcessingException;
import org.carrot2.core.attribute.AttributeNames;
import org.carrot2.core.attribute.Processing;
import org.carrot2.text.linguistic.ILexicalData;
import org.carrot2.text.preprocessing.pipeline.BasicPreprocessingPipeline;
import org.carrot2.text.util.MutableCharArray;
import org.carrot2.util.attribute.Attribute;
import org.carrot2.util.attribute.Bindable;
import org.carrot2.util.attribute.Input;
import org.carrot2.util.attribute.Output;

/**
 * A mock implementation of Carrot2 clustering algorithm for testing whether the
 * customized lexical resource lookup works correctly. This algorithm ignores
 * the input documents and instead for each word from {@link #wordsToCheck}, it
 * outputs a cluster labeled with the word only if the word is neither a stop
 * word nor a stop label.
 */
@Bindable(prefix = "LexicalResourcesCheckClusteringAlgorithm")
public class LexicalResourcesCheckClusteringAlgorithm extends
    ProcessingComponentBase implements IClusteringAlgorithm {

  @Output
  @Processing
  @Attribute(key = AttributeNames.CLUSTERS)
  public List<Cluster> clusters;

  @Input
  @Processing
  @Attribute
  public String wordsToCheck;

  public BasicPreprocessingPipeline preprocessing = new BasicPreprocessingPipeline();

  @Override
  public void process() throws ProcessingException {
    clusters = new ArrayList<>();
    if (wordsToCheck == null) {
      return;
    }

    // Test with Maltese so that the English clustering performed in other tests
    // is not affected by the test stopwords and stoplabels.
    ILexicalData lexicalData = preprocessing.lexicalDataFactory
        .getLexicalData(LanguageCode.MALTESE);

    for (String word : wordsToCheck.split(",")) {
      if (!lexicalData.isCommonWord(new MutableCharArray(word))
          && !lexicalData.isStopLabel(word)) {
        clusters.add(new Cluster(word));
      }
    }
  }
}

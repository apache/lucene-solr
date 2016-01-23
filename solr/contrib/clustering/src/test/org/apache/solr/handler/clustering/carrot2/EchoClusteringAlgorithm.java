package org.apache.solr.handler.clustering.carrot2;
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
import java.util.List;

import org.carrot2.core.Cluster;
import org.carrot2.core.Document;
import org.carrot2.core.IClusteringAlgorithm;
import org.carrot2.core.ProcessingComponentBase;
import org.carrot2.core.ProcessingException;
import org.carrot2.core.attribute.AttributeNames;
import org.carrot2.core.attribute.Processing;
import org.carrot2.util.attribute.Attribute;
import org.carrot2.util.attribute.Bindable;
import org.carrot2.util.attribute.Input;
import org.carrot2.util.attribute.Output;

import com.google.common.collect.Lists;

/**
 * A mock Carrot2 clustering algorithm that outputs input documents as clusters.
 * Useful only in tests.
 */
@Bindable(prefix = "EchoClusteringAlgorithm")
public class EchoClusteringAlgorithm extends ProcessingComponentBase implements
        IClusteringAlgorithm {
  @Input
  @Processing
  @Attribute(key = AttributeNames.DOCUMENTS)
  private List<Document> documents;

  @Output
  @Processing
  @Attribute(key = AttributeNames.CLUSTERS)
  private List<Cluster> clusters;

  @Input
  @Processing
  @Attribute(key = "custom-fields")
  private String customFields = "";

  
  @Override
  public void process() throws ProcessingException {
    clusters = Lists.newArrayListWithCapacity(documents.size());
    
    for (Document document : documents) {
      final Cluster cluster = new Cluster();
      cluster.addPhrases(document.getTitle(), document.getSummary());
      if (document.getLanguage() != null) {
        cluster.addPhrases(document.getLanguage().name());
      }
      for (String field : customFields.split(",")) {
        Object value = document.getField(field);
        if (value != null) {
          cluster.addPhrases(value.toString());
        }
      }
      cluster.addDocuments(document);
      clusters.add(cluster);
    }
  }
}

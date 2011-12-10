package org.apache.lucene.index.codecs;
/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
import java.io.Closeable;
import java.io.IOException;

import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.MergeState;

/**
 * Abstract API that consumes per document values. Concrete implementations of
 * this convert field values into a Codec specific format during indexing.
 * <p>
 * The {@link PerDocConsumer} API is accessible through the
 * {@link PostingsFormat} - API providing per field consumers and producers for inverted
 * data (terms, postings) as well as per-document data.
 * 
 * @lucene.experimental
 */
public abstract class PerDocConsumer implements Closeable{
  /** Adds a new DocValuesField */
  public abstract DocValuesConsumer addValuesField(DocValues.Type type, FieldInfo field)
      throws IOException;

  /**
   * Consumes and merges the given {@link PerDocProducer} producer
   * into this consumers format.   
   */
  public void merge(MergeState mergeState) throws IOException {
    final DocValues[] docValues = new DocValues[mergeState.readers.size()];

    for (FieldInfo fieldInfo : mergeState.fieldInfos) {
      mergeState.fieldInfo = fieldInfo; // set the field we are merging
      if (fieldInfo.hasDocValues()) {
        for (int i = 0; i < docValues.length; i++) {
          docValues[i] = mergeState.readers.get(i).reader.docValues(fieldInfo.name);
        }
        final DocValuesConsumer docValuesConsumer = addValuesField(fieldInfo.getDocValuesType(), fieldInfo);
        assert docValuesConsumer != null;
        docValuesConsumer.merge(mergeState, docValues);
      }
    }
  }  
}

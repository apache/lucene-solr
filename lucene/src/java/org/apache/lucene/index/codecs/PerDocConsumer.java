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
import org.apache.lucene.index.values.DocValues;

/**
 * Abstract API that consumes per document values. Concrete implementations of
 * this convert field values into a Codec specific format during indexing.
 * <p>
 * The {@link PerDocConsumer} API is accessible through flexible indexing / the
 * {@link Codec} - API providing per field consumers and producers for inverted
 * data (terms, postings) as well as per-document data.
 * 
 * @lucene.experimental
 */
public abstract class PerDocConsumer implements Closeable{
  /** Adds a new DocValuesField */
  public abstract DocValuesConsumer addValuesField(FieldInfo field)
      throws IOException;

  /**
   * Consumes and merges the given {@link PerDocValues} producer
   * into this consumers format.   
   */
  public void merge(MergeState mergeState, PerDocValues producer)
      throws IOException {
    Iterable<String> fields = producer.fields();
    for (String field : fields) {
      mergeState.fieldInfo = mergeState.fieldInfos.fieldInfo(field);
      assert mergeState.fieldInfo != null : "FieldInfo for field is null: "
          + field;
      if (mergeState.fieldInfo.hasDocValues()) {
        final DocValues docValues = producer.docValues(field);
        if (docValues == null) {
          /*
           * It is actually possible that a fieldInfo has a values type but no
           * values are actually available. this can happen if there are already
           * segments without values around.
           */
          continue;
        }
        final DocValuesConsumer docValuesConsumer = addValuesField(mergeState.fieldInfo);
        assert docValuesConsumer != null;
        docValuesConsumer.merge(mergeState, docValues);
      }
    }

  }
}

package org.apache.lucene.codecs;
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
import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.DocValues.Type;

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
public abstract class PerDocConsumer implements Closeable {
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
      if (canMerge(fieldInfo)) {
        for (int i = 0; i < docValues.length; i++) {
          docValues[i] = getDocValuesForMerge(mergeState.readers.get(i).reader, fieldInfo);
        }
        Type docValuesType = getDocValuesType(fieldInfo);
        assert docValuesType != null;
        
        final DocValuesConsumer docValuesConsumer = addValuesField(docValuesType, fieldInfo);
        assert docValuesConsumer != null;
        docValuesConsumer.merge(mergeState, docValues);
      }
    }
  }

  /**
   * Returns a {@link DocValues} instance for merging from the given reader for the given
   * {@link FieldInfo}. This method is used for merging and uses
   * {@link AtomicReader#docValues(String)} by default.
   * <p>
   * To enable {@link DocValues} merging for different {@link DocValues} than
   * the default override this method accordingly.
   * <p>
   */
  protected DocValues getDocValuesForMerge(AtomicReader reader, FieldInfo info) throws IOException {
    return reader.docValues(info.name);
  }
  
  /**
   * Returns <code>true</code> iff the given field can be merged ie. has {@link DocValues}.
   * By default this method uses {@link FieldInfo#hasDocValues()}.
   * <p>
   * To enable {@link DocValues} merging for different {@link DocValues} than
   * the default override this method accordingly.
   * <p>
   */
  protected boolean canMerge(FieldInfo info) {
    return info.hasDocValues();
  }
  
  /**
   * Returns the {@link DocValues} {@link Type} for the given {@link FieldInfo}.
   * By default this method uses {@link FieldInfo#getDocValuesType()}.
   * <p>
   * To enable {@link DocValues} merging for different {@link DocValues} than
   * the default override this method accordingly.
   * <p>
   */
  protected Type getDocValuesType(FieldInfo info) {
    return info.getDocValuesType();
  }
  
  /**
   * Called during indexing if the indexing session is aborted due to a unrecoverable exception.
   * This method should cleanup all resources.
   */
  public abstract void abort();
}

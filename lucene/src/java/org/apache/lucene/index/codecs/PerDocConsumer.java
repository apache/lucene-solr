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
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.values.IndexDocValues;
import org.apache.lucene.index.values.TypePromoter;
import org.apache.lucene.index.values.ValueType;

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
  public abstract DocValuesConsumer addValuesField(FieldInfo field)
      throws IOException;

  /**
   * Consumes and merges the given {@link PerDocValues} producer
   * into this consumers format.   
   */
  public void merge(MergeState mergeState)
      throws IOException {
    final FieldInfos fieldInfos = mergeState.fieldInfos;
    final IndexDocValues[] docValues = new IndexDocValues[mergeState.readers.size()];
    final PerDocValues[] perDocValues = new PerDocValues[mergeState.readers.size()];
    // pull all PerDocValues 
    for (int i = 0; i < perDocValues.length; i++) {
      perDocValues[i] =  mergeState.readers.get(i).reader.perDocValues();
    }
    for (FieldInfo fieldInfo : fieldInfos) {
      mergeState.fieldInfo = fieldInfo;
      TypePromoter currentPromoter = TypePromoter.getIdentityPromoter();
      if (fieldInfo.hasDocValues()) {
        for (int i = 0; i < perDocValues.length; i++) {
          if (perDocValues[i] != null) { // get all IDV to merge
            docValues[i] = perDocValues[i].docValues(fieldInfo.name);
            if (docValues[i] != null) {
              currentPromoter = promoteValueType(fieldInfo, docValues[i], currentPromoter);
              if (currentPromoter == null) {
                break;
              }     
            }
          }
        }
        
        if (currentPromoter == null) {
          fieldInfo.resetDocValues(null);
          continue;
        }
        assert currentPromoter != TypePromoter.getIdentityPromoter();
        if (fieldInfo.getDocValues() != currentPromoter.type()) {
          // reset the type if we got promoted
          fieldInfo.resetDocValues(currentPromoter.type());
        }
        
        final DocValuesConsumer docValuesConsumer = addValuesField(mergeState.fieldInfo);
        assert docValuesConsumer != null;
        docValuesConsumer.merge(mergeState, docValues);
      }
    }
    /* NOTE: don't close the perDocProducers here since they are private segment producers
     * and will be closed once the SegmentReader goes out of scope */ 
  }

  protected TypePromoter promoteValueType(final FieldInfo fieldInfo, final IndexDocValues docValues,
      TypePromoter currentPromoter) {
    assert currentPromoter != null;
    final TypePromoter incomingPromoter = TypePromoter.create(docValues.type(),  docValues.getValueSize());
    assert incomingPromoter != null;
    final TypePromoter newPromoter = currentPromoter.promote(incomingPromoter);
    return newPromoter == null ? handleIncompatibleValueType(fieldInfo, incomingPromoter, currentPromoter) : newPromoter;    
  }

  /**
   * Resolves a conflicts of incompatible {@link TypePromoter}s. The default
   * implementation promotes incompatible types to
   * {@link ValueType#BYTES_VAR_STRAIGHT} and preserves all values. If this
   * method returns <code>null</code> all docvalues for the given
   * {@link FieldInfo} are dropped and all values are lost.
   * 
   * @param incomingPromoter
   *          the incompatible incoming promoter
   * @param currentPromoter
   *          the current promoter
   * @return a promoted {@link TypePromoter} or <code>null</code> iff this index
   *         docvalues should be dropped for this field.
   */
  protected TypePromoter handleIncompatibleValueType(FieldInfo fieldInfo, TypePromoter incomingPromoter, TypePromoter currentPromoter) {
    return TypePromoter.create(ValueType.BYTES_VAR_STRAIGHT, TypePromoter.VAR_TYPE_VALUE_SIZE);
  }
  
}

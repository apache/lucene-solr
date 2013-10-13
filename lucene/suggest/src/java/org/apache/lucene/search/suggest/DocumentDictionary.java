package org.apache.lucene.search.suggest;

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
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.index.StorableField;
import org.apache.lucene.index.StoredDocument;
import org.apache.lucene.search.spell.Dictionary;
import org.apache.lucene.search.spell.TermFreqPayloadIterator;
import org.apache.lucene.search.suggest.analyzing.AnalyzingInfixSuggester; // javadoc
import org.apache.lucene.search.suggest.fst.FSTCompletionLookup; // javadoc
import org.apache.lucene.search.suggest.fst.WFSTCompletionLookup; // javadoc
import org.apache.lucene.search.suggest.jaspell.JaspellLookup; // javadoc
import org.apache.lucene.search.suggest.tst.TSTLookup; // javadoc
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefIterator;

/**
 * Dictionary with terms, weights and optionally payload information 
 * taken from stored fields in a Lucene index.
 * 
 * <b>NOTE: </b> 
 *  <ul>
 *    <li>
 *      The term, weight and (optionally) payload fields supplied
 *      are required for ALL documents and has to be stored
 *    </li>
 *  </ul>
 */
public class DocumentDictionary implements Dictionary {
  
  private final IndexReader reader;
  private final String field;
  private final String weightField;
  private final String payloadField;
  
  /**
   * Creates a new dictionary with the contents of the fields named <code>field</code>
   * for the terms and <code>weightField</code> for the weights that will be used for
   * the corresponding terms.
   */
  public DocumentDictionary(IndexReader reader, String field, String weightField) {
    this.reader = reader;
    this.field = field;
    this.weightField = weightField;
    this.payloadField = null;
  }
  
  /**
   * Creates a new dictionary with the contents of the fields named <code>field</code>
   * for the terms, <code>weightField</code> for the weights that will be used for the 
   * the corresponding terms and <code>payloadField</code> for the corresponding payloads
   * for the entry.
   */
  public DocumentDictionary(IndexReader reader, String field, String weightField, String payloadField) {
    this.reader = reader;
    this.field = field;
    this.weightField = weightField;
    this.payloadField = payloadField;
  }
  
  @Override
  public BytesRefIterator getWordsIterator() throws IOException {
    return new TermWeightPayloadIterator(payloadField!=null);
  }
    
  final class TermWeightPayloadIterator implements TermFreqPayloadIterator {
    private final int docCount;
    private final Set<String> relevantFields;
    private final boolean hasPayloads;
    private final Bits liveDocs;
    private int currentDocId = -1;
    private long currentWeight;
    private BytesRef currentPayload;
    
    /**
     * Creates an iterator over term, weight and payload fields from the lucene
     * index. setting <code>withPayload</code> to false, implies an iterator
     * over only term and weight.
     */
    public TermWeightPayloadIterator(boolean hasPayloads) throws IOException {
      docCount = reader.maxDoc() - 1;
      this.hasPayloads = hasPayloads;
      currentPayload = null;
      liveDocs = MultiFields.getLiveDocs(reader);
      List<String> relevantFieldList;
      if(hasPayloads) {
        relevantFieldList = Arrays.asList(field, weightField, payloadField);
      } else {
        relevantFieldList = Arrays.asList(field, weightField);
      }
      this.relevantFields = new HashSet<>(relevantFieldList);
    }

    @Override
    public long weight() {
      return currentWeight;
    }

    @Override
    public BytesRef next() throws IOException {
      while (currentDocId < docCount) {
        currentDocId++;
        if (liveDocs != null && !liveDocs.get(currentDocId)) { 
          continue;
        }

        StoredDocument doc = reader.document(currentDocId, relevantFields);
        
        if (hasPayloads) {
          StorableField payload = doc.getField(payloadField);
          if (payload == null) {
            throw new IllegalArgumentException(payloadField + " does not exist");
          } else if (payload.binaryValue() == null) {
            throw new IllegalArgumentException(payloadField + " does not have binary value");
          }
          currentPayload = payload.binaryValue();
        }
        
        StorableField weight = doc.getField(weightField);
        if (weight == null) {
          throw new IllegalArgumentException(weightField + " does not exist");
        } else if (weight.numericValue() == null) {
          throw new IllegalArgumentException(weightField + " does not have numeric value");
        }
        currentWeight = weight.numericValue().longValue();
        
        StorableField fieldVal = doc.getField(field);
        if (fieldVal == null) {
          throw new IllegalArgumentException(field + " does not exist");
        } else if(fieldVal.stringValue() == null) {
          throw new IllegalArgumentException(field + " does not have string value");
        }
        
        return new BytesRef(fieldVal.stringValue());
      }
      return null;
    }

    @Override
    public BytesRef payload() {
      return currentPayload;
    }

    @Override
    public boolean hasPayloads() {
      return hasPayloads;
    }
    
  }
}

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
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.search.spell.Dictionary;
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
  
  /** {@link IndexReader} to load documents from */
  protected final IndexReader reader;

  /** Field to read payload from */
  protected final String payloadField;
  private final String field;
  private final String weightField;
  
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
    return new DocumentInputIterator(payloadField!=null);
  }

  /** Implements {@link InputIterator} from stored fields. */
  protected class DocumentInputIterator implements InputIterator {
    private final int docCount;
    private final Set<String> relevantFields;
    private final boolean hasPayloads;
    private final Bits liveDocs;
    private int currentDocId = -1;
    private long currentWeight;
    private BytesRef currentPayload;
    private Document doc;
    
    /**
     * Creates an iterator over term, weight and payload fields from the lucene
     * index. setting <code>withPayload</code> to false, implies an iterator
     * over only term and weight.
     */
    public DocumentInputIterator(boolean hasPayloads) throws IOException {
      docCount = reader.maxDoc() - 1;
      this.hasPayloads = hasPayloads;
      currentPayload = null;
      liveDocs = MultiFields.getLiveDocs(reader);
      this.relevantFields = getRelevantFields(new String [] {field, weightField, payloadField});
    }

    @Override
    public long weight() {
      return currentWeight;
    }

    @Override
    public Comparator<BytesRef> getComparator() {
      return null;
    }

    @Override
    public BytesRef next() throws IOException {
      while (currentDocId < docCount) {
        currentDocId++;
        if (liveDocs != null && !liveDocs.get(currentDocId)) { 
          continue;
        }

        doc = reader.document(currentDocId, relevantFields);
        
        if (hasPayloads) {
          IndexableField payload = doc.getField(payloadField);
          if (payload == null) {
            throw new IllegalArgumentException(payloadField + " does not exist");
          } else if (payload.binaryValue() == null) {
            throw new IllegalArgumentException(payloadField + " does not have binary value");
          }
          currentPayload = payload.binaryValue();
        }
        
        currentWeight = getWeight(currentDocId);
        
        IndexableField fieldVal = doc.getField(field);
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

    /** Return the suggestion weight for this document */
    protected long getWeight(int docId) {
      IndexableField weight = doc.getField(weightField);
      if (weight == null) {
        throw new IllegalArgumentException(weightField + " does not exist");
      } else if (weight.numericValue() == null) {
        throw new IllegalArgumentException(weightField + " does not have numeric value");
      }
      return weight.numericValue().longValue();
    }
    
    private Set<String> getRelevantFields(String... fields) {
      Set<String> relevantFields = new HashSet<String>();
      for (String relevantField : fields) {
        if (relevantField != null) {
          relevantFields.add(relevantField);
        }
      }
      return relevantFields;
    }
  }
}

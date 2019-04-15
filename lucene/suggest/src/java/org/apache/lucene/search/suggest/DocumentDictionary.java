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
package org.apache.lucene.search.suggest;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.MultiBits;
import org.apache.lucene.index.MultiDocValues;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.search.spell.Dictionary;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;



/**
 * <p>
 * Dictionary with terms, weights, payload (optional) and contexts (optional)
 * information taken from stored/indexed fields in a Lucene index.
 * </p>
 * <b>NOTE:</b> 
 *  <ul>
 *    <li>
 *      The term field has to be stored; if it is missing, the document is skipped.
 *    </li>
 *    <li>
 *      The payload and contexts field are optional and are not required to be stored.
 *    </li>
 *    <li>
 *      The weight field can be stored or can be a {@link NumericDocValues}.
 *      If the weight field is not defined, the value of the weight is <code>0</code>
 *    </li>
 *  </ul>
 */
public class DocumentDictionary implements Dictionary {
  
  /** {@link IndexReader} to load documents from */
  protected final IndexReader reader;

  /** Field to read payload from */
  protected final String payloadField;
  /** Field to read contexts from */
  protected final String contextsField;
  private final String field;
  private final String weightField;
  
  /**
   * Creates a new dictionary with the contents of the fields named <code>field</code>
   * for the terms and <code>weightField</code> for the weights that will be used for
   * the corresponding terms.
   */
  public DocumentDictionary(IndexReader reader, String field, String weightField) {
    this(reader, field, weightField, null);
  }
  
  /**
   * Creates a new dictionary with the contents of the fields named <code>field</code>
   * for the terms, <code>weightField</code> for the weights that will be used for the 
   * the corresponding terms and <code>payloadField</code> for the corresponding payloads
   * for the entry.
   */
  public DocumentDictionary(IndexReader reader, String field, String weightField, String payloadField) {
    this(reader, field, weightField, payloadField, null);
  }

  /**
   * Creates a new dictionary with the contents of the fields named <code>field</code>
   * for the terms, <code>weightField</code> for the weights that will be used for the 
   * the corresponding terms, <code>payloadField</code> for the corresponding payloads
   * for the entry and <code>contextsField</code> for associated contexts.
   */
  public DocumentDictionary(IndexReader reader, String field, String weightField, String payloadField, String contextsField) {
    this.reader = reader;
    this.field = field;
    this.weightField = weightField;
    this.payloadField = payloadField;
    this.contextsField = contextsField;
  }

  @Override
  public InputIterator getEntryIterator() throws IOException {
    return new DocumentInputIterator(payloadField!=null, contextsField!=null);
  }

  /** Implements {@link InputIterator} from stored fields. */
  protected class DocumentInputIterator implements InputIterator {

    private final int docCount;
    private final Set<String> relevantFields;
    private final boolean hasPayloads;
    private final boolean hasContexts;
    private final Bits liveDocs;
    private int currentDocId = -1;
    private long currentWeight = 0;
    private BytesRef currentPayload = null;
    private Set<BytesRef> currentContexts;
    private final NumericDocValues weightValues;
    IndexableField[] currentDocFields = new IndexableField[0];
    int nextFieldsPosition = 0;

    /**
     * Creates an iterator over term, weight and payload fields from the lucene
     * index. setting <code>withPayload</code> to false, implies an iterator
     * over only term and weight.
     */
    public DocumentInputIterator(boolean hasPayloads, boolean hasContexts) throws IOException {
      this.hasPayloads = hasPayloads;
      this.hasContexts = hasContexts;
      docCount = reader.maxDoc() - 1;
      weightValues = (weightField != null) ? MultiDocValues.getNumericValues(reader, weightField) : null;
      liveDocs = (reader.leaves().size() > 0) ? MultiBits.getLiveDocs(reader) : null;
      relevantFields = getRelevantFields(new String [] {field, weightField, payloadField, contextsField});
    }

    @Override
    public long weight() {
      return currentWeight;
    }

    @Override
    public BytesRef next() throws IOException {
      while (true) {
        if (nextFieldsPosition < currentDocFields.length) {
          // Still values left from the document
          IndexableField fieldValue = currentDocFields[nextFieldsPosition++];
          if (fieldValue.binaryValue() != null) {
            return fieldValue.binaryValue();
          } else if (fieldValue.stringValue() != null) {
            return new BytesRef(fieldValue.stringValue());
          } else {
            continue;
          }
        }

        if (currentDocId == docCount) {
          // Iterated over all the documents.
          break;
        }

        currentDocId++;
        if (liveDocs != null && !liveDocs.get(currentDocId)) { 
          continue;
        }

        Document doc = reader.document(currentDocId, relevantFields);

        BytesRef tempPayload = null;
        if (hasPayloads) {
          IndexableField payload = doc.getField(payloadField);
          if (payload != null) {
            if (payload.binaryValue() != null) {
              tempPayload =  payload.binaryValue();
            } else if (payload.stringValue() != null) {
              tempPayload = new BytesRef(payload.stringValue());
            }
          }
          // in case that the iterator has payloads configured, use empty values
          // instead of null for payload
          if (tempPayload == null) {
            tempPayload = new BytesRef();
          }
        }

        Set<BytesRef> tempContexts;
        if (hasContexts) {
          tempContexts = new HashSet<>();
          final IndexableField[] contextFields = doc.getFields(contextsField);
          for (IndexableField contextField : contextFields) {
            if (contextField.binaryValue() != null) {
              tempContexts.add(contextField.binaryValue());
            } else if (contextField.stringValue() != null) {
              tempContexts.add(new BytesRef(contextField.stringValue()));
            } else {
              continue;
            }
          }
        } else {
          tempContexts = Collections.emptySet();
        }

        currentDocFields = doc.getFields(field);
        nextFieldsPosition = 0;
        if (currentDocFields.length == 0) { // no values in this document
          continue;
        }
        IndexableField fieldValue = currentDocFields[nextFieldsPosition++];
        BytesRef tempTerm;
        if (fieldValue.binaryValue() != null) {
          tempTerm = fieldValue.binaryValue();
        } else if (fieldValue.stringValue() != null) {
          tempTerm = new BytesRef(fieldValue.stringValue());
        } else {
          continue;
        }

        currentPayload = tempPayload;
        currentContexts = tempContexts;
        currentWeight = getWeight(doc, currentDocId);

        return tempTerm;
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
    
    /** 
     * Returns the value of the <code>weightField</code> for the current document.
     * Retrieves the value for the <code>weightField</code> if it's stored (using <code>doc</code>)
     * or if it's indexed as {@link NumericDocValues} (using <code>docId</code>) for the document.
     * If no value is found, then the weight is 0.
     */
    protected long getWeight(Document doc, int docId) throws IOException {
      IndexableField weight = doc.getField(weightField);
      if (weight != null) { // found weight as stored
        return (weight.numericValue() != null) ? weight.numericValue().longValue() : 0;
      } else if (weightValues != null) {  // found weight as NumericDocValue
        if (weightValues.docID() < docId) {
          weightValues.advance(docId);
        }
        if (weightValues.docID() == docId) {
          return weightValues.longValue();
        } else {
          // missing
          return 0;
        }
      } else { // fall back
        return 0;
      }
    }
    
    private Set<String> getRelevantFields(String... fields) {
      Set<String> relevantFields = new HashSet<>();
      for (String relevantField : fields) {
        if (relevantField != null) {
          relevantFields.add(relevantField);
        }
      }
      return relevantFields;
    }

    @Override
    public Set<BytesRef> contexts() {
      if (hasContexts) {
        return currentContexts;
      }
      return null;
    }

    @Override
    public boolean hasContexts() {
      return hasContexts;
    }
  }
}

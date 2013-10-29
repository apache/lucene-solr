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
import java.text.ParseException;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

import org.apache.lucene.document.NumericDocValuesField; // javadocs
import org.apache.lucene.expressions.Expression;
import org.apache.lucene.expressions.SimpleBindings;
import org.apache.lucene.expressions.js.JavascriptCompiler;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.search.SortField;
import org.apache.lucene.util.BytesRefIterator;


/**
 * Dictionary with terms and optionally payload information 
 * taken from stored fields in a Lucene index. Similar to 
 * {@link DocumentDictionary}, except it computes the weight
 * of the terms in a document based on a user-defined expression
 * having one or more {@link NumericDocValuesField} in the document.
 * 
 * <b>NOTE:</b> 
 *  <ul>
 *    <li>
 *      The term and (optionally) payload fields supplied
 *      are required for ALL documents and has to be stored
 *    </li>
 *  </ul>
 */
public class DocumentExpressionDictionary extends DocumentDictionary {
  
  private ValueSource weightsValueSource;
  
  /**
   * Creates a new dictionary with the contents of the fields named <code>field</code>
   * for the terms and computes the corresponding weights of the term by compiling the
   * user-defined <code>weightExpression</code> using the <code>sortFields</code>
   * bindings.
   */
  public DocumentExpressionDictionary(IndexReader reader, String field,
      String weightExpression, Set<SortField> sortFields) {
    this(reader, field, weightExpression, sortFields, null);
  }
  
  /**
   * Creates a new dictionary with the contents of the fields named <code>field</code>
   * for the terms, <code>payloadField</code> for the corresponding payloads
   * and computes the corresponding weights of the term by compiling the
   * user-defined <code>weightExpression</code> using the <code>sortFields</code>
   * bindings.
   */
  public DocumentExpressionDictionary(IndexReader reader, String field,
      String weightExpression, Set<SortField> sortFields, String payload) {
    super(reader, field, null, payload);
    Expression expression = null;
    try {
      expression = JavascriptCompiler.compile(weightExpression);
    } catch (ParseException e) {
      throw new RuntimeException();
    }
    SimpleBindings bindings = new SimpleBindings();
    for (SortField sortField: sortFields) {
      bindings.add(sortField);
    }
    weightsValueSource = expression.getValueSource(bindings);
    
  }
  
  @Override
  public BytesRefIterator getWordsIterator() throws IOException {
    return new DocumentExpressionInputIterator(payloadField!=null);
  }
  
  final class DocumentExpressionInputIterator extends DocumentDictionary.DocumentInputIterator {
    
    private FunctionValues currentWeightValues;
    private int currentLeafIndex = 0;
    private final List<AtomicReaderContext> leaves;
    
    private final int[] starts;
    
    public DocumentExpressionInputIterator(boolean hasPayloads)
        throws IOException {
      super(hasPayloads);
      leaves = reader.leaves();
      if (leaves.size() == 0) {
        throw new IllegalArgumentException("Reader has to have at least one leaf");
      }
      starts = new int[leaves.size() + 1];
      for (int i = 0; i < leaves.size(); i++) {
        starts[i] = leaves.get(i).docBase;
      }
      starts[leaves.size()] = reader.maxDoc();
      
      currentLeafIndex = 0;
      currentWeightValues = weightsValueSource.getValues(new HashMap<String, Object>(), leaves.get(currentLeafIndex));
    }
    
    @Override
    protected long getWeight(int docId) {
      int subIndex = ReaderUtil.subIndex(docId, starts);
      if (subIndex != currentLeafIndex) {
        currentLeafIndex = subIndex;
        try {
          currentWeightValues = weightsValueSource.getValues(new HashMap<String, Object>(), leaves.get(currentLeafIndex));
        } catch (IOException e) {
          throw new RuntimeException();
        }
      }
      return currentWeightValues.longVal(docId - starts[subIndex]);
    }

  }
}

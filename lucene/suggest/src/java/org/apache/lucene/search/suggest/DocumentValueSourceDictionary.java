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
import java.util.HashMap;
import java.util.List;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.ValueSource;


/**
 * <p>
 * Dictionary with terms and optionally payload and
 * optionally contexts information
 * taken from stored fields in a Lucene index. Similar to 
 * {@link DocumentDictionary}, except it obtains the weight
 * of the terms in a document based on a {@link ValueSource}.
 * </p>
 * <b>NOTE:</b> 
 *  <ul>
 *    <li>
 *      The term field has to be stored; if it is missing, the document is skipped.
 *    </li>
 *    <li>
 *      The payload and contexts field are optional and are not required to be stored.
 *    </li>
 *  </ul>
 *  <p>
 *  In practice the {@link ValueSource} will likely be obtained
 *  using the lucene expression module. The following example shows
 *  how to create a {@link ValueSource} from a simple addition of two
 *  fields:
 *  <code>
 *    Expression expression = JavascriptCompiler.compile("f1 + f2");
 *    SimpleBindings bindings = new SimpleBindings();
 *    bindings.add(new SortField("f1", SortField.Type.LONG));
 *    bindings.add(new SortField("f2", SortField.Type.LONG));
 *    ValueSource valueSource = expression.getValueSource(bindings);
 *  </code>
 *  </p>
 *
 */
public class DocumentValueSourceDictionary extends DocumentDictionary {
  
  private final ValueSource weightsValueSource;
  
  /**
   * Creates a new dictionary with the contents of the fields named <code>field</code>
   * for the terms, <code>payload</code> for the corresponding payloads, <code>contexts</code>
   * for the associated contexts and uses the <code>weightsValueSource</code> supplied 
   * to determine the score.
   */
  public DocumentValueSourceDictionary(IndexReader reader, String field,
                                       ValueSource weightsValueSource, String payload, String contexts) {
    super(reader, field, null, payload, contexts);
    this.weightsValueSource = weightsValueSource;
  }
  /**
   * Creates a new dictionary with the contents of the fields named <code>field</code>
   * for the terms, <code>payloadField</code> for the corresponding payloads
   * and uses the <code>weightsValueSource</code> supplied to determine the 
   * score.
   */
  public DocumentValueSourceDictionary(IndexReader reader, String field,
                                       ValueSource weightsValueSource, String payload) {
    super(reader, field, null, payload);
    this.weightsValueSource = weightsValueSource;
  }
  
  /** 
   * Creates a new dictionary with the contents of the fields named <code>field</code>
   * for the terms and uses the <code>weightsValueSource</code> supplied to determine the 
   * score.
   */
  public DocumentValueSourceDictionary(IndexReader reader, String field,
                                       ValueSource weightsValueSource) {
    super(reader, field, null, null);
    this.weightsValueSource = weightsValueSource;  
  }
  
  @Override
  public InputIterator getEntryIterator() throws IOException {
    return new DocumentValueSourceInputIterator(payloadField!=null, contextsField!=null);
  }
  
  final class DocumentValueSourceInputIterator extends DocumentDictionary.DocumentInputIterator {
    
    private FunctionValues currentWeightValues;
    /** leaves of the reader */
    private final List<LeafReaderContext> leaves;
    /** starting docIds of all the leaves */
    private final int[] starts;
    /** current leave index */
    private int currentLeafIndex = 0;

    public DocumentValueSourceInputIterator(boolean hasPayloads, boolean hasContexts)
        throws IOException {
      super(hasPayloads, hasContexts);
      leaves = reader.leaves();
      starts = new int[leaves.size() + 1];
      for (int i = 0; i < leaves.size(); i++) {
        starts[i] = leaves.get(i).docBase;
      }
      starts[leaves.size()] = reader.maxDoc();
      currentWeightValues = (leaves.size() > 0) 
          ? weightsValueSource.getValues(new HashMap<String, Object>(), leaves.get(currentLeafIndex))
          : null;
    }
    
    /** 
     * Returns the weight for the current <code>docId</code> as computed 
     * by the <code>weightsValueSource</code>
     * */
    @Override
    protected long getWeight(Document doc, int docId) {    
      if (currentWeightValues == null) {
        return 0;
      }
      int subIndex = ReaderUtil.subIndex(docId, starts);
      if (subIndex != currentLeafIndex) {
        currentLeafIndex = subIndex;
        try {
          currentWeightValues = weightsValueSource.getValues(new HashMap<String, Object>(), leaves.get(currentLeafIndex));
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
      return currentWeightValues.longVal(docId - starts[subIndex]);
    }

  }
}

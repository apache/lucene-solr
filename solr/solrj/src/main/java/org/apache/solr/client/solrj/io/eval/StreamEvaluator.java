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
package org.apache.solr.client.solrj.io.eval;

import java.io.IOException;
import java.io.Serializable;

import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.stream.StreamContext;
import org.apache.solr.client.solrj.io.stream.expr.Expressible;

public interface StreamEvaluator extends Expressible, Serializable {
  void setStreamContext(StreamContext streamContext);
  StreamContext getStreamContext();
  
  Object evaluate(final Tuple tuple) throws IOException;
  
  /**
   * Execute the evaluator over lets stored within the StreamContext. This allows 
   * evaluators to be executed over values calculated elsewhere in the pipeline
   * and stored in the {@link StreamContext#getLets() streamContext.lets}
   * 
   * Default implementation just creates a tuple out of all values in the context 
   * and passes that to {@link StreamEvaluator#evaluate(Tuple)}.
   * 
   * @return Evaluated value
   * @throws IOException throw on error during evaluation
   */
  default Object evaluateOverContext() throws IOException{
    StreamContext context = getStreamContext();
    if(null != context){
      Tuple contextTuple = new Tuple(context.getLets());
      return evaluate(contextTuple);
    }
    
    return null;
  }
  
}
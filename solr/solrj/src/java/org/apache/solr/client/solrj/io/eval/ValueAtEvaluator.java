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
import java.util.List;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;

public class ValueAtEvaluator extends RecursiveObjectEvaluator implements ManyValueWorker {
  protected static final long serialVersionUID = 1L;

  public ValueAtEvaluator(StreamExpression expression, StreamFactory factory) throws IOException {
    super(expression, factory);
  }

  @Override
  public Object doWork(Object... values) throws IOException {
    if(values[0] instanceof List) {

      @SuppressWarnings({"unchecked"})
      List<Number> c = (List<Number>) values[0];
      int index = -1;
      if(values.length == 2) {
        index = ((Number)values[1]).intValue();
        if(index >= c.size()) {
          throw new IOException("Index out of bounds: "+index);
        }
      } else {
        throw new IOException("The valueAt function expects an array and array index as parameters.");
      }
      return c.get(index);

    } else if(values[0] instanceof Matrix) {

      Matrix c = (Matrix) values[0];
      double[][] data = c.getData();
      int row = -1;
      int col = -1;
      if(values.length == 3) {
        row = ((Number)values[1]).intValue();
        if(row >= data.length) {
          throw new IOException("Row index out of bounds: "+row);
        }

        col = ((Number)values[2]).intValue();
        if(col >= data[0].length) {
          throw new IOException("Column index out of bounds: "+col);
        }

      } else {
        throw new IOException("The valueAt function expects a matrix and row and column indexes");
      }
      return data[row][col];
    } else {
      throw new IOException("The valueAt function expects a numeric array or matrix as the first parameter");
    }

  }
}

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
package org.apache.solr.search.function;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.queries.function.FunctionValues;
import org.apache.solr.search.CollapsingQParserPlugin.CollapseScore;
import java.util.Map;
import java.io.IOException;

public class CollapseScoreFunction extends ValueSource {

  public String description() {
    return "CollapseScoreFunction";
  }

  public boolean equals(Object o) {
    if(o instanceof CollapseScoreFunction){
      return true;
    } else {
      return false;
    }
  }

  public int hashCode() {
    return 1213241257;
  }

  @SuppressWarnings({"rawtypes"})
  public FunctionValues getValues(Map context, LeafReaderContext readerContext) throws IOException {
    return new CollapseScoreFunctionValues(context);
  }

  public static class CollapseScoreFunctionValues extends FunctionValues {

    private CollapseScore cscore;

    @SuppressWarnings({"rawtypes"})
    public CollapseScoreFunctionValues(Map context) {
      this.cscore = (CollapseScore) context.get("CSCORE");
      assert null != this.cscore;
    }

    public int intVal(int doc) {
      return 0;
    }

    public String toString(int doc) {
      return Float.toString(cscore.score);
    }

    public float floatVal(int doc) {
      return cscore.score;
    }

    public double doubleVal(int doc) {
      return 0.0D;
    }
  }
}

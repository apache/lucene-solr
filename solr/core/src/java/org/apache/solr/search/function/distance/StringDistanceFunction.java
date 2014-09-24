package org.apache.solr.search.function.distance;

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

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.queries.function.docvalues.FloatDocValues;
import org.apache.lucene.search.spell.StringDistance;

import java.io.IOException;
import java.util.Map;


/**
 *
 *
 **/
public class StringDistanceFunction extends ValueSource {
  protected ValueSource str1, str2;
  protected StringDistance dist;

  public StringDistanceFunction(ValueSource str1, ValueSource str2, StringDistance measure) {
    this.str1 = str1;
    this.str2 = str2;
    dist = measure;


  }

  @Override
  public FunctionValues getValues(Map context, LeafReaderContext readerContext) throws IOException {
    final FunctionValues str1DV = str1.getValues(context, readerContext);
    final FunctionValues str2DV = str2.getValues(context, readerContext);
    return new FloatDocValues(this) {

      @Override
      public float floatVal(int doc) {
        return dist.getDistance(str1DV.strVal(doc), str2DV.strVal(doc));
      }

      @Override
      public String toString(int doc) {
        StringBuilder sb = new StringBuilder();
        sb.append("strdist").append('(');
        sb.append(str1DV.toString(doc)).append(',').append(str2DV.toString(doc))
                .append(", dist=").append(dist.getClass().getName());
        sb.append(')');
        return sb.toString();
      }
    };
  }

  @Override
  public String description() {
    StringBuilder sb = new StringBuilder();
    sb.append("strdist").append('(');
    sb.append(str1).append(',').append(str2).append(", dist=").append(dist.getClass().getName());
    sb.append(')');
    return sb.toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof StringDistanceFunction)) return false;

    StringDistanceFunction that = (StringDistanceFunction) o;

    if (!dist.equals(that.dist)) return false;
    if (!str1.equals(that.str1)) return false;
    if (!str2.equals(that.str2)) return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = str1.hashCode();
    result = 31 * result + str2.hashCode();
    result = 31 * result + dist.hashCode();
    return result;
  }
}

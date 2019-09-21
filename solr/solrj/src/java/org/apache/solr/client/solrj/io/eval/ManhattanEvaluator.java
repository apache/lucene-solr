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

import org.apache.commons.math3.ml.distance.ManhattanDistance;
import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;

public class ManhattanEvaluator extends RecursiveEvaluator {
  protected static final long serialVersionUID = 1L;

  public ManhattanEvaluator(StreamExpression expression, StreamFactory factory) throws IOException{
    super(expression, factory);
  }

  public ManhattanEvaluator(StreamExpression expression, StreamFactory factory, List<String> ignoredNamedParameters) throws IOException{
    super(expression, factory, ignoredNamedParameters);
  }

  @Override
  public Object evaluate(Tuple tuple) throws IOException {
    return new ManhattanDistance();
  }

  @Override
  public Object doWork(Object... values) throws IOException {
    // Nothing to do here
    throw new IOException("This call should never occur");
  }
}
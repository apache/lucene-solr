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
package org.apache.solr.client.solrj.io.stream.expr;

import java.io.IOException;

/**
 * Defines a stream that can be expressed in an expression
 */
public interface Expressible {
//  public String getFunctionName();
//  public void setFunctionName(String functionName);
  StreamExpressionParameter toExpression(StreamFactory factory) throws IOException;
  
  /**
   * Returns an explanation about the stream object
   * @param factory Stream factory for this, contains information about the function name
   * @return Explanation about this stream object containing explanations of any child stream objects
   * @throws IOException throw on any error
   */
  Explanation toExplanation(StreamFactory factory) throws IOException;
  
}

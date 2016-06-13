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
package org.apache.solr.handler.dataimport;

import static org.apache.solr.handler.dataimport.DataImportHandlerException.SEVERE;
import static org.apache.solr.handler.dataimport.DataImportHandlerException.wrapAndThrow;

import java.net.URLEncoder;
import java.util.List;

/**
 * <p>Escapes reserved characters in Solr queries</p>
 *
 * @see org.apache.solr.client.solrj.util.ClientUtils#escapeQueryChars(String)
 */
public class UrlEvaluator extends Evaluator {
  @Override
  public String evaluate(String expression, Context context) {
    List<Object> l = parseParams(expression, context.getVariableResolver());
    if (l.size() != 1) {
      throw new DataImportHandlerException(SEVERE, "'encodeUrl' must have at least one parameter ");
    }
    String s = l.get(0).toString();

    try {
      return URLEncoder.encode(s.toString(), "UTF-8");
    } catch (Exception e) {
      wrapAndThrow(SEVERE, e, "Unable to encode expression: " + expression + " with value: " + s);
      return null;
    }
  }
}

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
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;

public class MatchesEvaluator extends RecursiveBooleanEvaluator implements ManyValueWorker {
  protected static final long serialVersionUID = 1L;
  private Pattern pattern;

  public MatchesEvaluator(StreamExpression expression, StreamFactory factory) throws IOException{
    super(expression, factory);

    if(containedEvaluators.size() != 2){
      throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - expecting two parameters but found %d",expression,containedEvaluators.size()));
    }
  }

  public Object doWork(Object[] values) throws IOException {
    if(values[1] instanceof String) {
      String s = values[0].toString();
      if(pattern == null) {
        String p = (String) values[1];
        pattern = Pattern.compile(p.replace("\"", ""));
      }

      if(s.startsWith("\"")) {
        s = s.replace("\"", "");
      }

      Matcher matcher = pattern.matcher(s);
      return matcher.find();
    } else {
      throw new IOException("The matches function requires a String regex");
    }
  }

  protected Checker constructChecker(Object value) throws IOException {
    return null;
  }
}

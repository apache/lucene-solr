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
package org.apache.solr.analysis;

import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.util.TokenFilterFactory;

import java.io.IOException;
import java.util.Map;

/**
 * Token filter factory that misbehaves on command.
 */
public class ThrowingMockTokenFilterFactory extends TokenFilterFactory {

  private Class<? extends RuntimeException> exceptionClass;

  /**
   * Initialize this factory via a set of key-value pairs.
   *
   * @param args the options.
   */
  @SuppressWarnings("unchecked")
  public ThrowingMockTokenFilterFactory(Map<String, String> args) {
    super(args);
    String exceptionClassName = args.get("exceptionClassName");
    if (exceptionClassName == null) {
      throw new RuntimeException("Required parameter exceptionClassName is missing");
    }
    try {
      exceptionClass = (Class<? extends RuntimeException>)Class.forName(exceptionClassName);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }

  }

  @Override
  public TokenStream create(TokenStream input) {
    return new TokenFilter(input) {
      @Override
      public boolean incrementToken() throws IOException {
        if (input.incrementToken()) {
          try {
            throw exceptionClass.newInstance();
          } catch (IllegalAccessException | InstantiationException iae) {
            throw new RuntimeException(iae);
          }
        }
        return false;
      }
    };
  }
}

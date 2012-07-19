package org.apache.solr.handler.clustering.carrot2;

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

import java.io.IOException;
import java.io.Reader;

import org.carrot2.core.LanguageCode;
import org.carrot2.text.analysis.ExtendedWhitespaceTokenizer;
import org.carrot2.text.analysis.ITokenizer;
import org.carrot2.text.linguistic.ITokenizerFactory;
import org.carrot2.text.util.MutableCharArray;

public class DuplicatingTokenizerFactory implements ITokenizerFactory {
  @Override
  public ITokenizer getTokenizer(LanguageCode language) {
    return new ITokenizer() {
      private final ExtendedWhitespaceTokenizer delegate = new ExtendedWhitespaceTokenizer();
      
      @Override
      public void setTermBuffer(MutableCharArray buffer) {
        delegate.setTermBuffer(buffer);
        buffer.reset(buffer.toString() + buffer.toString());
      }
      
      @Override
      public void reset(Reader input) {
        delegate.reset(input);
      }
      
      @Override
      public short nextToken() throws IOException {
        return delegate.nextToken();
      }
    };
  }
}

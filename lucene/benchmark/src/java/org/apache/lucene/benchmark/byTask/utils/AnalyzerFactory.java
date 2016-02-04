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
package org.apache.lucene.benchmark.byTask.utils;


import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.util.CharFilterFactory;
import org.apache.lucene.analysis.util.TokenFilterFactory;
import org.apache.lucene.analysis.util.TokenizerFactory;

import java.io.Reader;
import java.util.List;

/**
 * A factory to create an analyzer.
 * See {@link org.apache.lucene.benchmark.byTask.tasks.AnalyzerFactoryTask}
 */
public final class AnalyzerFactory {
  final private List<CharFilterFactory> charFilterFactories;
  final private TokenizerFactory tokenizerFactory;
  final private List<TokenFilterFactory> tokenFilterFactories;
  private String name = null;
  private Integer positionIncrementGap = null;
  private Integer offsetGap = null;

  public AnalyzerFactory(List<CharFilterFactory> charFilterFactories,
                         TokenizerFactory tokenizerFactory,
                         List<TokenFilterFactory> tokenFilterFactories) {
    this.charFilterFactories = charFilterFactories;
    assert null != tokenizerFactory;
    this.tokenizerFactory = tokenizerFactory;
    this.tokenFilterFactories = tokenFilterFactories;
  }

  public void setName(String name) {
    this.name = name;
  }

  public void setPositionIncrementGap(Integer positionIncrementGap) {
    this.positionIncrementGap = positionIncrementGap;
  }

  public void setOffsetGap(Integer offsetGap) {
    this.offsetGap = offsetGap;
  }

  public Analyzer create() {
    return new Analyzer() {
      private final Integer positionIncrementGap = AnalyzerFactory.this.positionIncrementGap;
      private final Integer offsetGap = AnalyzerFactory.this.offsetGap;

      @Override
      public Reader initReader(String fieldName, Reader reader) {
        if (charFilterFactories != null && charFilterFactories.size() > 0) {
          Reader wrappedReader = reader;
          for (CharFilterFactory charFilterFactory : charFilterFactories) {
            wrappedReader = charFilterFactory.create(wrappedReader);
          }
          reader = wrappedReader;
        }
        return reader;
      }

      @Override
      protected Analyzer.TokenStreamComponents createComponents(String fieldName) {
        final Tokenizer tokenizer = tokenizerFactory.create();
        TokenStream tokenStream = tokenizer;
        for (TokenFilterFactory filterFactory : tokenFilterFactories) {
          tokenStream = filterFactory.create(tokenStream);
        }
        return new TokenStreamComponents(tokenizer, tokenStream);
      }

      @Override
      public int getPositionIncrementGap(String fieldName) {
        return null == positionIncrementGap ? super.getPositionIncrementGap(fieldName) : positionIncrementGap;
      }

      @Override
      public int getOffsetGap(String fieldName) {
        return null == offsetGap ? super.getOffsetGap(fieldName) : offsetGap;
      }
    };
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("AnalyzerFactory(");
    if (null != name) {
      sb.append("name:");
      sb.append(name);
      sb.append(", ");
    }
    if (null != positionIncrementGap) {
      sb.append("positionIncrementGap:");
      sb.append(positionIncrementGap);
      sb.append(", ");
    }
    if (null != offsetGap) {
      sb.append("offsetGap:");
      sb.append(offsetGap);
      sb.append(", ");
    }
    for (CharFilterFactory charFilterFactory: charFilterFactories) {
      sb.append(charFilterFactory);
      sb.append(", ");
    }
    sb.append(tokenizerFactory);
    for (TokenFilterFactory tokenFilterFactory : tokenFilterFactories) {
      sb.append(", ");
      sb.append(tokenFilterFactory);
    }
    sb.append(')');
    return sb.toString();
  }
}

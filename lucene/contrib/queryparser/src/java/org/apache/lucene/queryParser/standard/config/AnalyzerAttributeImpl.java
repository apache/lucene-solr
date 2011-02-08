package org.apache.lucene.queryParser.standard.config;

/**
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

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.queryParser.core.config.QueryConfigHandler;
import org.apache.lucene.queryParser.standard.processors.AnalyzerQueryNodeProcessor;
import org.apache.lucene.util.AttributeImpl;

/**
 * This attribute is used by {@link AnalyzerQueryNodeProcessor} processor and
 * must be defined in the {@link QueryConfigHandler}. It provides to this
 * processor the {@link Analyzer}, if there is one, which will be used to
 * analyze the query terms. <br/>
 * 
 * @see org.apache.lucene.queryParser.standard.config.AnalyzerAttribute
 */
public class AnalyzerAttributeImpl extends AttributeImpl 
				implements AnalyzerAttribute {

  private Analyzer analyzer;

  public AnalyzerAttributeImpl() {
    analyzer = null; //default value 2.4
  }

  public void setAnalyzer(Analyzer analyzer) {
    this.analyzer = analyzer;
  }

  public Analyzer getAnalyzer() {
    return this.analyzer;
  }

  @Override
  public void clear() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void copyTo(AttributeImpl target) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean equals(Object other) {

    if (other instanceof AnalyzerAttributeImpl) {
    	AnalyzerAttributeImpl analyzerAttr = (AnalyzerAttributeImpl) other;

      if (analyzerAttr.analyzer == this.analyzer
          || (this.analyzer != null && analyzerAttr.analyzer != null && this.analyzer
              .equals(analyzerAttr.analyzer))) {

        return true;

      }

    }

    return false;

  }

  @Override
  public int hashCode() {
    return (this.analyzer == null) ? 0 : this.analyzer.hashCode();
  }

  @Override
  public String toString() {
    return "<analyzerAttribute analyzer='" + this.analyzer + "'/>";
  }

}

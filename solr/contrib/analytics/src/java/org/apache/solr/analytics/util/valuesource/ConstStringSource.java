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
package org.apache.solr.analytics.util.valuesource;

import org.apache.lucene.queries.function.valuesource.LiteralValueSource;
import org.apache.solr.analytics.util.AnalyticsParams;

/**
 * <code>ConstStringSource</code> returns a constant string for all documents
 */
public class ConstStringSource extends LiteralValueSource {
  public final static String NAME = AnalyticsParams.CONSTANT_STRING;

  public ConstStringSource(String string) {
    super(string);
  }

  @Override
  public String description() {
    return name()+"(" + string + ")";
  }

  protected String name() {
    return NAME;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof ConstStringSource)) return false;
    ConstStringSource that = (ConstStringSource) o;

    return getValue().equals(that.getValue());
  }

}

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

import java.io.IOException;
import java.text.ParseException;
import java.util.Date;

import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.queries.function.valuesource.BytesRefFieldSource;
import org.apache.solr.analytics.util.AnalyticsParams;
import org.apache.solr.util.DateMathParser;

/**
 * <code>DateMathFunction</code> returns a start date modified by a list of DateMath operations.
 */
public class DateMathFunction extends MultiDateFunction {
  public final static String NAME = AnalyticsParams.DATE_MATH;
  final private DateMathParser parser;
  
  /**
   * @param sources A list of ValueSource objects. The first element in the list
   * should be a {@link DateFieldSource} or {@link ConstDateSource} object which
   * represents the starting date. The rest of the field should be {@link BytesRefFieldSource}
   * or {@link ConstStringSource} objects which contain the DateMath operations to perform on 
   * the start date.
   */
  public DateMathFunction(ValueSource[] sources) {
    super(sources);
    parser = new DateMathParser();
  }

  @Override
  protected String name() {
    return NAME;
  }

  @Override
  protected long func(int doc, FunctionValues[] valsArr) throws IOException {
    long time = 0;
    Date date = (Date)valsArr[0].objectVal(doc);
    try {
      parser.setNow(date);
      for (int count = 1; count < valsArr.length; count++) {
          date = parser.parseMath(valsArr[count].strVal(doc));
        parser.setNow(date);
      }
      time = parser.getNow().getTime();
    } catch (ParseException e) {
      e.printStackTrace();
      time = date.getTime();
    }
    return time;
  }

}

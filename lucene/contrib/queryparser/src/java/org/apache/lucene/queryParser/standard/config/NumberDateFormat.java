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

import java.text.DateFormat;
import java.text.FieldPosition;
import java.text.NumberFormat;
import java.text.ParsePosition;
import java.util.Date;

public class NumberDateFormat extends NumberFormat {
  
  private static final long serialVersionUID = 964823936071308283L;
  
  final private DateFormat dateFormat;
  
  public NumberDateFormat(DateFormat dateFormat) {
    this.dateFormat = dateFormat;
  }
  
  @Override
  public StringBuffer format(double number, StringBuffer toAppendTo,
      FieldPosition pos) {
    return dateFormat.format(new Date((long) number), toAppendTo, pos);
  }
  
  @Override
  public StringBuffer format(long number, StringBuffer toAppendTo,
      FieldPosition pos) {
    return dateFormat.format(new Date(number), toAppendTo, pos);
  }
  
  @Override
  public Number parse(String source, ParsePosition parsePosition) {
    return dateFormat.parse(source, parsePosition).getTime();
  }
  
  @Override
  public StringBuffer format(Object number, StringBuffer toAppendTo,
      FieldPosition pos) {
    return dateFormat.format(number, toAppendTo, pos);
  }
  
}

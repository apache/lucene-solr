package org.apache.lucene.queryparser.flexible.standard.processors;

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

import java.text.DateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.TimeZone;

import org.apache.lucene.document.DateTools;
import org.apache.lucene.document.DateTools.Resolution;
import org.apache.lucene.queryparser.flexible.core.QueryNodeException;
import org.apache.lucene.queryparser.flexible.core.config.FieldConfig;
import org.apache.lucene.queryparser.flexible.core.config.QueryConfigHandler;
import org.apache.lucene.queryparser.flexible.core.nodes.FieldQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.QueryNode;
import org.apache.lucene.queryparser.flexible.core.processors.QueryNodeProcessorImpl;
import org.apache.lucene.queryparser.flexible.standard.config.StandardQueryConfigHandler.ConfigurationKeys;
import org.apache.lucene.queryparser.flexible.standard.nodes.TermRangeQueryNode;

/**
 * This processors process {@link TermRangeQueryNode}s. It reads the lower and
 * upper bounds value from the {@link TermRangeQueryNode} object and try
 * to parse their values using a {@link DateFormat}. If the values cannot be
 * parsed to a date value, it will only create the {@link TermRangeQueryNode}
 * using the non-parsed values. <br/>
 * <br/>
 * If a {@link ConfigurationKeys#LOCALE} is defined in the
 * {@link QueryConfigHandler} it will be used to parse the date, otherwise
 * {@link Locale#getDefault()} will be used. <br/>
 * <br/>
 * If a {@link ConfigurationKeys#DATE_RESOLUTION} is defined and the
 * {@link Resolution} is not <code>null</code> it will also be used to parse the
 * date value. <br/>
 * <br/>
 * 
 * @see ConfigurationKeys#DATE_RESOLUTION
 * @see ConfigurationKeys#LOCALE
 * @see TermRangeQueryNode
 */
public class TermRangeQueryNodeProcessor extends QueryNodeProcessorImpl {
  
  public TermRangeQueryNodeProcessor() {
  // empty constructor
  }
  
  @Override
  protected QueryNode postProcessNode(QueryNode node) throws QueryNodeException {
    
    if (node instanceof TermRangeQueryNode) {
      TermRangeQueryNode termRangeNode = (TermRangeQueryNode) node;
      FieldQueryNode upper = termRangeNode.getUpperBound();
      FieldQueryNode lower = termRangeNode.getLowerBound();
      
      DateTools.Resolution dateRes = null;
      boolean inclusive = false;
      Locale locale = getQueryConfigHandler().get(ConfigurationKeys.LOCALE);
      
      if (locale == null) {
        locale = Locale.getDefault();
      }
      
      TimeZone timeZone = getQueryConfigHandler().get(ConfigurationKeys.TIMEZONE);
      
      if (timeZone == null) {
        timeZone = TimeZone.getDefault();
      }
      
      CharSequence field = termRangeNode.getField();
      String fieldStr = null;
      
      if (field != null) {
        fieldStr = field.toString();
      }
      
      FieldConfig fieldConfig = getQueryConfigHandler()
          .getFieldConfig(fieldStr);
      
      if (fieldConfig != null) {
        dateRes = fieldConfig.get(ConfigurationKeys.DATE_RESOLUTION);
      }
      
      if (termRangeNode.isUpperInclusive()) {
        inclusive = true;
      }
      
      String part1 = lower.getTextAsString();
      String part2 = upper.getTextAsString();
      
      try {
        DateFormat df = DateFormat.getDateInstance(DateFormat.SHORT, locale);
        df.setLenient(true);
        
        if (part1.length() > 0) {
          Date d1 = df.parse(part1);
          part1 = DateTools.dateToString(d1, dateRes);
          lower.setText(part1);
        }
        
        if (part2.length() > 0) {
          Date d2 = df.parse(part2);
          if (inclusive) {
            // The user can only specify the date, not the time, so make sure
            // the time is set to the latest possible time of that date to
            // really
            // include all documents:
            Calendar cal = Calendar.getInstance(timeZone, locale);
            cal.setTime(d2);
            cal.set(Calendar.HOUR_OF_DAY, 23);
            cal.set(Calendar.MINUTE, 59);
            cal.set(Calendar.SECOND, 59);
            cal.set(Calendar.MILLISECOND, 999);
            d2 = cal.getTime();
          }
          
          part2 = DateTools.dateToString(d2, dateRes);
          upper.setText(part2);
          
        }
        
      } catch (Exception e) {
        // do nothing
      }
      
    }
    
    return node;
    
  }
  
  @Override
  protected QueryNode preProcessNode(QueryNode node) throws QueryNodeException {
    
    return node;
    
  }
  
  @Override
  protected List<QueryNode> setChildrenOrder(List<QueryNode> children)
      throws QueryNodeException {
    
    return children;
    
  }
  
}

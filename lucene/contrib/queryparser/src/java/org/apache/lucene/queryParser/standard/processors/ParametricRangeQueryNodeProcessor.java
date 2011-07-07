package org.apache.lucene.queryParser.standard.processors;

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
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Locale;

import org.apache.lucene.document.DateTools;
import org.apache.lucene.document.DateTools.Resolution;
import org.apache.lucene.queryParser.core.QueryNodeException;
import org.apache.lucene.queryParser.core.config.FieldConfig;
import org.apache.lucene.queryParser.core.config.QueryConfigHandler;
import org.apache.lucene.queryParser.core.nodes.ParametricQueryNode;
import org.apache.lucene.queryParser.core.nodes.ParametricRangeQueryNode;
import org.apache.lucene.queryParser.core.nodes.QueryNode;
import org.apache.lucene.queryParser.core.nodes.ParametricQueryNode.CompareOperator;
import org.apache.lucene.queryParser.core.processors.QueryNodeProcessorImpl;
import org.apache.lucene.queryParser.standard.config.StandardQueryConfigHandler;
import org.apache.lucene.queryParser.standard.config.StandardQueryConfigHandler.ConfigurationKeys;
import org.apache.lucene.queryParser.standard.nodes.RangeQueryNode;

/**
 * This processor converts {@link ParametricRangeQueryNode} objects to
 * {@link RangeQueryNode} objects. It reads the lower and upper bounds value
 * from the {@link ParametricRangeQueryNode} object and try to parse their
 * values using a {@link DateFormat}. If the values cannot be parsed to a date
 * value, it will only create the {@link RangeQueryNode} using the non-parsed
 * values. <br/>
 * <br/>
 * If a {@link ConfigurationKeys#LOCALE} is defined in the {@link QueryConfigHandler} it
 * will be used to parse the date, otherwise {@link Locale#getDefault()} will be
 * used. <br/>
 * <br/>
 * If a {@link ConfigurationKeys#DATE_RESOLUTION} is defined and the {@link Resolution} is
 * not <code>null</code> it will also be used to parse the date value. <br/>
 * <br/>
 * 
 * @see ConfigurationKeys#DATE_RESOLUTION
 * @see ConfigurationKeys#LOCALE
 * @see RangeQueryNode
 * @see ParametricRangeQueryNode
 */
public class ParametricRangeQueryNodeProcessor extends QueryNodeProcessorImpl {

  public ParametricRangeQueryNodeProcessor() {
    // empty constructor
  }

  @Override
  protected QueryNode postProcessNode(QueryNode node) throws QueryNodeException {

    if (node instanceof ParametricRangeQueryNode) {
      ParametricRangeQueryNode parametricRangeNode = (ParametricRangeQueryNode) node;
      ParametricQueryNode upper = parametricRangeNode.getUpperBound();
      ParametricQueryNode lower = parametricRangeNode.getLowerBound();
      
      DateTools.Resolution dateRes = null;
      boolean inclusive = false;
      Locale locale = getQueryConfigHandler().get(ConfigurationKeys.LOCALE);

      if (locale == null) {
        locale = Locale.getDefault();
      }

      CharSequence field = parametricRangeNode.getField();
      String fieldStr = null;

      if (field != null) {
        fieldStr = field.toString();
      }

      FieldConfig fieldConfig = getQueryConfigHandler()
          .getFieldConfig(fieldStr);

      if (fieldConfig != null) {
        dateRes = fieldConfig.get(ConfigurationKeys.DATE_RESOLUTION);
      }

      if (upper.getOperator() == CompareOperator.LE) {
        inclusive = true;

      } else if (lower.getOperator() == CompareOperator.GE) {
        inclusive = true;
      }

      String part1 = lower.getTextAsString();
      String part2 = upper.getTextAsString();

      try {
        DateFormat df = DateFormat.getDateInstance(DateFormat.SHORT, locale);
        df.setLenient(true);
        Date d1 = df.parse(part1);
        Date d2 = df.parse(part2);
        if (inclusive) {
          // The user can only specify the date, not the time, so make sure
          // the time is set to the latest possible time of that date to really
          // include all documents:
          Calendar cal = Calendar.getInstance(locale);
          cal.setTime(d2);
          cal.set(Calendar.HOUR_OF_DAY, 23);
          cal.set(Calendar.MINUTE, 59);
          cal.set(Calendar.SECOND, 59);
          cal.set(Calendar.MILLISECOND, 999);
          d2 = cal.getTime();
        }

        part1 = DateTools.dateToString(d1, dateRes);
        part2 = DateTools.dateToString(d2, dateRes);
      } catch (Exception e) {
        // do nothing
      }

      lower.setText(part1);
      upper.setText(part2);

      return new RangeQueryNode(lower, upper);

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

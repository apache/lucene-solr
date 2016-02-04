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
package org.apache.lucene.queryparser.xml.builders;

import org.apache.lucene.search.NumericRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.NumericUtils;
import org.apache.lucene.queryparser.xml.DOMUtils;
import org.apache.lucene.queryparser.xml.ParserException;
import org.apache.lucene.queryparser.xml.QueryBuilder;
import org.w3c.dom.Element;

/**
 * Creates a {@link NumericRangeQuery}. The table below specifies the required
 * attributes and the defaults if optional attributes are omitted. For more
 * detail on what each of the attributes actually do, consult the documentation
 * for {@link NumericRangeQuery}:
 * <table summary="supported attributes">
 * <tr>
 * <th>Attribute name</th>
 * <th>Values</th>
 * <th>Required</th>
 * <th>Default</th>
 * </tr>
 * <tr>
 * <td>fieldName</td>
 * <td>String</td>
 * <td>Yes</td>
 * <td>N/A</td>
 * </tr>
 * <tr>
 * <td>lowerTerm</td>
 * <td>Specified by <tt>type</tt></td>
 * <td>Yes</td>
 * <td>N/A</td>
 * </tr>
 * <tr>
 * <td>upperTerm</td>
 * <td>Specified by <tt>type</tt></td>
 * <td>Yes</td>
 * <td>N/A</td>
 * </tr>
 * <tr>
 * <td>type</td>
 * <td>int, long, float, double</td>
 * <td>No</td>
 * <td>int</td>
 * </tr>
 * <tr>
 * <td>includeLower</td>
 * <td>true, false</td>
 * <td>No</td>
 * <td>true</td>
 * </tr>
 * <tr>
 * <td>includeUpper</td>
 * <td>true, false</td>
 * <td>No</td>
 * <td>true</td>
 * </tr>
 * <tr>
 * <td>precisionStep</td>
 * <td>Integer</td>
 * <td>No</td>
 * <td>4</td>
 * </tr>
 * </table>
 * <p>
 * A {@link ParserException} will be thrown if an error occurs parsing the
 * supplied <tt>lowerTerm</tt> or <tt>upperTerm</tt> into the numeric type
 * specified by <tt>type</tt>.
 */
public class NumericRangeQueryBuilder implements QueryBuilder {

  @Override
  public Query getQuery(Element e) throws ParserException {
    String field = DOMUtils.getAttributeWithInheritanceOrFail(e, "fieldName");
    String lowerTerm = DOMUtils.getAttributeOrFail(e, "lowerTerm");
    String upperTerm = DOMUtils.getAttributeOrFail(e, "upperTerm");
    boolean lowerInclusive = DOMUtils.getAttribute(e, "includeLower", true);
    boolean upperInclusive = DOMUtils.getAttribute(e, "includeUpper", true);
    int precisionStep = DOMUtils.getAttribute(e, "precisionStep", NumericUtils.PRECISION_STEP_DEFAULT);

    String type = DOMUtils.getAttribute(e, "type", "int");
    try {
      Query filter;
      if (type.equalsIgnoreCase("int")) {
        filter = NumericRangeQuery.newIntRange(field, precisionStep, Integer
            .valueOf(lowerTerm), Integer.valueOf(upperTerm), lowerInclusive,
            upperInclusive);
      } else if (type.equalsIgnoreCase("long")) {
        filter = NumericRangeQuery.newLongRange(field, precisionStep, Long
            .valueOf(lowerTerm), Long.valueOf(upperTerm), lowerInclusive,
            upperInclusive);
      } else if (type.equalsIgnoreCase("double")) {
        filter = NumericRangeQuery.newDoubleRange(field, precisionStep, Double
            .valueOf(lowerTerm), Double.valueOf(upperTerm), lowerInclusive,
            upperInclusive);
      } else if (type.equalsIgnoreCase("float")) {
        filter = NumericRangeQuery.newFloatRange(field, precisionStep, Float
            .valueOf(lowerTerm), Float.valueOf(upperTerm), lowerInclusive,
            upperInclusive);
      } else {
        throw new ParserException("type attribute must be one of: [long, int, double, float]");
      }
      return filter;
    } catch (NumberFormatException nfe) {
      throw new ParserException("Could not parse lowerTerm or upperTerm into a number", nfe);
    }
  }
}

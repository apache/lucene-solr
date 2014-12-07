package org.apache.lucene.queryparser.xml.builders;

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

import org.apache.lucene.document.FieldTypes;
import org.apache.lucene.queryparser.xml.DOMUtils;
import org.apache.lucene.queryparser.xml.ParserException;
import org.apache.lucene.queryparser.xml.QueryBuilder;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.NumericUtils;
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
 * <p/>
 * A {@link ParserException} will be thrown if an error occurs parsing the
 * supplied <tt>lowerTerm</tt> or <tt>upperTerm</tt> into the numeric type
 * specified by <tt>type</tt>.
 */
public class NumericRangeQueryBuilder implements QueryBuilder {

  @Override
  public Query getQuery(FieldTypes fieldTypes, Element e) throws ParserException {
    String field = DOMUtils.getAttributeWithInheritanceOrFail(e, "fieldName");
    String lowerTerm = DOMUtils.getAttributeOrFail(e, "lowerTerm");
    String upperTerm = DOMUtils.getAttributeOrFail(e, "upperTerm");
    boolean lowerInclusive = DOMUtils.getAttribute(e, "includeLower", true);
    boolean upperInclusive = DOMUtils.getAttribute(e, "includeUpper", true);

    String type = DOMUtils.getAttribute(e, "type", "int");

    try {
      Filter filter;
      if (type.equalsIgnoreCase("int")) {
        filter = fieldTypes.newIntRangeFilter(field, Integer.valueOf(lowerTerm), lowerInclusive, Integer.valueOf(upperTerm), upperInclusive);
      } else if (type.equalsIgnoreCase("long")) {
        filter = fieldTypes.newLongRangeFilter(field, Long.valueOf(lowerTerm), lowerInclusive, Long.valueOf(upperTerm), upperInclusive);
      } else if (type.equalsIgnoreCase("double")) {
        filter = fieldTypes.newDoubleRangeFilter(field, Double.valueOf(lowerTerm), lowerInclusive, Double.valueOf(upperTerm), upperInclusive);
      } else if (type.equalsIgnoreCase("float")) {
        filter = fieldTypes.newFloatRangeFilter(field, Float.valueOf(lowerTerm), lowerInclusive, Float.valueOf(upperTerm), upperInclusive);
      } else {
        throw new ParserException("type attribute must be one of: [long, int, double, float]");
      }
      return new ConstantScoreQuery(filter);
    } catch (NumberFormatException nfe) {
      throw new ParserException("Could not parse lowerTerm or upperTerm into a number", nfe);
    }
  }
}

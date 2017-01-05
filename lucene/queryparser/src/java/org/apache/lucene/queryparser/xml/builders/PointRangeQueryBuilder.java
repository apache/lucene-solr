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

import org.apache.lucene.search.Query;
import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.FloatPoint;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.queryparser.xml.DOMUtils;
import org.apache.lucene.queryparser.xml.ParserException;
import org.apache.lucene.queryparser.xml.QueryBuilder;
import org.w3c.dom.Element;

/**
 * Creates a range query across 1D {@link PointValues}. The table below specifies the required
 * attributes and the defaults if optional attributes are omitted:
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
 * <td>No</td>
 * <td>Integer.MIN_VALUE Long.MIN_VALUE Float.NEGATIVE_INFINITY Double.NEGATIVE_INFINITY</td>
 * </tr>
 * <tr>
 * <td>upperTerm</td>
 * <td>Specified by <tt>type</tt></td>
 * <td>No</td>
 * <td>Integer.MAX_VALUE Long.MAX_VALUE Float.POSITIVE_INFINITY Double.POSITIVE_INFINITY</td>
 * </tr>
 * <tr>
 * <td>type</td>
 * <td>int, long, float, double</td>
 * <td>No</td>
 * <td>int</td>
 * </tr>
 * </table>
 * <p>
 * A {@link ParserException} will be thrown if an error occurs parsing the
 * supplied <tt>lowerTerm</tt> or <tt>upperTerm</tt> into the numeric type
 * specified by <tt>type</tt>.
 */
public class PointRangeQueryBuilder implements QueryBuilder {

  @Override
  public Query getQuery(Element e) throws ParserException {
    String field = DOMUtils.getAttributeWithInheritanceOrFail(e, "fieldName");
    final String lowerTerm = DOMUtils.getAttribute(e, "lowerTerm", null);
    final String upperTerm = DOMUtils.getAttribute(e, "upperTerm", null);

    String type = DOMUtils.getAttribute(e, "type", "int");
    try {
      if (type.equalsIgnoreCase("int")) {
        return IntPoint.newRangeQuery(field,
            (lowerTerm == null ? Integer.MIN_VALUE : Integer.valueOf(lowerTerm)),
            (upperTerm == null ? Integer.MAX_VALUE : Integer.valueOf(upperTerm)));
      } else if (type.equalsIgnoreCase("long")) {
        return LongPoint.newRangeQuery(field,
            (lowerTerm == null ? Long.MIN_VALUE : Long.valueOf(lowerTerm)),
            (upperTerm == null ? Long.MAX_VALUE : Long.valueOf(upperTerm)));
      } else if (type.equalsIgnoreCase("double")) {
        return DoublePoint.newRangeQuery(field,
            (lowerTerm == null ? Double.NEGATIVE_INFINITY : Double.valueOf(lowerTerm)),
            (upperTerm == null ? Double.POSITIVE_INFINITY : Double.valueOf(upperTerm)));
      } else if (type.equalsIgnoreCase("float")) {
        return FloatPoint.newRangeQuery(field,
            (lowerTerm == null ? Float.NEGATIVE_INFINITY : Float.valueOf(lowerTerm)),
            (upperTerm == null ? Float.POSITIVE_INFINITY : Float.valueOf(upperTerm)));
      } else {
        throw new ParserException("type attribute must be one of: [long, int, double, float]");
      }
    } catch (NumberFormatException nfe) {
      throw new ParserException("Could not parse lowerTerm or upperTerm into a number", nfe);
    }
  }
}

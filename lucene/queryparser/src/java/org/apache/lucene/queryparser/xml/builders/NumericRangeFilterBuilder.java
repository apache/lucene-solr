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

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.NumericRangeFilter;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.NumericUtils;
import org.apache.lucene.queryparser.xml.DOMUtils;
import org.apache.lucene.queryparser.xml.FilterBuilder;
import org.apache.lucene.queryparser.xml.ParserException;
import org.w3c.dom.Element;

import java.io.IOException;

/**
 * Creates a {@link NumericRangeFilter}. The table below specifies the required
 * attributes and the defaults if optional attributes are omitted. For more
 * detail on what each of the attributes actually do, consult the documentation
 * for {@link NumericRangeFilter}:
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
 * If an error occurs parsing the supplied <tt>lowerTerm</tt> or
 * <tt>upperTerm</tt> into the numeric type specified by <tt>type</tt>, then the
 * error will be silently ignored and the resulting filter will not match any
 * documents.
 */
public class NumericRangeFilterBuilder implements FilterBuilder {
  
  private static final NoMatchFilter NO_MATCH_FILTER = new NoMatchFilter();

  private boolean strictMode = false;

  /**
   * Specifies how this {@link NumericRangeFilterBuilder} will handle errors.
   * <p>
   * If this is set to true, {@link #getFilter(Element)} will throw a
   * {@link ParserException} if it is unable to parse the lowerTerm or upperTerm
   * into the appropriate numeric type. If this is set to false, then this
   * exception will be silently ignored and the resulting filter will not match
   * any documents.
   * <p>
   * Defaults to false.
   */
  public void setStrictMode(boolean strictMode) {
    this.strictMode = strictMode;
  }

  @Override
  public Filter getFilter(Element e) throws ParserException {
    String field = DOMUtils.getAttributeWithInheritanceOrFail(e, "fieldName");
    String lowerTerm = DOMUtils.getAttributeOrFail(e, "lowerTerm");
    String upperTerm = DOMUtils.getAttributeOrFail(e, "upperTerm");
    boolean lowerInclusive = DOMUtils.getAttribute(e, "includeLower", true);
    boolean upperInclusive = DOMUtils.getAttribute(e, "includeUpper", true);
    int precisionStep = DOMUtils.getAttribute(e, "precisionStep", NumericUtils.PRECISION_STEP_DEFAULT);

    String type = DOMUtils.getAttribute(e, "type", "int");
    try {
      Filter filter;
      if (type.equalsIgnoreCase("int")) {
        filter = NumericRangeFilter.newIntRange(field, precisionStep, Integer
            .valueOf(lowerTerm), Integer.valueOf(upperTerm), lowerInclusive,
            upperInclusive);
      } else if (type.equalsIgnoreCase("long")) {
        filter = NumericRangeFilter.newLongRange(field, precisionStep, Long
            .valueOf(lowerTerm), Long.valueOf(upperTerm), lowerInclusive,
            upperInclusive);
      } else if (type.equalsIgnoreCase("double")) {
        filter = NumericRangeFilter.newDoubleRange(field, precisionStep, Double
            .valueOf(lowerTerm), Double.valueOf(upperTerm), lowerInclusive,
            upperInclusive);
      } else if (type.equalsIgnoreCase("float")) {
        filter = NumericRangeFilter.newFloatRange(field, precisionStep, Float
            .valueOf(lowerTerm), Float.valueOf(upperTerm), lowerInclusive,
            upperInclusive);
      } else {
        throw new ParserException("type attribute must be one of: [long, int, double, float]");
      }
      return filter;
    } catch (NumberFormatException nfe) {
      if (strictMode) {
        throw new ParserException("Could not parse lowerTerm or upperTerm into a number", nfe);
      }
      return NO_MATCH_FILTER;
    }
  }

  static class NoMatchFilter extends Filter {

    @Override
    public DocIdSet getDocIdSet(LeafReaderContext context, Bits acceptDocs) throws IOException {
      return null;
    }


    @Override
    public String toString(String field) {
      return "NoMatchFilter()";
    }
  }
}

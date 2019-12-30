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
package org.apache.lucene.spatial.query;

import org.locationtech.spatial4j.context.SpatialContext;
import org.locationtech.spatial4j.exception.InvalidShapeException;
import org.locationtech.spatial4j.shape.Shape;

import java.text.ParseException;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.StringTokenizer;

/**
 * Parses a string that usually looks like "OPERATION(SHAPE)" into a {@link SpatialArgs}
 * object. The set of operations supported are defined in {@link SpatialOperation}, such
 * as "Intersects" being a common one. The shape portion is defined by WKT {@link org.locationtech.spatial4j.io.WktShapeParser},
 * but it can be overridden/customized via {@link #parseShape(String, org.locationtech.spatial4j.context.SpatialContext)}.
 * There are some optional name-value pair parameters that follow the closing parenthesis.  Example:
 * <pre>
 *   Intersects(ENVELOPE(-10,-8,22,20)) distErrPct=0.025
 * </pre>
 * <p>
 * In the future it would be good to support something at least semi-standardized like a
 * variant of <a href="http://docs.geoserver.org/latest/en/user/filter/ecql_reference.html#spatial-predicate">
 *   [E]CQL</a>.
 *
 * @lucene.experimental
 */
public class SpatialArgsParser {

  public static final String DIST_ERR_PCT = "distErrPct";
  public static final String DIST_ERR = "distErr";

  /** Writes a close approximation to the parsed input format. */
  static String writeSpatialArgs(SpatialArgs args) {
    StringBuilder str = new StringBuilder();
    str.append(args.getOperation().getName());
    str.append('(');
    str.append(args.getShape().toString());
    if (args.getDistErrPct() != null)
      str.append(" distErrPct=").append(String.format(Locale.ROOT, "%.2f%%", args.getDistErrPct() * 100d));
    if (args.getDistErr() != null)
      str.append(" distErr=").append(args.getDistErr());
    str.append(')');
    return str.toString();
  }

  /**
   * Parses a string such as "Intersects(ENVELOPE(-10,-8,22,20)) distErrPct=0.025".
   *
   * @param v   The string to parse. Mandatory.
   * @param ctx The spatial context. Mandatory.
   * @return Not null.
   * @throws IllegalArgumentException if the parameters don't make sense or an add-on parameter is unknown
   * @throws ParseException If there is a problem parsing the string
   * @throws InvalidShapeException When the coordinates are invalid for the shape
   */
  public SpatialArgs parse(String v, SpatialContext ctx) throws ParseException, InvalidShapeException {
    int idx = v.indexOf('(');
    int edx = v.lastIndexOf(')');

    if (idx < 0 || idx > edx) {
      throw new ParseException("missing parens: " + v, -1);
    }

    SpatialOperation op = SpatialOperation.get(v.substring(0, idx).trim());

    String body = v.substring(idx + 1, edx).trim();
    if (body.length() < 1) {
      throw new ParseException("missing body : " + v, idx + 1);
    }

    Shape shape = parseShape(body, ctx);
    SpatialArgs args = newSpatialArgs(op, shape);

    if (v.length() > (edx + 1)) {
      body = v.substring(edx + 1).trim();
      if (body.length() > 0) {
        Map<String, String> aa = parseMap(body);
        readNameValuePairs(args, aa);
        if (!aa.isEmpty()) {
          throw new IllegalArgumentException("unused parameters: " + aa);
        }
      }
    }
    args.validate();
    return args;
  }

  protected SpatialArgs newSpatialArgs(SpatialOperation op, Shape shape) {
    return new SpatialArgs(op, shape);
  }

  protected void readNameValuePairs(SpatialArgs args, Map<String, String> nameValPairs) {
    args.setDistErrPct(readDouble(nameValPairs.remove(DIST_ERR_PCT)));
    args.setDistErr(readDouble(nameValPairs.remove(DIST_ERR)));
  }

  protected Shape parseShape(String str, SpatialContext ctx) throws ParseException {
    //return ctx.readShape(str);//still in Spatial4j 0.4 but will be deleted
    return ctx.readShapeFromWkt(str);
  }

  protected static Double readDouble(String v) {
    return v == null ? null : Double.valueOf(v);
  }

  protected static boolean readBool(String v, boolean defaultValue) {
    return v == null ? defaultValue : Boolean.parseBoolean(v);
  }

  /** Parses "a=b c=d f" (whitespace separated) into name-value pairs. If there
   * is no '=' as in 'f' above then it's short for f=f. */
  protected static Map<String, String> parseMap(String body) {
    Map<String, String> map = new HashMap<>();
    StringTokenizer st = new StringTokenizer(body, " \n\t");
    while (st.hasMoreTokens()) {
      String a = st.nextToken();
      int idx = a.indexOf('=');
      if (idx > 0) {
        String k = a.substring(0, idx);
        String v = a.substring(idx + 1);
        map.put(k, v);
      } else {
        map.put(a, a);
      }
    }
    return map;
  }
}

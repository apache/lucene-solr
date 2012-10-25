package org.apache.lucene.spatial.query;

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

import com.spatial4j.core.context.SpatialContext;
import com.spatial4j.core.exception.InvalidShapeException;
import com.spatial4j.core.io.ShapeReadWriter;
import com.spatial4j.core.shape.Shape;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.StringTokenizer;

/**
 * Parses a string that usually looks like "OPERATION(SHAPE)" into a {@link SpatialArgs}
 * object. The set of operations supported are defined in {@link SpatialOperation}, such
 * as "Intersects" being a common one. The shape portion is defined by {@link
 * ShapeReadWriter#readShape(String)}. There are some optional name-value pair parameters
 * that follow the closing parenthesis.  Example:
 * <pre>
 *   Intersects(-10,20,-8,22) distErrPct=0.025
 * </pre>
 * <p/>
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
   * Parses a string such as "Intersects(-10,20,-8,22) distErrPct=0.025".
   *
   * @param v   The string to parse. Mandatory.
   * @param ctx The spatial context. Mandatory.
   * @return Not null.
   * @throws IllegalArgumentException If there is a problem parsing the string.
   * @throws InvalidShapeException  Thrown from {@link ShapeReadWriter#readShape(String)}
   */
  public SpatialArgs parse(String v, SpatialContext ctx) throws IllegalArgumentException, InvalidShapeException {
    int idx = v.indexOf('(');
    int edx = v.lastIndexOf(')');

    if (idx < 0 || idx > edx) {
      throw new IllegalArgumentException("missing parens: " + v, null);
    }

    SpatialOperation op = SpatialOperation.get(v.substring(0, idx).trim());

    String body = v.substring(idx + 1, edx).trim();
    if (body.length() < 1) {
      throw new IllegalArgumentException("missing body : " + v, null);
    }

    Shape shape = ctx.readShape(body);
    SpatialArgs args = new SpatialArgs(op, shape);

    if (v.length() > (edx + 1)) {
      body = v.substring(edx + 1).trim();
      if (body.length() > 0) {
        Map<String, String> aa = parseMap(body);
        args.setDistErrPct(readDouble(aa.remove(DIST_ERR_PCT)));
        args.setDistErr(readDouble(aa.remove(DIST_ERR)));
        if (!aa.isEmpty()) {
          throw new IllegalArgumentException("unused parameters: " + aa, null);
        }
      }
    }
    args.validate();
    return args;
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
    Map<String, String> map = new HashMap<String, String>();
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

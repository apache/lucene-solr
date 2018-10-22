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
package org.apache.solr.util;

import java.text.ParseException;

import org.locationtech.spatial4j.context.SpatialContext;
import org.locationtech.spatial4j.exception.InvalidShapeException;
import org.locationtech.spatial4j.shape.Point;
import org.locationtech.spatial4j.shape.Rectangle;
import org.locationtech.spatial4j.shape.Shape;
import org.apache.solr.common.SolrException;

/** Utility methods pertaining to spatial. */
public class SpatialUtils {

  private SpatialUtils() {}

  /**
   * Parses a 'geom' parameter (might also be used to parse shapes for indexing). {@code geomStr} can either be WKT or
   * a rectangle-range syntax (see {@link #parseRectangle(String, org.locationtech.spatial4j.context.SpatialContext)}.
   */
  public static Shape parseGeomSolrException(String geomStr, SpatialContext ctx) {
    if (geomStr.length() == 0) {
      throw new IllegalArgumentException("0-length geometry string");
    }
    char c = geomStr.charAt(0);
    if (c == '[' || c == '{') {
      return parseRectangeSolrException(geomStr, ctx);
    }
    //TODO parse a raw point?
    try {
      return ctx.readShapeFromWkt(geomStr);
    } catch (ParseException e) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
          "Expecting WKT or '[minPoint TO maxPoint]': " + e, e);
    }

  }

  /** Parses either "lat, lon" (spaces optional on either comma side) or "x y" style formats. Spaces can be basically
   * anywhere.  And not any whitespace, just the space char.
   *
   * @param str Non-null; may have leading or trailing spaces
   * @param ctx Non-null
   * @return Non-null
   * @throws InvalidShapeException If for any reason there was a problem parsing the string or creating the point.
   */
  public static Point parsePoint(String str, SpatialContext ctx) throws InvalidShapeException {
    //note we don't do generic whitespace, just a literal space char detection
    try {
      double x, y;
      str = str.trim();//TODO use findIndexNotSpace instead?
      int commaIdx = str.indexOf(',');
      if (commaIdx == -1) {
        //  "x y" format
        int spaceIdx = str.indexOf(' ');
        if (spaceIdx == -1)
          throw new InvalidShapeException("Point must be in 'lat, lon' or 'x y' format: " + str);
        int middleEndIdx = findIndexNotSpace(str, spaceIdx + 1, +1);
        x = Double.parseDouble(str.substring(0, spaceIdx));
        y = Double.parseDouble(str.substring(middleEndIdx));
      } else {
        // "lat, lon" format
        int middleStartIdx = findIndexNotSpace(str, commaIdx - 1, -1);
        int middleEndIdx = findIndexNotSpace(str, commaIdx + 1, +1);
        y = Double.parseDouble(str.substring(0, middleStartIdx + 1));
        x = Double.parseDouble(str.substring(middleEndIdx));
      }

      x = ctx.normX(x);//by default norm* methods do nothing but perhaps it's been customized
      y = ctx.normY(y);
      return ctx.makePoint(x, y);//will verify x & y fit in boundary
    } catch (InvalidShapeException e) {
      throw e;
    } catch (Exception e) {
      throw new InvalidShapeException(e.toString(), e);
    }
  }

  private static int findIndexNotSpace(String str, int startIdx, int inc) {
    assert inc == +1 || inc == -1;
    int idx = startIdx;
    while (idx >= 0 && idx < str.length() && str.charAt(idx) == ' ')
      idx += inc;
    return idx;
  }

  /** Calls {@link #parsePoint(String, org.locationtech.spatial4j.context.SpatialContext)} and wraps
   * the exception with {@link org.apache.solr.common.SolrException} with a helpful message. */
  public static Point parsePointSolrException(String externalVal, SpatialContext ctx) throws SolrException {
    try {
      return parsePoint(externalVal, ctx);
    } catch (InvalidShapeException e) {
      String message = e.getMessage();
      if (!message.contains(externalVal))
        message = "Can't parse point '" + externalVal + "' because: " + message;
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, message, e);
    }
  }

  /**
   * Parses {@code str} in the format of '[minPoint TO maxPoint]' where {@code minPoint} is the lower left corner
   * and maxPoint is the upper-right corner of the bounding box.  Both corners may optionally be wrapped with a quote
   * and then it's parsed via {@link #parsePoint(String, org.locationtech.spatial4j.context.SpatialContext)}.
   * @param str Non-null; may *not* have leading or trailing spaces
   * @param ctx Non-null
   * @return the Rectangle
   * @throws InvalidShapeException If for any reason there was a problem parsing the string or creating the rectangle.
   */
  public static Rectangle parseRectangle(String str, SpatialContext ctx) throws InvalidShapeException {
    //note we don't do generic whitespace, just a literal space char detection
    try {
      int toIdx = str.indexOf(" TO ");
      if (toIdx == -1 || str.charAt(0) != '[' || str.charAt(str.length() - 1) != ']') {
        throw new InvalidShapeException("expecting '[bottomLeft TO topRight]'");
      }
      String leftPart = unwrapQuotes(str.substring(1, toIdx).trim());
      String rightPart = unwrapQuotes(str.substring(toIdx + " TO ".length(), str.length() - 1).trim());
      return ctx.makeRectangle(parsePoint(leftPart, ctx), parsePoint(rightPart, ctx));
    } catch (InvalidShapeException e) {
      throw e;
    } catch (Exception e) {
      throw new InvalidShapeException(e.toString(), e);
    }
  }

  /**
   * Calls {@link #parseRectangle(String, org.locationtech.spatial4j.context.SpatialContext)} and wraps the exception with
   * {@link org.apache.solr.common.SolrException} with a helpful message.
   */
  public static Rectangle parseRectangeSolrException(String externalVal, SpatialContext ctx) throws SolrException {
    try {
      return parseRectangle(externalVal, ctx);
    } catch (InvalidShapeException e) {
      String message = e.getMessage();
      if (!message.contains(externalVal))
        message = "Can't parse rectangle '" + externalVal + "' because: " + message;
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, message, e);
    }
  }

  private static String unwrapQuotes(String str) {
    if (str.length() >= 2 && str.charAt(0) == '\"' && str.charAt(str.length()-1) == '\"') {
      return str.substring(1, str.length()-1);
    }
    return str;
  }

}

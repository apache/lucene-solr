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
package org.apache.lucene.geo;

import java.io.IOException;
import java.io.StreamTokenizer;
import java.io.StringReader;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

/**
 * Parses shape geometry represented in WKT format
 *
 * complies with OGC® document: 12-063r5 and ISO/IEC 13249-3:2016 standard
 * located at http://docs.opengeospatial.org/is/12-063r5/12-063r5.html
 */
public class SimpleWKTShapeParser {
  public static final String EMPTY = "EMPTY";
  public static final String SPACE = " ";
  public static final String LPAREN = "(";
  public static final String RPAREN = ")";
  public static final String COMMA = ",";
  public static final String NAN = "NaN";

  private static final String NUMBER = "<NUMBER>";
  private static final String EOF = "END-OF-STREAM";
  private static final String EOL = "END-OF-LINE";

  // no instance
  private SimpleWKTShapeParser() {}

  public static Object parse(String wkt) throws IOException, ParseException {
    return parseExpectedType(wkt, null);
  }

  public static Object parseExpectedType(String wkt, final ShapeType shapeType) throws IOException, ParseException {
    try (StringReader reader = new StringReader(wkt)) {
      // setup the tokenizer; configured to read words w/o numbers
      StreamTokenizer tokenizer = new StreamTokenizer(reader);
      tokenizer.resetSyntax();
      tokenizer.wordChars('a', 'z');
      tokenizer.wordChars('A', 'Z');
      tokenizer.wordChars(128 + 32, 255);
      tokenizer.wordChars('0', '9');
      tokenizer.wordChars('-', '-');
      tokenizer.wordChars('+', '+');
      tokenizer.wordChars('.', '.');
      tokenizer.whitespaceChars(0, ' ');
      tokenizer.commentChar('#');
      Object geometry = parseGeometry(tokenizer, shapeType);
      checkEOF(tokenizer);
      return geometry;
    }
  }

  /** parse geometry from the stream tokenizer */
  private static Object parseGeometry(StreamTokenizer stream, ShapeType shapeType) throws IOException, ParseException {
    final ShapeType type = ShapeType.forName(nextWord(stream));
    if (shapeType != null && shapeType != ShapeType.GEOMETRYCOLLECTION) {
      if (type.wktName().equals(shapeType.wktName()) == false) {
        throw new ParseException("Expected geometry type: [" + shapeType + "], but found: [" + type + "]", stream.lineno());
      }
    }
    switch (type) {
      case POINT:
        return parsePoint(stream);
      case MULTIPOINT:
        return parseMultiPoint(stream);
      case LINESTRING:
        return parseLine(stream);
      case MULTILINESTRING:
        return parseMultiLine(stream);
      case POLYGON:
        return parsePolygon(stream);
      case MULTIPOLYGON:
        return parseMultiPolygon(stream);
      case ENVELOPE:
        return parseBBox(stream);
      case GEOMETRYCOLLECTION:
        return parseGeometryCollection(stream);
      default:
        throw new IllegalArgumentException("Unknown geometry type: " + type);
    }
  }

  /** Parses a point as a double array */
  private static double[] parsePoint(StreamTokenizer stream) throws IOException, ParseException {
    if (nextEmptyOrOpen(stream).equals(EMPTY)) {
      return null;
    }
    double[] pt = new double[]{nextNumber(stream), nextNumber(stream)};
    if (isNumberNext(stream) == true) {
      nextNumber(stream);
    }
    nextCloser(stream);
    return pt;
  }

  /** Parses a list of points into latitude and longitude arraylists */
  @SuppressWarnings("rawtypes")
  private static void parseCoordinates(StreamTokenizer stream, ArrayList lats, ArrayList lons)
      throws IOException, ParseException {
    boolean isOpenParen = false;
    if (isNumberNext(stream) || (isOpenParen = nextWord(stream).equals(LPAREN))) {
      parseCoordinate(stream, lats, lons);
    }

    while (nextCloserOrComma(stream).equals(COMMA)) {
      isOpenParen = false;
      if (isNumberNext(stream) || (isOpenParen = nextWord(stream).equals(LPAREN))) {
        parseCoordinate(stream, lats, lons);
      }
      if (isOpenParen && nextCloser(stream).equals(RPAREN) == false) {
        throw new ParseException("expected: [" + RPAREN + "] but found: [" + tokenString(stream) + "]", stream.lineno());
      }
    }

    if (isOpenParen && nextCloser(stream).equals(RPAREN) == false) {
      throw new ParseException("expected: [" + RPAREN + "] but found: [" + tokenString(stream) + "]", stream.lineno());
    }
  }

  /** parses a single coordinate, w/ optional 3rd dimension */
  @SuppressWarnings({"unchecked", "rawtypes"})
  private static void parseCoordinate(StreamTokenizer stream, ArrayList lats, ArrayList lons)
      throws IOException, ParseException {
    lons.add(nextNumber(stream));
    lats.add(nextNumber(stream));
    if (isNumberNext(stream)) {
      nextNumber(stream);
    }
  }

  /** parses a MULTIPOINT type */
  private static double[][] parseMultiPoint(StreamTokenizer stream) throws IOException, ParseException {
    String token = nextEmptyOrOpen(stream);
    if (token.equals(EMPTY)) {
      return null;
    }
    ArrayList<Double> lats = new ArrayList<>();
    ArrayList<Double> lons = new ArrayList<>();
    parseCoordinates(stream, lats, lons);
    double[][] result = new double[lats.size()][2];
    for (int i = 0; i < lats.size(); ++i) {
      result[i] = new double[] {lons.get(i), lats.get(i)};
    }
    return result;
  }

  /** parses a LINESTRING */
  private static Line parseLine(StreamTokenizer stream) throws IOException, ParseException {
    String token = nextEmptyOrOpen(stream);
    if (token.equals(EMPTY)) {
      return null;
    }
    ArrayList<Double> lats = new ArrayList<>();
    ArrayList<Double> lons = new ArrayList<>();
    parseCoordinates(stream, lats, lons);
    return new Line(lats.stream().mapToDouble(i->i).toArray(), lons.stream().mapToDouble(i->i).toArray());
  }

  /** parses a MULTILINESTRING */
  private static Line[] parseMultiLine(StreamTokenizer stream) throws IOException, ParseException {
    String token = nextEmptyOrOpen(stream);
    if (token.equals(EMPTY)) {
      return null;
    }
    ArrayList<Line> lines = new ArrayList<>();
    lines.add(parseLine(stream));
    while (nextCloserOrComma(stream).equals(COMMA)) {
      lines.add(parseLine(stream));
    }
    return lines.toArray(new Line[lines.size()]);
  }

  /** parses the hole of a polygon */
  @SuppressWarnings({"rawtypes", "unchecked"})
  private static Polygon parsePolygonHole(StreamTokenizer stream) throws IOException, ParseException {
    ArrayList<Double> lats = new ArrayList();
    ArrayList<Double> lons = new ArrayList();
    parseCoordinates(stream, lats, lons);
    return new Polygon(lats.stream().mapToDouble(i->i).toArray(), lons.stream().mapToDouble(i->i).toArray());
  }

  /** parses a POLYGON */
  @SuppressWarnings({"rawtypes", "unchecked"})
  private static Polygon parsePolygon(StreamTokenizer stream) throws IOException, ParseException {
    if (nextEmptyOrOpen(stream).equals(EMPTY)) {
      return null;
    }
    nextOpener(stream);
    ArrayList<Double> lats = new ArrayList();
    ArrayList<Double> lons = new ArrayList();
    parseCoordinates(stream, lats, lons);
    ArrayList<Polygon> holes = new ArrayList<>();
    while (nextCloserOrComma(stream).equals(COMMA)) {
      holes.add(parsePolygonHole(stream));
    }

    if (holes.isEmpty() == false) {
      return new Polygon(lats.stream().mapToDouble(i->i).toArray(), lons.stream().mapToDouble(i->i).toArray(), holes.toArray(new Polygon[holes.size()]));
    }
    return new Polygon(lats.stream().mapToDouble(i->i).toArray(), lons.stream().mapToDouble(i->i).toArray());
  }

  /** parses a MULTIPOLYGON */
  private static Polygon[] parseMultiPolygon(StreamTokenizer stream) throws IOException, ParseException {
    String token = nextEmptyOrOpen(stream);
    if (token.equals(EMPTY)) {
      return null;
    }
    ArrayList<Polygon> polygons = new ArrayList<>();
    polygons.add(parsePolygon(stream));
    while (nextCloserOrComma(stream).equals(COMMA)) {
      polygons.add(parsePolygon(stream));
    }
    return polygons.toArray(new Polygon[polygons.size()]);
  }

  /** parses an ENVELOPE */
  private static Rectangle parseBBox(StreamTokenizer stream) throws IOException, ParseException {
    if (nextEmptyOrOpen(stream).equals(EMPTY)) {
      return null;
    }
    double minLon = nextNumber(stream);
    nextComma(stream);
    double maxLon = nextNumber(stream);
    nextComma(stream);
    double maxLat = nextNumber(stream);
    nextComma(stream);
    double minLat = nextNumber(stream);
    nextCloser(stream);
    return new Rectangle(minLat, maxLat, minLon, maxLon);
  }

  /** parses a GEOMETRYCOLLECTION */
  private static Object[] parseGeometryCollection(StreamTokenizer stream) throws IOException, ParseException {
    if (nextEmptyOrOpen(stream).equals(EMPTY)) {
      return null;
    }
    ArrayList<Object> geometries = new ArrayList<>();
    geometries.add(parseGeometry(stream, ShapeType.GEOMETRYCOLLECTION));
    while (nextCloserOrComma(stream).equals(COMMA)) {
      geometries.add(parseGeometry(stream, null));
    }
    return geometries.toArray(new Object[geometries.size()]);
  }

  /** next word in the stream */
  private static String nextWord(StreamTokenizer stream) throws ParseException, IOException {
    switch (stream.nextToken()) {
      case StreamTokenizer.TT_WORD:
        final String word = stream.sval;
        return word.equalsIgnoreCase(EMPTY) ? EMPTY : word;
      case '(': return LPAREN;
      case ')': return RPAREN;
      case ',': return COMMA;
    }
    throw new ParseException("expected word but found: " + tokenString(stream), stream.lineno());
  }

  /** next number in the stream */
  private static double nextNumber(StreamTokenizer stream) throws IOException, ParseException {
    if (stream.nextToken() == StreamTokenizer.TT_WORD) {
      if (stream.sval.equalsIgnoreCase(NAN)) {
        return Double.NaN;
      } else {
        try {
          return Double.parseDouble(stream.sval);
        } catch (NumberFormatException e) {
          throw new ParseException("invalid number found: " + stream.sval, stream.lineno());
        }
      }
    }
    throw new ParseException("expected number but found: " + tokenString(stream), stream.lineno());
  }

  /** next token in the stream */
  private static String tokenString(StreamTokenizer stream) {
    switch (stream.ttype) {
      case StreamTokenizer.TT_WORD: return stream.sval;
      case StreamTokenizer.TT_EOF: return EOF;
      case StreamTokenizer.TT_EOL: return EOL;
      case StreamTokenizer.TT_NUMBER: return NUMBER;
    }
    return "'" + (char)stream.ttype + "'";
  }

  /** checks if the next token is a number */
  private static boolean isNumberNext(StreamTokenizer stream) throws IOException {
    final int type = stream.nextToken();
    stream.pushBack();
    return type == StreamTokenizer.TT_WORD;
  }

  /** checks if next token is an EMPTY or open paren */
  private static String nextEmptyOrOpen(StreamTokenizer stream) throws IOException, ParseException {
    final String next = nextWord(stream);
    if (next.equals(EMPTY) || next.equals(LPAREN)) {
      return next;
    }
    throw new ParseException("expected " + EMPTY + " or " + LPAREN
        + " but found: " + tokenString(stream), stream.lineno());
  }

  /** checks if next token is a closing paren */
  private static String nextCloser(StreamTokenizer stream) throws IOException, ParseException {
    if (nextWord(stream).equals(RPAREN)) {
      return RPAREN;
    }
    throw new ParseException("expected " + RPAREN + " but found: " + tokenString(stream), stream.lineno());
  }

  /** expects a comma as next token */
  private static String nextComma(StreamTokenizer stream) throws IOException, ParseException {
    if (nextWord(stream).equals(COMMA) == true) {
      return COMMA;
    }
    throw new ParseException("expected " + COMMA + " but found: " + tokenString(stream), stream.lineno());
  }

  /** expects an open RPAREN as the next toke */
  private static String nextOpener(StreamTokenizer stream) throws IOException, ParseException {
    if (nextWord(stream).equals(LPAREN)) {
      return LPAREN;
    }
    throw new ParseException("expected " + LPAREN + " but found: " + tokenString(stream), stream.lineno());
  }

  /** expects either a closing LPAREN or comma as the next token */
  private static String nextCloserOrComma(StreamTokenizer stream) throws IOException, ParseException {
    String token = nextWord(stream);
    if (token.equals(COMMA) || token.equals(RPAREN)) {
      return token;
    }
    throw new ParseException("expected " + COMMA + " or " + RPAREN
        + " but found: " + tokenString(stream), stream.lineno());
  }

  /** next word in the stream */
  private static void checkEOF(StreamTokenizer stream) throws ParseException, IOException {
    if (stream.nextToken() != StreamTokenizer.TT_EOF) {
      throw new ParseException("expected end of WKT string but found additional text: "
          + tokenString(stream), stream.lineno());
    }
  }

  /** Enumerated type for Shapes */
  public enum ShapeType {
    POINT("point"),
    MULTIPOINT("multipoint"),
    LINESTRING("linestring"),
    MULTILINESTRING("multilinestring"),
    POLYGON("polygon"),
    MULTIPOLYGON("multipolygon"),
    GEOMETRYCOLLECTION("geometrycollection"),
    ENVELOPE("envelope"); // not part of the actual WKB spec

    private final String shapeName;
    private static Map<String, ShapeType> shapeTypeMap = new HashMap<>();
    private static final String BBOX = "BBOX";

    static {
      for (ShapeType type : values()) {
        shapeTypeMap.put(type.shapeName, type);
      }
      shapeTypeMap.put(ENVELOPE.wktName().toLowerCase(Locale.ROOT), ENVELOPE);
    }

    ShapeType(String shapeName) {
      this.shapeName = shapeName;
    }

    protected String typename() {
      return shapeName;
    }

    /** wkt shape name */
    public String wktName() {
      return this == ENVELOPE ? BBOX : this.shapeName;
    }

    public static ShapeType forName(String shapename) {
      String typename = shapename.toLowerCase(Locale.ROOT);
      for (ShapeType type : values()) {
        if(type.shapeName.equals(typename)) {
          return type;
        }
      }
      throw new IllegalArgumentException("unknown geo_shape ["+shapename+"]");
    }
  }
}
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
package org.apache.lucene.index;

import java.io.IOException;
import java.math.BigInteger;
import java.net.InetAddress;

import org.apache.lucene.document.BinaryPoint;
import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.FloatPoint;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.util.bkd.BKDWriter;

/** 
 * Access to indexed numeric values.
 * <p>
 * Points represent numeric values and are indexed differently than ordinary text. Instead of an inverted index, 
 * points are indexed with datastructures such as <a href="https://en.wikipedia.org/wiki/K-d_tree">KD-trees</a>. 
 * These structures are optimized for operations such as <i>range</i>, <i>distance</i>, <i>nearest-neighbor</i>, 
 * and <i>point-in-polygon</i> queries. 
 * <h1>Basic Point Types</h1>
 * <table summary="Basic point types in Java and Lucene">
 *   <tr><th>Java type</th><th>Lucene class</th></tr>
 *   <tr><td>{@code int}</td><td>{@link IntPoint}</td></tr>
 *   <tr><td>{@code long}</td><td>{@link LongPoint}</td></tr>
 *   <tr><td>{@code float}</td><td>{@link FloatPoint}</td></tr>
 *   <tr><td>{@code double}</td><td>{@link DoublePoint}</td></tr>
 *   <tr><td>{@code byte[]}</td><td>{@link BinaryPoint}</td></tr>
 *   <tr><td>{@link BigInteger}</td><td><a href="{@docRoot}/../sandbox/org/apache/lucene/document/BigIntegerPoint.html">BigIntegerPoint</a>*</td></tr>
 *   <tr><td>{@link InetAddress}</td><td><a href="{@docRoot}/../sandbox/org/apache/lucene/document/InetAddressPoint.html">InetAddressPoint</a>*</td></tr>
 * </table>
 * * in the <i>lucene-sandbox</i> jar<br>
 * <p>
 * Basic Lucene point types behave like their java peers: for example {@link IntPoint} represents a signed 32-bit 
 * {@link Integer}, supporting values ranging from {@link Integer#MIN_VALUE} to {@link Integer#MAX_VALUE}, ordered
 * consistent with {@link Integer#compareTo(Integer)}. In addition to indexing support, point classes also contain 
 * static methods (such as {@link IntPoint#newRangeQuery(String, int, int)}) for creating common queries. For example:
 * <pre class="prettyprint">
 *   // add year 1970 to document
 *   document.add(new IntPoint("year", 1970));
 *   // index document
 *   writer.addDocument(document);
 *   ...
 *   // issue range query of 1960-1980
 *   Query query = IntPoint.newRangeQuery("year", 1960, 1980);
 *   TopDocs docs = searcher.search(query, ...);
 * </pre>
 * <h1>Geospatial Point Types</h1>
 * Although basic point types such as {@link DoublePoint} support points in multi-dimensional space too, Lucene has
 * specialized classes for location data. These classes are optimized for location data: they are more space-efficient and 
 * support special operations such as <i>distance</i> and <i>polygon</i> queries. There are currently two implementations:
 * <br>
 * <ol>
 *   <li><a href="{@docRoot}/../sandbox/org/apache/lucene/document/LatLonPoint.html">LatLonPoint</a> in <i>lucene-sandbox</i>: indexes {@code (latitude,longitude)} as {@code (x,y)} in two-dimensional space.
 *   <li><a href="{@docRoot}/../spatial3d/org/apache/lucene/spatial3d/Geo3DPoint.html">Geo3DPoint</a>* in <i>lucene-spatial3d</i>: indexes {@code (latitude,longitude)} as {@code (x,y,z)} in three-dimensional space.
 * </ol>
 * * does <b>not</b> support altitude, 3D here means "uses three dimensions under-the-hood"<br>
 * <h1>Advanced usage</h1>
 * Custom structures can be created on top of single- or multi- dimensional basic types, on top of 
 * {@link BinaryPoint} for more flexibility, or via custom {@link Field} subclasses.
 *
 *  @lucene.experimental */
public abstract class PointValues {

  /** Maximum number of bytes for each dimension */
  public static final int MAX_NUM_BYTES = 16;

  /** Maximum number of dimensions */
  public static final int MAX_DIMENSIONS = BKDWriter.MAX_DIMS;

  /** Default constructor */
  protected PointValues() {
  }

  /** Used by {@link #intersect} to check how each recursive cell corresponds to the query. */
  public enum Relation {
    /** Return this if the cell is fully contained by the query */
    CELL_INSIDE_QUERY,
    /** Return this if the cell and query do not overlap */
    CELL_OUTSIDE_QUERY,
    /** Return this if the cell partially overlaps the query */
    CELL_CROSSES_QUERY
  };

  /** We recurse the BKD tree, using a provided instance of this to guide the recursion.
   *
   * @lucene.experimental */
  public interface IntersectVisitor {
    /** Called for all documents in a leaf cell that's fully contained by the query.  The
     *  consumer should blindly accept the docID. */
    void visit(int docID) throws IOException;

    /** Called for all documents in a leaf cell that crosses the query.  The consumer
     *  should scrutinize the packedValue to decide whether to accept it.  In the 1D case,
     *  values are visited in increasing order, and in the case of ties, in increasing
     *  docID order. */
    void visit(int docID, byte[] packedValue) throws IOException;

    /** Called for non-leaf cells to test how the cell relates to the query, to
     *  determine how to further recurse down the tree. */
    Relation compare(byte[] minPackedValue, byte[] maxPackedValue);

    /** Notifies the caller that this many documents (from one block) are about
     *  to be visited */
    default void grow(int count) {};
  }

  /** Finds all documents and points matching the provided visitor.
   *  This method does not enforce live documents, so it's up to the caller
   *  to test whether each document is deleted, if necessary. */
  public abstract void intersect(String fieldName, IntersectVisitor visitor) throws IOException;

  /** Returns minimum value for each dimension, packed, or null if {@link #size} is <code>0</code> */
  public abstract byte[] getMinPackedValue(String fieldName) throws IOException;

  /** Returns maximum value for each dimension, packed, or null if {@link #size} is <code>0</code> */
  public abstract byte[] getMaxPackedValue(String fieldName) throws IOException;

  /** Returns how many dimensions were indexed */
  public abstract int getNumDimensions(String fieldName) throws IOException;

  /** Returns the number of bytes per dimension */
  public abstract int getBytesPerDimension(String fieldName) throws IOException;

  /** Returns the total number of indexed points across all documents in this field. */
  public abstract long size(String fieldName);

  /** Returns the total number of documents that have indexed at least one point for this field. */
  public abstract int getDocCount(String fieldName);
}

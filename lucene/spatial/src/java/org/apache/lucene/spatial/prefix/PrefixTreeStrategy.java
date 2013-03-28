package org.apache.lucene.spatial.prefix;

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

import com.spatial4j.core.shape.Point;
import com.spatial4j.core.shape.Shape;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.spatial.SpatialStrategy;
import org.apache.lucene.spatial.prefix.tree.Cell;
import org.apache.lucene.spatial.prefix.tree.SpatialPrefixTree;
import org.apache.lucene.spatial.query.SpatialArgs;
import org.apache.lucene.spatial.util.ShapeFieldCacheDistanceValueSource;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * An abstract SpatialStrategy based on {@link SpatialPrefixTree}. The two
 * subclasses are {@link RecursivePrefixTreeStrategy} and {@link
 * TermQueryPrefixTreeStrategy}.  This strategy is most effective as a fast
 * approximate spatial search filter.
 *
 * <h4>Characteristics:</h4>
 * <ul>
 * <li>Can index any shape; however only {@link RecursivePrefixTreeStrategy}
 * can effectively search non-point shapes.</li>
 * <li>Can index a variable number of shapes per field value. This strategy
 * can do it via multiple calls to {@link #createIndexableFields(com.spatial4j.core.shape.Shape)}
 * for a document or by giving it some sort of Shape aggregate (e.g. JTS
 * WKT MultiPoint).  The shape's boundary is approximated to a grid precision.
 * </li>
 * <li>Can query with any shape.  The shape's boundary is approximated to a grid
 * precision.</li>
 * <li>Only {@link org.apache.lucene.spatial.query.SpatialOperation#Intersects}
 * is supported.  If only points are indexed then this is effectively equivalent
 * to IsWithin.</li>
 * <li>The strategy supports {@link #makeDistanceValueSource(com.spatial4j.core.shape.Point)}
 * even for multi-valued data, so long as the indexed data is all points; the
 * behavior is undefined otherwise.  However, <em>it will likely be removed in
 * the future</em> in lieu of using another strategy with a more scalable
 * implementation.  Use of this call is the only
 * circumstance in which a cache is used.  The cache is simple but as such
 * it doesn't scale to large numbers of points nor is it real-time-search
 * friendly.</li>
 * </ul>
 *
 * <h4>Implementation:</h4>
 * The {@link SpatialPrefixTree} does most of the work, for example returning
 * a list of terms representing grids of various sizes for a supplied shape.
 * An important
 * configuration item is {@link #setDistErrPct(double)} which balances
 * shape precision against scalability.  See those javadocs.
 *
 * @lucene.internal
 */
public abstract class PrefixTreeStrategy extends SpatialStrategy {
  protected final SpatialPrefixTree grid;
  private final Map<String, PointPrefixTreeFieldCacheProvider> provider = new ConcurrentHashMap<String, PointPrefixTreeFieldCacheProvider>();
  protected final boolean simplifyIndexedCells;
  protected int defaultFieldValuesArrayLen = 2;
  protected double distErrPct = SpatialArgs.DEFAULT_DISTERRPCT;// [ 0 TO 0.5 ]

  public PrefixTreeStrategy(SpatialPrefixTree grid, String fieldName, boolean simplifyIndexedCells) {
    super(grid.getSpatialContext(), fieldName);
    this.grid = grid;
    this.simplifyIndexedCells = simplifyIndexedCells;
  }

  /**
   * A memory hint used by {@link #makeDistanceValueSource(com.spatial4j.core.shape.Point)}
   * for how big the initial size of each Document's array should be. The
   * default is 2.  Set this to slightly more than the default expected number
   * of points per document.
   */
  public void setDefaultFieldValuesArrayLen(int defaultFieldValuesArrayLen) {
    this.defaultFieldValuesArrayLen = defaultFieldValuesArrayLen;
  }

  public double getDistErrPct() {
    return distErrPct;
  }

  /**
   * The default measure of shape precision affecting shapes at index and query
   * times. Points don't use this as they are always indexed at the configured
   * maximum precision ({@link org.apache.lucene.spatial.prefix.tree.SpatialPrefixTree#getMaxLevels()});
   * this applies to all other shapes. Specific shapes at index and query time
   * can use something different than this default value.  If you don't set a
   * default then the default is {@link SpatialArgs#DEFAULT_DISTERRPCT} --
   * 2.5%.
   *
   * @see org.apache.lucene.spatial.query.SpatialArgs#getDistErrPct()
   */
  public void setDistErrPct(double distErrPct) {
    this.distErrPct = distErrPct;
  }

  @Override
  public Field[] createIndexableFields(Shape shape) {
    double distErr = SpatialArgs.calcDistanceFromErrPct(shape, distErrPct, ctx);
    return createIndexableFields(shape, distErr);
  }

  public Field[] createIndexableFields(Shape shape, double distErr) {
    int detailLevel = grid.getLevelForDistance(distErr);
    List<Cell> cells = grid.getCells(shape, detailLevel, true, simplifyIndexedCells);//intermediates cells

    //TODO is CellTokenStream supposed to be re-used somehow? see Uwe's comments:
    //  http://code.google.com/p/lucene-spatial-playground/issues/detail?id=4

    Field field = new Field(getFieldName(),
        new CellTokenStream(cells.iterator()), FIELD_TYPE);
    return new Field[]{field};
  }

  /* Indexed, tokenized, not stored. */
  public static final FieldType FIELD_TYPE = new FieldType();

  static {
    FIELD_TYPE.setIndexed(true);
    FIELD_TYPE.setTokenized(true);
    FIELD_TYPE.setOmitNorms(true);
    FIELD_TYPE.setIndexOptions(FieldInfo.IndexOptions.DOCS_ONLY);
    FIELD_TYPE.freeze();
  }

  /** Outputs the tokenString of a cell, and if its a leaf, outputs it again with the leaf byte. */
  final static class CellTokenStream extends TokenStream {

    private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);

    private Iterator<Cell> iter = null;

    public CellTokenStream(Iterator<Cell> tokens) {
      this.iter = tokens;
    }

    CharSequence nextTokenStringNeedingLeaf = null;

    @Override
    public boolean incrementToken() {
      clearAttributes();
      if (nextTokenStringNeedingLeaf != null) {
        termAtt.append(nextTokenStringNeedingLeaf);
        termAtt.append((char) Cell.LEAF_BYTE);
        nextTokenStringNeedingLeaf = null;
        return true;
      }
      if (iter.hasNext()) {
        Cell cell = iter.next();
        CharSequence token = cell.getTokenString();
        termAtt.append(token);
        if (cell.isLeaf())
          nextTokenStringNeedingLeaf = token;
        return true;
      }
      return false;
    }

  }

  @Override
  public ValueSource makeDistanceValueSource(Point queryPoint) {
    PointPrefixTreeFieldCacheProvider p = provider.get( getFieldName() );
    if( p == null ) {
      synchronized (this) {//double checked locking idiom is okay since provider is threadsafe
        p = provider.get( getFieldName() );
        if (p == null) {
          p = new PointPrefixTreeFieldCacheProvider(grid, getFieldName(), defaultFieldValuesArrayLen);
          provider.put(getFieldName(),p);
        }
      }
    }

    return new ShapeFieldCacheDistanceValueSource(ctx, p, queryPoint);
  }

  public SpatialPrefixTree getGrid() {
    return grid;
  }
}

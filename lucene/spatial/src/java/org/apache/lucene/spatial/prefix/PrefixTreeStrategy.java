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

import com.spatial4j.core.distance.DistanceCalculator;
import com.spatial4j.core.shape.Point;
import com.spatial4j.core.shape.Shape;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.spatial.SimpleSpatialFieldInfo;
import org.apache.lucene.spatial.SpatialStrategy;
import org.apache.lucene.spatial.prefix.tree.Node;
import org.apache.lucene.spatial.prefix.tree.SpatialPrefixTree;
import org.apache.lucene.spatial.query.SpatialArgs;
import org.apache.lucene.spatial.util.CachedDistanceValueSource;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @lucene.internal
 */
public abstract class PrefixTreeStrategy extends SpatialStrategy<SimpleSpatialFieldInfo> {
  protected final SpatialPrefixTree grid;
  private final Map<String, PointPrefixTreeFieldCacheProvider> provider = new ConcurrentHashMap<String, PointPrefixTreeFieldCacheProvider>();
  protected int defaultFieldValuesArrayLen = 2;
  protected double distErrPct = SpatialArgs.DEFAULT_DIST_PRECISION;

  public PrefixTreeStrategy(SpatialPrefixTree grid) {
    super(grid.getSpatialContext());
    this.grid = grid;
  }

  /** Used in the in-memory ValueSource as a default ArrayList length for this field's array of values, per doc. */
  public void setDefaultFieldValuesArrayLen(int defaultFieldValuesArrayLen) {
    this.defaultFieldValuesArrayLen = defaultFieldValuesArrayLen;
  }

  /** See {@link SpatialPrefixTree#getMaxLevelForPrecision(com.spatial4j.core.shape.Shape, double)}. */
  public void setDistErrPct(double distErrPct) {
    this.distErrPct = distErrPct;
  }

  @Override
  public IndexableField createField(SimpleSpatialFieldInfo fieldInfo, Shape shape, boolean index, boolean store) {
    int detailLevel = grid.getMaxLevelForPrecision(shape,distErrPct);
    List<Node> cells = grid.getNodes(shape, detailLevel, true);//true=intermediates cells
    //If shape isn't a point, add a full-resolution center-point so that
    // PrefixFieldCacheProvider has the center-points.
    // TODO index each center of a multi-point? Yes/no?
    if (!(shape instanceof Point)) {
      Point ctr = shape.getCenter();
      //TODO should be smarter; don't index 2 tokens for this in CellTokenizer. Harmless though.
      cells.add(grid.getNodes(ctr,grid.getMaxLevels(),false).get(0));
    }

    //TODO is CellTokenStream supposed to be re-used somehow? see Uwe's comments:
    //  http://code.google.com/p/lucene-spatial-playground/issues/detail?id=4

    String fname = fieldInfo.getFieldName();
    if( store ) {
      //TODO figure out how to re-use original string instead of reconstituting it.
      String wkt = grid.getSpatialContext().toString(shape);
      if( index ) {
        Field f = new Field(fname,wkt,TYPE_STORED);
        f.setTokenStream(new CellTokenStream(cells.iterator()));
        return f;
      }
      return new StoredField(fname,wkt);
    }
    
    if( index ) {
      return new Field(fname,new CellTokenStream(cells.iterator()),TYPE_NOT_STORED);
    }
    
    throw new UnsupportedOperationException("Fields need to be indexed or store ["+fname+"]");
  }

  /* Indexed, tokenized, not stored. */
  public static final FieldType TYPE_NOT_STORED = new FieldType();

  /* Indexed, tokenized, stored. */
  public static final FieldType TYPE_STORED = new FieldType();

  static {
    TYPE_NOT_STORED.setIndexed(true);
    TYPE_NOT_STORED.setTokenized(true);
    TYPE_NOT_STORED.setOmitNorms(true);
    TYPE_NOT_STORED.freeze();

    TYPE_STORED.setStored(true);
    TYPE_STORED.setIndexed(true);
    TYPE_STORED.setTokenized(true);
    TYPE_STORED.setOmitNorms(true);
    TYPE_STORED.freeze();
  }

  /** Outputs the tokenString of a cell, and if its a leaf, outputs it again with the leaf byte. */
  final static class CellTokenStream extends TokenStream {

    private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);

    private Iterator<Node> iter = null;

    public CellTokenStream(Iterator<Node> tokens) {
      this.iter = tokens;
    }

    CharSequence nextTokenStringNeedingLeaf = null;

    @Override
    public boolean incrementToken() {
      clearAttributes();
      if (nextTokenStringNeedingLeaf != null) {
        termAtt.append(nextTokenStringNeedingLeaf);
        termAtt.append((char) Node.LEAF_BYTE);
        nextTokenStringNeedingLeaf = null;
        return true;
      }
      if (iter.hasNext()) {
        Node cell = iter.next();
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
  public ValueSource makeValueSource(SpatialArgs args, SimpleSpatialFieldInfo fieldInfo) {
    DistanceCalculator calc = grid.getSpatialContext().getDistCalc();
    return makeValueSource(args, fieldInfo, calc);
  }
  
  public ValueSource makeValueSource(SpatialArgs args, SimpleSpatialFieldInfo fieldInfo, DistanceCalculator calc) {
    PointPrefixTreeFieldCacheProvider p = provider.get( fieldInfo.getFieldName() );
    if( p == null ) {
      synchronized (this) {//double checked locking idiom is okay since provider is threadsafe
        p = provider.get( fieldInfo.getFieldName() );
        if (p == null) {
          p = new PointPrefixTreeFieldCacheProvider(grid, fieldInfo.getFieldName(), defaultFieldValuesArrayLen);
          provider.put(fieldInfo.getFieldName(),p);
        }
      }
    }
    Point point = args.getShape().getCenter();
    return new CachedDistanceValueSource(point, calc, p);
  }

  public SpatialPrefixTree getGrid() {
    return grid;
  }
}

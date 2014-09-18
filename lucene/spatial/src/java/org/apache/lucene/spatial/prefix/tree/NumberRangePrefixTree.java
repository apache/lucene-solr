package org.apache.lucene.spatial.prefix.tree;

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
import com.spatial4j.core.context.SpatialContextFactory;
import com.spatial4j.core.shape.Point;
import com.spatial4j.core.shape.Rectangle;
import com.spatial4j.core.shape.Shape;
import com.spatial4j.core.shape.SpatialRelation;
import com.spatial4j.core.shape.impl.RectangleImpl;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.StringHelper;

import java.text.ParseException;

/**
 * A special SpatialPrefixTree for single-dimensional number ranges of integral values. It's based
 * on a stack of integers, and thus it's not limited to a long.
 * @see <a href="https://issues.apache.org/jira/browse/LUCENE-5648">LUCENE-5648</a>
 * @lucene.experimental
 */
public abstract class NumberRangePrefixTree extends SpatialPrefixTree {

  //
  //    Dummy SpatialContext
  //

  private static final SpatialContext DUMMY_CTX;
  static {
    SpatialContextFactory factory = new SpatialContextFactory();
    factory.geo = false;
    factory.worldBounds = new RectangleImpl(Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY, 0L, 0L, null);
    DUMMY_CTX = factory.newSpatialContext();
  }

  //
  //    LevelledValue
  //

  /** A value implemented as a stack of numbers. Spatially speaking, it's
   * analogous to a Point but 1D yet has some precision width.
   * @lucene.internal */
  protected static interface LevelledValue extends Shape {
    int getLevel();//0 means the world (universe).
    int getValAtLevel(int level);//level >= 0 && <= getLevel()
    LevelledValue getLVAtLevel(int level);
  }

  /** Compares a to b, returning less than 0, 0, or greater than 0, if a is less than, equal to, or
   * greater than b, respectively. Only min(a.levels,b.levels) are compared.
   * @lucene.internal */
  protected static int comparePrefixLV(LevelledValue a, LevelledValue b) {
    int minLevel = Math.min(a.getLevel(), b.getLevel());
    for (int level = 1; level <= minLevel; level++) {
      int diff = a.getValAtLevel(level) - b.getValAtLevel(level);
      if (diff != 0)
        return diff;
    }
    return 0;
  }

  protected String toStringLV(LevelledValue lv) {
    StringBuilder buf = new StringBuilder();
    buf.append('[');
    for (int level = 1; level <= lv.getLevel(); level++) {
      buf.append(lv.getValAtLevel(level)).append(',');
    }
    buf.setLength(buf.length()-1);//chop off ','
    buf.append(']');
    return buf.toString();
  }

  //
  //    NRShape
  //

  /** Number Range Shape; based on a pair of {@link LevelledValue}.
   * Spatially speaking, it's analogous to a Rectangle but 1D.
   * @lucene.internal */
  protected class NRShape implements Shape {

    private final LevelledValue minLV, maxLV;
    private final int lastLevelInCommon;//computed; not part of identity

    /** Don't call directly; see {@link #toRangeShape(com.spatial4j.core.shape.Shape, com.spatial4j.core.shape.Shape)}. */
    private NRShape(LevelledValue minLV, LevelledValue maxLV) {
      this.minLV = minLV;
      this.maxLV = maxLV;

      //calc lastLevelInCommon
      int level = 1;
      for (; level <= minLV.getLevel() && level <= maxLV.getLevel(); level++) {
        if (minLV.getValAtLevel(level) != maxLV.getValAtLevel(level))
          break;
      }
      lastLevelInCommon = level - 1;
    }

    public LevelledValue getMinLV() { return minLV; }

    public LevelledValue getMaxLV() { return maxLV; }

    /** How many levels are in common between minLV and maxLV. */
    private int getLastLevelInCommon() { return lastLevelInCommon; }

    @Override
    public SpatialRelation relate(Shape shape) {
//      if (shape instanceof LevelledValue)
//        return relate((LevelledValue)shape);
      if (shape instanceof NRShape)
        return relate((NRShape) shape);
      return shape.relate(this).transpose();//probably a LevelledValue
    }

    public SpatialRelation relate(NRShape ext) {
      //This logic somewhat mirrors RectangleImpl.relate_range()
      int extMin_intMax = comparePrefixLV(ext.getMinLV(), getMaxLV());
      if (extMin_intMax > 0)
        return SpatialRelation.DISJOINT;
      int extMax_intMin = comparePrefixLV(ext.getMaxLV(), getMinLV());
      if (extMax_intMin < 0)
        return SpatialRelation.DISJOINT;
      int extMin_intMin = comparePrefixLV(ext.getMinLV(), getMinLV());
      int extMax_intMax = comparePrefixLV(ext.getMaxLV(), getMaxLV());
      if ((extMin_intMin > 0 || extMin_intMin == 0 && ext.getMinLV().getLevel() >= getMinLV().getLevel())
          && (extMax_intMax < 0 || extMax_intMax == 0 && ext.getMaxLV().getLevel() >= getMaxLV().getLevel()))
        return SpatialRelation.CONTAINS;
      if ((extMin_intMin < 0 || extMin_intMin == 0 && ext.getMinLV().getLevel() <= getMinLV().getLevel())
          && (extMax_intMax > 0 || extMax_intMax == 0 && ext.getMaxLV().getLevel() <= getMaxLV().getLevel()))
        return SpatialRelation.WITHIN;
      return SpatialRelation.INTERSECTS;
    }

    @Override
    public Rectangle getBoundingBox() { throw new UnsupportedOperationException(); }

    @Override
    public boolean hasArea() { return true; }

    @Override
    public double getArea(SpatialContext spatialContext) { throw new UnsupportedOperationException(); }

    @Override
    public Point getCenter() { throw new UnsupportedOperationException(); }

    @Override
    public Shape getBuffered(double v, SpatialContext spatialContext) { throw new UnsupportedOperationException(); }

    @Override
    public boolean isEmpty() { return false; }

    @Override
    public String toString() { return "[" + toStringLV(minLV) + " TO " + toStringLV(maxLV) + "]"; }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      NRShape nrShape = (NRShape) o;

      if (!maxLV.equals(nrShape.maxLV)) return false;
      if (!minLV.equals(nrShape.minLV)) return false;

      return true;
    }

    @Override
    public int hashCode() {
      int result = minLV.hashCode();
      result = 31 * result + maxLV.hashCode();
      return result;
    }
  }// class NRShapeImpl

  /** Converts the value to a shape (usually not a range). If it's a JDK object (e.g. Number, Calendar)
   * that could be parsed from a String, this class won't do it; you must parse it. */
  public abstract Shape toShape(Object value);

  /** Detects a range pattern and parses it, otherwise it's parsed as one shape via
   * {@link #parseShapeLV(String)}.  The range pattern looks like this BNF:
   * <pre>
   *   '[' + parseShapeLV + ' TO ' + parseShapeLV + ']'
   * </pre>
   * It's the same thing as the toString() of the range shape, notwithstanding range optimization.
   * @param str not null or empty
   * @return not null
   * @throws java.text.ParseException If there is a problem
   */
  public Shape parseShape(String str) throws ParseException {
    if (str == null || str.isEmpty())
      throw new IllegalArgumentException("str is null or blank");
    if (str.charAt(0) == '[') {
      if (str.charAt(str.length()-1) != ']')
        throw new ParseException("If starts with [ must end with ]; got "+str, str.length()-1);
      int middle = str.indexOf(" TO ");
      if (middle < 0)
        throw new ParseException("If starts with [ must contain ' TO '; got "+str, -1);
      String leftStr = str.substring(1, middle);
      String rightStr = str.substring(middle + " TO ".length(), str.length()-1);
      return toRangeShape(parseShapeLV(leftStr), parseShapeLV(rightStr));
    } else if (str.charAt(0) == '{') {
      throw new ParseException("Exclusive ranges not supported; got "+str, 0);
    } else {
      return parseShapeLV(str);
    }
  }

  /** Parse a String to a LevelledValue. "*" should be the full-range. */
  protected abstract LevelledValue parseShapeLV(String str) throws ParseException;

  /** Returns a shape that represents the continuous range between {@code start} and {@code end}. It will
   * be optimized.
   * @throws IllegalArgumentException if the arguments are in the wrong order, or if either contains the other.
   */
  public Shape toRangeShape(Shape start, Shape end) {
    if (!(start instanceof LevelledValue && end instanceof LevelledValue))
      throw new IllegalArgumentException("Must pass "+LevelledValue.class+" but got "+start.getClass());
    LevelledValue minLV = (LevelledValue) start;
    LevelledValue maxLV = (LevelledValue) end;
    if (minLV.equals(maxLV))
      return minLV;
    //Optimize precision of the range, e.g. April 1st to April 30th is April.
    minLV = minLV.getLVAtLevel(truncateStartVals(minLV, 0));
    maxLV = maxLV.getLVAtLevel(truncateEndVals(maxLV, 0));
    int cmp = comparePrefixLV(minLV, maxLV);
    if (cmp > 0) {
      throw new IllegalArgumentException("Wrong order: "+start+" TO "+end);
    }
    if (cmp == 0 && minLV.getLevel() == maxLV.getLevel())
      return minLV;
    return new NRShape(minLV, maxLV);
  }

  /** From lv.getLevel on up, it returns the first Level seen with val != 0. It doesn't check past endLevel. */
  private int truncateStartVals(LevelledValue lv, int endLevel) {
    for (int level = lv.getLevel(); level > endLevel; level--) {
      if (lv.getValAtLevel(level) != 0)
        return level;
    }
    return endLevel;
  }

  private int truncateEndVals(LevelledValue lv, int endLevel) {
    for (int level = lv.getLevel(); level > endLevel; level--) {
      int max = getNumSubCells(lv.getLVAtLevel(level-1)) - 1;
      if (lv.getValAtLevel(level) != max)
        return level;
    }
    return endLevel;
  }

  //
  //    NumberRangePrefixTree
  //

  protected final int[] maxSubCellsByLevel;
  protected final int[] termLenByLevel;
  protected final int[] levelByTermLen;
  protected final int maxTermLen; // how long could cell.getToken... (that is a leaf) possibly be?

  protected NumberRangePrefixTree(int[] maxSubCellsByLevel) {
    super(DUMMY_CTX, maxSubCellsByLevel.length);
    this.maxSubCellsByLevel = maxSubCellsByLevel;

    // Fill termLenByLevel
    this.termLenByLevel = new int[maxLevels + 1];
    termLenByLevel[0] = 0;
    final int MAX_STATES = 1 << 15;//1 bit less than 2 bytes
    for (int level = 1; level <= maxLevels; level++) {
      final int states = maxSubCellsByLevel[level - 1];
      if (states >= MAX_STATES || states <= 1) {
        throw new IllegalArgumentException("Max states is "+MAX_STATES+", given "+states+" at level "+level);
      }
      boolean twoBytes = states >= 256;
      termLenByLevel[level] = termLenByLevel[level-1] + (twoBytes ? 2 : 1);
    }
    maxTermLen = termLenByLevel[maxLevels] + 1;// + 1 for leaf byte

    // Fill levelByTermLen
    levelByTermLen = new int[maxTermLen];
    levelByTermLen[0] = 0;
    for (int level = 1; level < termLenByLevel.length; level++) {
      int termLen = termLenByLevel[level];
      int prevTermLen = termLenByLevel[level-1];
      if (termLen - prevTermLen == 2) {//2 byte delta
        //if the term doesn't completely cover this cell then it must be a leaf of the prior.
        levelByTermLen[termLen-1] = -1;//won't be used; otherwise erroneous
        levelByTermLen[termLen] = level;
      } else {//1 byte delta
        assert termLen - prevTermLen == 1;
        levelByTermLen[termLen] = level;
      }
    }

  }

  @Override
  public String toString() {
    return getClass().getSimpleName();
  }

  @Override
  public int getLevelForDistance(double dist) {
    return maxLevels;
  }

  @Override
  public double getDistanceForLevel(int level) {
    throw new UnsupportedOperationException("Not applicable.");
  }

  protected Shape toShape(int[] valStack, int len) {
    final NRCell[] cellStack = newCellStack(len);
    for (int i = 0; i < len; i++) {
      cellStack[i+1].resetCellWithCellNum(valStack[i]);
    }
    return cellStack[len];
  }

  @Override
  public Cell getWorldCell() {
    return newCellStack(maxLevels)[0];
  }

  protected NRCell[] newCellStack(int levels) {
    final NRCell[] cellsByLevel = new NRCell[levels + 1];
    final BytesRef term = new BytesRef(maxTermLen);
    for (int level = 0; level <= levels; level++) {
      cellsByLevel[level] = new NRCell(cellsByLevel,term,level);
    }
    return cellsByLevel;
  }

  @Override
  public Cell readCell(BytesRef term, Cell scratch) {
    if (scratch == null)
      scratch = getWorldCell();

    //We decode level, leaf, and populate bytes.

    //reverse lookup term length to the level and hence the cell
    NRCell[] cellsByLevel = ((NRCell) scratch).cellsByLevel;
    boolean isLeaf = term.bytes[term.offset + term.length - 1] == 0;
    int lenNoLeaf = isLeaf ? term.length - 1 : term.length;

    NRCell result = cellsByLevel[levelByTermLen[lenNoLeaf]];
    if (cellsByLevel[0].termBuf == null)
      cellsByLevel[0].termBuf = result.term.bytes;//a kluge; see cell.ensureOwnTermBytes()
    result.term.bytes = term.bytes;
    result.term.offset = term.offset;
    result.term.length = lenNoLeaf;//technically this isn't used but may help debugging
    result.reset();
    if (isLeaf)
      result.setLeaf();

    result.cellNumber = -1;//lazy decode flag

    return result;
  }

  protected int getNumSubCells(LevelledValue lv) {
    return maxSubCellsByLevel[lv.getLevel()];
  }

  //
  //    NRCell
  //

  /** Most of the PrefixTree implementation is in this one class, which is both
   * the Cell, the CellIterator, and the Shape to reduce object allocation. It's implemented as a re-used array/stack
   * of Cells at adjacent levels, that all have a reference back to the cell array to traverse. They also share a common
   * BytesRef for the term.
   * @lucene.internal */
  protected class NRCell extends CellIterator implements Cell, LevelledValue {

    //Shared: (TODO put this in a new class)
    final NRCell[] cellsByLevel;
    final BytesRef term;//AKA the token
    byte[] termBuf;// see ensureOwnTermBytes(), only for cell0

    //Cell state...
    final int cellLevel; // assert levelStack[cellLevel] == this
    int cellNumber; //relative to parent cell. It's unused for level 0. Starts at 0.

    SpatialRelation cellShapeRel;
    boolean cellIsLeaf;

    //CellIterator state is defined further below

    NRCell(NRCell[] cellsByLevel, BytesRef term, int cellLevel) {
      this.cellsByLevel = cellsByLevel;
      this.term = term;
      this.cellLevel = cellLevel;
      this.cellNumber = cellLevel == 0 ? 0 : -1;
      this.cellIsLeaf = false;
      assert cellsByLevel[cellLevel] == null;
    }

    /** Ensure we own term.bytes so that it's safe to modify. We detect via a kluge in which cellsByLevel[0].termBuf
     * is non-null, which is a pre-allocated for use to replace term.bytes. */
    void ensureOwnTermBytes() {
      NRCell cell0 = cellsByLevel[0];
      if (cell0.termBuf == null)
        return;//we already own the bytes
      System.arraycopy(term.bytes, term.offset, cell0.termBuf, 0, term.length);
      term.bytes = cell0.termBuf;
      term.offset = 0;
      cell0.termBuf = null;
    }

    private void reset() {
      this.cellIsLeaf = false;
      this.cellShapeRel = null;
    }

    private void resetCellWithCellNum(int cellNumber) {
      reset();

      //update bytes
      //  note: see lazyInitCellNumsFromBytes() for the reverse
      if (cellNumber >= 0) {//valid
        ensureOwnTermBytes();
        int termLen = termLenByLevel[getLevel()];
        boolean twoBytes = (termLen - termLenByLevel[getLevel()-1]) > 1;
        if (twoBytes) {
          //right 7 bits, plus 1 (may overflow to 8th bit which is okay)
          term.bytes[termLen-2] = (byte) (cellNumber >> 7);
          term.bytes[termLen-1] = (byte) ((cellNumber & 0x7F) + 1);
        } else {
          term.bytes[termLen-1] = (byte) (cellNumber+1);
        }
        assert term.bytes[termLen-1] != 0;
        term.length = termLen;
      }
      this.cellNumber = cellNumber;
    }

    private void ensureDecoded() {
      if (cellNumber >= 0)
        return;
      //Decode cell numbers from bytes. This is the inverse of resetCellWithCellNum().
      for (int level = 1; level <= getLevel(); level++) {
        NRCell cell = cellsByLevel[level];
        int termLen = termLenByLevel[level];
        boolean twoBytes = (termLen - termLenByLevel[level-1]) > 1;
        if (twoBytes) {
          int byteH = (term.bytes[term.offset + termLen - 2] & 0xFF);
          int byteL = (term.bytes[term.offset + termLen - 1] & 0xFF);
          assert byteL - 1 < (1<<7);
          cell.cellNumber = (byteH << 7) + (byteL-1);
          assert cell.cellNumber < 1<<15;
        } else {
          cell.cellNumber = (term.bytes[term.offset + termLen - 1] & 0xFF) - 1;
          assert cell.cellNumber < 255;
        }
        assert cell.cellNumber >= 0;
      }
    }

    @Override // for Cell & for LevelledValue
    public int getLevel() {
      return cellLevel;
    }

    @Override
    public SpatialRelation getShapeRel() {
      return cellShapeRel;
    }

    @Override
    public void setShapeRel(SpatialRelation rel) {
      cellShapeRel = rel;
    }

    @Override
    public boolean isLeaf() {
      return cellIsLeaf;
    }

    @Override
    public void setLeaf() {
      cellIsLeaf = true;
    }

    @Override
    public Shape getShape() {
      ensureDecoded(); return this;
    }

    @Override
    public BytesRef getTokenBytesNoLeaf(BytesRef result) {
      if (result == null)
        result = new BytesRef();
      result.bytes = term.bytes;
      result.offset = term.offset;
      result.length = termLenByLevel[cellLevel];
      assert result.length <= term.length;
      return result;
    }

    @Override
    public BytesRef getTokenBytesWithLeaf(BytesRef result) {
      ensureOwnTermBytes();//normally shouldn't do anything
      result = getTokenBytesNoLeaf(result);
      if (isLeaf()) {
        result.bytes[result.length++] = 0;
      }
      return result;
    }

    @Override
    public boolean isPrefixOf(Cell c) {
      NRCell otherCell = (NRCell) c;
      assert term != otherCell.term;
      //trick to re-use bytesref; provided that we re-instate it
      int myLastLen = term.length;
      term.length = termLenByLevel[getLevel()];
      int otherLastLen = otherCell.term.length;
      otherCell.term.length = termLenByLevel[otherCell.getLevel()];
      boolean answer = StringHelper.startsWith(otherCell.term, term);
      term.length = myLastLen;
      otherCell.term.length = otherLastLen;
      return answer;
    }

    @Override
    public int compareToNoLeaf(Cell fromCell) {
      final NRCell nrCell = (NRCell) fromCell;
      assert term != nrCell.term;
      //trick to re-use bytesref; provided that we re-instate it
      int myLastLen = term.length;
      int otherLastLen = nrCell.term.length;
      term.length = termLenByLevel[getLevel()];
      nrCell.term.length = termLenByLevel[nrCell.getLevel()];
      int answer = term.compareTo(nrCell.term);
      term.length = myLastLen;
      nrCell.term.length = otherLastLen;
      return answer;
    }

    @Override
    public CellIterator getNextLevelCells(Shape shapeFilter) {
      ensureDecoded();
      NRCell subCell = cellsByLevel[cellLevel + 1];
      subCell.initIter(shapeFilter);
      return subCell;
    }

    //----------- CellIterator

    Shape iterFilter;//LevelledValue or NRShape
    boolean iterFirstIsIntersects;
    boolean iterLastIsIntersects;
    int iterFirstCellNumber;
    int iterLastCellNumber;

    private void initIter(Shape filter) {
      cellNumber = -1;
      if (filter instanceof LevelledValue && ((LevelledValue) filter).getLevel() == 0)
        filter = null;//world means everything -- no filter
      iterFilter = filter;

      NRCell parent = getLVAtLevel(getLevel() - 1);

      // Initialize iter* members.

      //no filter means all subcells
      if (filter == null) {
        iterFirstCellNumber = 0;
        iterFirstIsIntersects = false;
        iterLastCellNumber = getNumSubCells(parent) - 1;
        iterLastIsIntersects = false;
        return;
      }

      final LevelledValue minLV;
      final LevelledValue maxLV;
      final int lastLevelInCommon;//between minLV & maxLV
      if (filter instanceof NRShape) {
        NRShape nrShape = (NRShape) iterFilter;
        minLV = nrShape.getMinLV();
        maxLV = nrShape.getMaxLV();
        lastLevelInCommon = nrShape.getLastLevelInCommon();
      } else {
        minLV = (LevelledValue) iterFilter;
        maxLV = minLV;
        lastLevelInCommon = minLV.getLevel();
      }

      //fast path optimization that is usually true, but never first level
      if (iterFilter == parent.iterFilter &&
          (getLevel() <= lastLevelInCommon || parent.iterFirstCellNumber != parent.iterLastCellNumber)) {
        //TODO benchmark if this optimization pays off. We avoid two comparePrefixLV calls.
        if (parent.iterFirstIsIntersects && parent.cellNumber == parent.iterFirstCellNumber
            && minLV.getLevel() >= getLevel()) {
          iterFirstCellNumber = minLV.getValAtLevel(getLevel());
          iterFirstIsIntersects = (minLV.getLevel() > getLevel());
        } else {
          iterFirstCellNumber = 0;
          iterFirstIsIntersects = false;
        }
        if (parent.iterLastIsIntersects && parent.cellNumber == parent.iterLastCellNumber
            && maxLV.getLevel() >= getLevel()) {
          iterLastCellNumber = maxLV.getValAtLevel(getLevel());
          iterLastIsIntersects = (maxLV.getLevel() > getLevel());
        } else {
          iterLastCellNumber = getNumSubCells(parent) - 1;
          iterLastIsIntersects = false;
        }
        if (iterFirstCellNumber == iterLastCellNumber) {
          if (iterLastIsIntersects)
            iterFirstIsIntersects = true;
          else if (iterFirstIsIntersects)
            iterLastIsIntersects = true;
        }
        return;
      }

      //not common to get here, except for level 1 which always happens

      int startCmp = comparePrefixLV(minLV, parent);
      if (startCmp > 0) {//start comes after this cell
        iterFirstCellNumber = 0;
        iterFirstIsIntersects = false;
        iterLastCellNumber = -1;//so ends early (no cells)
        iterLastIsIntersects = false;
        return;
      }
      int endCmp = comparePrefixLV(maxLV, parent);//compare to end cell
      if (endCmp < 0) {//end comes before this cell
        iterFirstCellNumber = 0;
        iterFirstIsIntersects = false;
        iterLastCellNumber = -1;//so ends early (no cells)
        iterLastIsIntersects = false;
        return;
      }
      if (startCmp < 0 || minLV.getLevel() < getLevel()) {
        //start comes before...
        iterFirstCellNumber = 0;
        iterFirstIsIntersects = false;
      } else {
        iterFirstCellNumber = minLV.getValAtLevel(getLevel());
        iterFirstIsIntersects = (minLV.getLevel() > getLevel());
      }
      if (endCmp > 0 || maxLV.getLevel() < getLevel()) {
        //end comes after...
        iterLastCellNumber = getNumSubCells(parent) - 1;
        iterLastIsIntersects = false;
      } else {
        iterLastCellNumber = maxLV.getValAtLevel(getLevel());
        iterLastIsIntersects = (maxLV.getLevel() > getLevel());
      }
      if (iterFirstCellNumber == iterLastCellNumber) {
        if (iterLastIsIntersects)
          iterFirstIsIntersects = true;
        else if (iterFirstIsIntersects)
          iterLastIsIntersects = true;
      }
    }

    @Override
    public boolean hasNext() {
      thisCell = null;
      if (nextCell != null)//calling hasNext twice in a row
        return true;

      if (cellNumber >= iterLastCellNumber)
        return false;

      resetCellWithCellNum(cellNumber < iterFirstCellNumber ? iterFirstCellNumber : cellNumber + 1);

      boolean hasChildren =
          (cellNumber == iterFirstCellNumber && iterFirstIsIntersects)
              || (cellNumber == iterLastCellNumber && iterLastIsIntersects);

      if (!hasChildren) {
        setLeaf();
        setShapeRel(SpatialRelation.WITHIN);
      } else if (iterFirstCellNumber == iterLastCellNumber) {
        setShapeRel(SpatialRelation.CONTAINS);
      } else {
        setShapeRel(SpatialRelation.INTERSECTS);
      }

      nextCell = this;
      return true;
    }

    //TODO override nextFrom to be more efficient

    //----------- LevelledValue / Shape

    @Override
    public int getValAtLevel(int level) {
      final int result = cellsByLevel[level].cellNumber;
      assert result >= 0;//initialized
      return result;
    }

    @Override
    public NRCell getLVAtLevel(int level) {
      assert level <= cellLevel;
      return cellsByLevel[level];
    }

    @Override
    public SpatialRelation relate(Shape shape) {
      ensureDecoded();
      if (shape == iterFilter && cellShapeRel != null)
        return cellShapeRel;
      if (shape instanceof LevelledValue)
        return relate((LevelledValue)shape);
      if (shape instanceof NRShape)
        return relate((NRShape)shape);
      return shape.relate(this).transpose();
    }

    public SpatialRelation relate(LevelledValue lv) {
      ensureDecoded();
      int cmp = comparePrefixLV(this, lv);
      if (cmp != 0)
        return SpatialRelation.DISJOINT;
      if (getLevel() > lv.getLevel())
        return SpatialRelation.WITHIN;//or equals
      return SpatialRelation.CONTAINS;
      //no INTERSECTS; that won't happen.
    }

    public SpatialRelation relate(NRShape nrShape) {
      ensureDecoded();
      int startCmp = comparePrefixLV(nrShape.getMinLV(), this);
      if (startCmp > 0) {//start comes after this cell
        return SpatialRelation.DISJOINT;
      }
      int endCmp = comparePrefixLV(nrShape.getMaxLV(), this);
      if (endCmp < 0) {//end comes before this cell
        return SpatialRelation.DISJOINT;
      }
      int nrMinLevel = nrShape.getMinLV().getLevel();
      int nrMaxLevel = nrShape.getMaxLV().getLevel();
      if ((startCmp < 0 || startCmp == 0 && nrMinLevel <= getLevel())
          && (endCmp > 0 || endCmp == 0 && nrMaxLevel <= getLevel()))
        return SpatialRelation.WITHIN;//or equals
      //At this point it's Contains or Within.
      if (startCmp != 0 || endCmp != 0)
        return SpatialRelation.INTERSECTS;
      //if min or max Level is less, it might be on the equivalent edge.
      for (;nrMinLevel < getLevel(); nrMinLevel++) {
        if (getValAtLevel(nrMinLevel + 1) != 0)
          return SpatialRelation.INTERSECTS;
      }
      for (;nrMaxLevel < getLevel(); nrMaxLevel++) {
        if (getValAtLevel(nrMaxLevel + 1) != getNumSubCells(getLVAtLevel(nrMaxLevel)) - 1)
          return SpatialRelation.INTERSECTS;
      }
      return SpatialRelation.CONTAINS;
    }

    @Override
    public Rectangle getBoundingBox() {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean hasArea() {
      return true;
    }

    @Override
    public double getArea(SpatialContext ctx) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Point getCenter() {
      throw new UnsupportedOperationException();
    }

    @Override
    public Shape getBuffered(double distance, SpatialContext ctx) { throw new UnsupportedOperationException(); }

    @Override
    public boolean isEmpty() {
      return false;
    }

    //------- Object

    @Override
    public boolean equals(Object obj) {
      if (!(obj instanceof NRCell)) {
        return false;
      }
      if (this == obj)
        return true;
      NRCell nrCell = (NRCell) obj;
      assert term != nrCell.term;
      if (getLevel() != nrCell.getLevel())
        return false;
      //trick to re-use bytesref; provided that we re-instate it
      int myLastLen = term.length;
      int otherLastLen = nrCell.term.length;
      boolean answer = getTokenBytesNoLeaf(term).equals(nrCell.getTokenBytesNoLeaf(nrCell.term));
      term.length = myLastLen;
      nrCell.term.length = otherLastLen;
      return answer;
    }

    @Override
    public int hashCode() {
      //trick to re-use bytesref; provided that we re-instate it
      int myLastLen = term.length;
      int result = getTokenBytesNoLeaf(term).hashCode();
      term.length = myLastLen;
      return result;
    }

    @Override
    public String toString() {
      ensureDecoded();
      String str = NumberRangePrefixTree.this.toStringLV(this);
      if (isLeaf())
        str += "•";//bullet (won't be confused with textual representation)
      return str;
    }

    /** Configure your IDE to use this. */
    public String toStringDebug() {
      String pretty = toString();
      if (getLevel() == 0)
        return pretty;
      //now prefix it by an array of integers of the cell levels
      StringBuilder buf = new StringBuilder(100);
      buf.append('[');
      for (int level = 1; level <= getLevel(); level++) {
        if (level != 1)
          buf.append(',');
        buf.append(getLVAtLevel(level).cellNumber);
      }
      buf.append("] ").append(pretty);
      return buf.toString();
    }

  } // END OF NRCell

}

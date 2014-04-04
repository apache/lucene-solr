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

import com.spatial4j.core.shape.Point;
import com.spatial4j.core.shape.Shape;
import com.spatial4j.core.shape.SpatialRelation;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.StringHelper;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * Represents a grid cell. These are not necessarily thread-safe, although calling {@link #getShape()} will
 * sufficiently prepare it to be so, if needed.
 *
 * @lucene.experimental
 */
public abstract class Cell {

  private static final byte LEAF_BYTE = '+';//NOTE: must sort before letters & numbers

  //Arguably we could simply use a BytesRef, using an extra Object.
  private byte[] bytes;
  private int b_off;
  private int b_len;

  /**
   * When set via getSubCells(filter), it is the relationship between this cell
   * and the given shape filter. Doesn't participate in shape equality.
   */
  protected SpatialRelation shapeRel;

  /** Warning: Refers to the same bytes (no copy). If {@link #setLeaf()} is subsequently called then it
   * may modify bytes. */
  protected Cell(byte[] bytes, int off, int len) {
    this.bytes = bytes;
    this.b_off = off;
    this.b_len = len;
  }

  /** Warning: Refers to the same bytes (no copy). If {@link #setLeaf()} is subsequently called then it
   * may modify bytes. */
  public void reset(byte[] bytes, int off, int len) {
    assert getLevel() != 0;
    shapeRel = null;
    this.bytes = bytes;
    this.b_off = off;
    this.b_len = len;
  }

  protected abstract SpatialPrefixTree getGrid();

  public SpatialRelation getShapeRel() {
    return shapeRel;
  }

  /**
   * For points, this is always false.  Otherwise this is true if there are no
   * further cells with this prefix for the shape (always true at maxLevels).
   */
  public boolean isLeaf() {
    return (b_len > 0 && bytes[b_off + b_len - 1] == LEAF_BYTE);
  }

  /** Modifies the bytes to reflect that this is a leaf. Warning: never invoke from a cell
   * initialized to reference the same bytes from termsEnum, which should be treated as immutable.
   * Note: not supported at level 0. */
  public void setLeaf() {
    assert getLevel() != 0;
    if (isLeaf())
      return;
    //if isn't big enough, we have to copy
    if (bytes.length < b_off + b_len) {
      //hopefully this copying doesn't happen too much (DWS: I checked and it doesn't seem to happen)
      byte[] copy = new byte[b_len + 1];
      System.arraycopy(bytes, b_off, copy, 0, b_len);
      copy[b_len++] = LEAF_BYTE;
      bytes = copy;
      b_off = 0;
    } else {
      bytes[b_off + b_len++] = LEAF_BYTE;
    }
  }

  /**
   * Returns the bytes for this cell.
   * The result param is used to save object allocation, though it's bytes aren't used.
   * @param result where the result goes, or null to create new
   */
  public BytesRef getTokenBytes(BytesRef result) {
    if (result == null)
      result = new BytesRef();
    result.bytes = bytes;
    result.offset = b_off;
    result.length = b_len;
    return result;
  }

  /**
   * Returns the bytes for this cell, without leaf set. The bytes should sort before any
   * cells that have the leaf set for the spatial location.
   * The result param is used to save object allocation, though it's bytes aren't used.
   * @param result where the result goes, or null to create new
   */
  public BytesRef getTokenBytesNoLeaf(BytesRef result) {
    result = getTokenBytes(result);
    if (isLeaf())
      result.length--;
    return result;
  }

  /** Level 0 is the world (and has no parent), from then on a higher level means a smaller
   * cell than the level before it.
   */
  public int getLevel() {
    return isLeaf() ? b_len - 1 : b_len;
  }

  /** Gets the parent cell that contains this one. Don't call on the world cell. */
  public Cell getParent() {
    assert getLevel() > 0;
    return getGrid().getCell(bytes, b_off, b_len - (isLeaf() ? 2 : 1));
  }

  /**
   * Like {@link #getSubCells()} but with the results filtered by a shape. If
   * that shape is a {@link com.spatial4j.core.shape.Point} then it must call
   * {@link #getSubCell(com.spatial4j.core.shape.Point)}. The returned cells
   * should have {@link Cell#getShapeRel()} set to their relation with {@code
   * shapeFilter}. In addition, {@link Cell#isLeaf()}
   * must be true when that relation is WITHIN.
   * <p/>
   * Precondition: Never called when getLevel() == maxLevel.
   *
   * @param shapeFilter an optional filter for the returned cells.
   * @return A set of cells (no dups), sorted. Not Modifiable.
   */
  public Collection<Cell> getSubCells(Shape shapeFilter) {
    //Note: Higher-performing subclasses might override to consider the shape filter to generate fewer cells.
    if (shapeFilter instanceof Point) {
      Cell subCell = getSubCell((Point) shapeFilter);
      subCell.shapeRel = SpatialRelation.CONTAINS;
      return Collections.singletonList(subCell);
    }
    Collection<Cell> cells = getSubCells();

    if (shapeFilter == null) {
      return cells;
    }

    //TODO change API to return a filtering iterator
    List<Cell> copy = new ArrayList<>(cells.size());
    for (Cell cell : cells) {
      SpatialRelation rel = cell.getShape().relate(shapeFilter);
      if (rel == SpatialRelation.DISJOINT)
        continue;
      cell.shapeRel = rel;
      if (rel == SpatialRelation.WITHIN)
        cell.setLeaf();
      copy.add(cell);
    }
    return copy;
  }

  /**
   * Performant implementations are expected to implement this efficiently by
   * considering the current cell's boundary. Precondition: Never called when
   * getLevel() == maxLevel.
   * <p/>
   * Precondition: this.getShape().relate(p) != DISJOINT.
   */
  public abstract Cell getSubCell(Point p);

  /**
   * Gets the cells at the next grid cell level that cover this cell.
   * Precondition: Never called when getLevel() == maxLevel.
   *
   * @return A set of cells (no dups), sorted, modifiable, not empty, not null.
   */
  protected abstract Collection<Cell> getSubCells();

  /**
   * {@link #getSubCells()}.size() -- usually a constant. Should be >=2
   */
  public abstract int getSubCellsSize();

  /** Gets the shape for this cell; typically a Rectangle. This method also serves to trigger any lazy
   * loading needed to make the cell instance thread-safe.
   */
  public abstract Shape getShape();

  /** TODO remove once no longer used. */
  public Point getCenter() {
    return getShape().getCenter();
  }

  @Override
  public boolean equals(Object obj) {
    //this method isn't "normally" called; just in asserts/tests
    if (obj instanceof Cell) {
      Cell cell = (Cell) obj;
      return getTokenBytes(null).equals(cell.getTokenBytes(null));
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    return getTokenBytesNoLeaf(null).hashCode();
  }

  @Override
  public String toString() {
    //this method isn't "normally" called; just in asserts/tests
    return getTokenBytes(null).utf8ToString();
  }

  /**
   * Returns if the target term is within/underneath this cell; not necessarily a direct descendant.
   * @param bytesNoLeaf must be getTokenBytesNoLeaf
   * @param term the term
   */
  public boolean isWithin(BytesRef bytesNoLeaf, BytesRef term) {
    assert bytesNoLeaf.equals(getTokenBytesNoLeaf(null));
    return StringHelper.startsWith(term, bytesNoLeaf);
  }
}

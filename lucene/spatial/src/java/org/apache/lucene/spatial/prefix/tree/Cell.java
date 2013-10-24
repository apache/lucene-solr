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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * Represents a grid cell. These are not necessarily thread-safe, although new
 * Cell("") (world cell) must be.
 *
 * @lucene.experimental
 */
public abstract class Cell implements Comparable<Cell> {
  public static final byte LEAF_BYTE = '+';//NOTE: must sort before letters & numbers

  /*
  Holds a byte[] and/or String representation of the cell. Both are lazy constructed from the other.
  Neither contains the trailing leaf byte.
   */
  private byte[] bytes;
  private int b_off;
  private int b_len;

  private String token;//this is the only part of equality

  /**
   * When set via getSubCells(filter), it is the relationship between this cell
   * and the given shape filter.
   */
  protected SpatialRelation shapeRel;

  /**
   * Always false for points. Otherwise, indicate no further sub-cells are going
   * to be provided because shapeRel is WITHIN or maxLevels or a detailLevel is
   * hit.
   */
  protected boolean leaf;

  protected Cell(String token) {
    this.token = token;
    if (token.length() > 0 && token.charAt(token.length() - 1) == (char) LEAF_BYTE) {
      this.token = token.substring(0, token.length() - 1);
      setLeaf();
    }

    if (getLevel() == 0)
      getShape();//ensure any lazy instantiation completes to make this threadsafe
  }

  protected Cell(byte[] bytes, int off, int len) {
    this.bytes = bytes;
    this.b_off = off;
    this.b_len = len;
    b_fixLeaf();
  }

  public void reset(byte[] bytes, int off, int len) {
    assert getLevel() != 0;
    token = null;
    shapeRel = null;
    this.bytes = bytes;
    this.b_off = off;
    this.b_len = len;
    b_fixLeaf();
  }

  private void b_fixLeaf() {
    //note that non-point shapes always have the maxLevels cell set with setLeaf
    if (bytes[b_off + b_len - 1] == LEAF_BYTE) {
      b_len--;
      setLeaf();
    } else {
      leaf = false;
    }
  }

  public SpatialRelation getShapeRel() {
    return shapeRel;
  }

  /**
   * For points, this is always false.  Otherwise this is true if there are no
   * further cells with this prefix for the shape (always true at maxLevels).
   */
  public boolean isLeaf() {
    return leaf;
  }

  /** Note: not supported at level 0. */
  public void setLeaf() {
    assert getLevel() != 0;
    leaf = true;
  }

  /**
   * Note: doesn't contain a trailing leaf byte.
   */
  public String getTokenString() {
    if (token == null) {
      token = new String(bytes, b_off, b_len, SpatialPrefixTree.UTF8);
    }
    return token;
  }

  /**
   * Note: doesn't contain a trailing leaf byte.
   */
  public byte[] getTokenBytes() {
    if (bytes != null) {
      if (b_off != 0 || b_len != bytes.length) {
        throw new IllegalStateException("Not supported if byte[] needs to be recreated.");
      }
    } else {
      bytes = token.getBytes(SpatialPrefixTree.UTF8);
      b_off = 0;
      b_len = bytes.length;
    }
    return bytes;
  }

  public int getLevel() {
    return token != null ? token.length() : b_len;
  }

  //TODO add getParent() and update some algorithms to use this?
  //public Cell getParent();

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
    List<Cell> copy = new ArrayList<Cell>(cells.size());
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

  //TODO Cell getSubCell(byte b)

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

  public abstract Shape getShape();

  public Point getCenter() {
    return getShape().getCenter();
  }

  @Override
  public int compareTo(Cell o) {
    return getTokenString().compareTo(o.getTokenString());
  }

  @Override
  public boolean equals(Object obj) {
    return !(obj == null || !(obj instanceof Cell)) && getTokenString().equals(((Cell) obj).getTokenString());
  }

  @Override
  public int hashCode() {
    return getTokenString().hashCode();
  }

  @Override
  public String toString() {
    return getTokenString() + (isLeaf() ? (char) LEAF_BYTE : "");
  }

}

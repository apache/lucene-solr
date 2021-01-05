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

package org.apache.lucene.spatial.prefix.tree;

import com.google.common.geometry.S2CellId;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.lucene.util.BytesRef;
import org.locationtech.spatial4j.shape.Shape;
import org.locationtech.spatial4j.shape.SpatialRelation;

/**
 * This class represents a S2 pixel in the RPT.
 *
 * @lucene.internal
 */
class S2PrefixTreeCell implements CellCanPrune {

  // Faces of S2 Geometry
  private static S2CellId[] FACES = new S2CellId[6];

  static {
    FACES[0] = S2CellId.fromFacePosLevel(0, 0, 0);
    FACES[1] = S2CellId.fromFacePosLevel(1, 0, 0);
    FACES[2] = S2CellId.fromFacePosLevel(2, 0, 0);
    FACES[3] = S2CellId.fromFacePosLevel(3, 0, 0);
    FACES[4] = S2CellId.fromFacePosLevel(4, 0, 0);
    FACES[5] = S2CellId.fromFacePosLevel(5, 0, 0);
  }

  /*Special character to define a cell leaf*/
  private static final byte LEAF = '+';
  /*Tokens are used to serialize cells*/
  private static final byte[] TOKENS = {
    '.', '/', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F', 'G',
    'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z',
    'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's',
    't', 'u', 'v', 'w', 'x', 'y', 'z'
  };
  /*Map containing mapping between tokens and integer values*/
  private static final Map<Byte, Integer> PIXELS;

  static {
    PIXELS = new HashMap<>(TOKENS.length);
    for (int i = 0; i < TOKENS.length; i++) {
      PIXELS.put(TOKENS[i], i);
    }
  }

  S2CellId cellId;
  int level; // cache level
  S2PrefixTree tree;

  SpatialRelation shapeRel = null;
  boolean isLeaf;
  Shape shape = null;

  S2PrefixTreeCell(S2PrefixTree tree, S2CellId cellId) {
    this.cellId = cellId;
    this.tree = tree;
    setLevel();
    if (getLevel() == tree.getMaxLevels()) {
      setLeaf();
    }
  }

  void readCell(S2PrefixTree tree, BytesRef ref) {
    isLeaf = false;
    shape = null;
    shapeRel = null;
    this.tree = tree;
    cellId = getS2CellIdFromBytesRef(ref);
    setLevel();
    if (isLeaf(ref) || getLevel() == tree.getMaxLevels()) {
      setLeaf();
    }
  }

  @Override
  public SpatialRelation getShapeRel() {
    return shapeRel;
  }

  @Override
  public void setShapeRel(SpatialRelation rel) {
    shapeRel = rel;
  }

  @Override
  public boolean isLeaf() {
    return isLeaf;
  }

  @Override
  public void setLeaf() {
    isLeaf = true;
  }

  @Override
  public BytesRef getTokenBytesWithLeaf(BytesRef result) {
    result = getTokenBytesNoLeaf(result);
    // max levels do not have leaf
    if (isLeaf() && !(getLevel() == tree.getMaxLevels())) {
      // Add leaf byte
      result.bytes[result.offset + result.length] = LEAF;
      result.length++;
    }
    return result;
  }

  @Override
  public BytesRef getTokenBytesNoLeaf(BytesRef result) {
    if (result == null) {
      result = new BytesRef();
    }
    getBytesRefFromS2CellId(cellId, result);
    return result;
  }

  @Override
  public int getLevel() {
    return this.level;
  }

  /** Cache level of cell. */
  private void setLevel() {
    if (this.cellId == null) {
      this.level = 0;
    } else {
      assert cellId.level() % tree.arity == 0;
      this.level = (this.cellId.level() / tree.arity) + 1;
    }
  }

  @Override
  public CellIterator getNextLevelCells(Shape shapeFilter) {
    S2CellId[] children;
    if (cellId == null) { // this is the world cell
      children = FACES;
    } else {
      int nChildren = (int) Math.pow(4, tree.arity);
      children = new S2CellId[nChildren];
      children[0] = cellId.childBegin(cellId.level() + tree.arity);
      for (int i = 1; i < nChildren; i++) {
        children[i] = children[i - 1].next();
      }
    }
    List<Cell> cells = new ArrayList<>(children.length);
    for (S2CellId pixel : children) {
      cells.add(new S2PrefixTreeCell(tree, pixel));
    }
    return new FilterCellIterator(cells.iterator(), shapeFilter);
  }

  @Override
  public Shape getShape() {
    if (shape == null) {
      if (cellId == null) { // World cell
        shape = tree.getSpatialContext().getWorldBounds();
      } else {
        shape = tree.s2ShapeFactory.getS2CellShape(cellId);
      }
    }
    return shape;
  }

  @Override
  public boolean isPrefixOf(Cell c) {
    if (cellId == null) {
      return true;
    }
    S2PrefixTreeCell cell = (S2PrefixTreeCell) c;
    return cellId.contains(cell.cellId);
  }

  @Override
  public int compareToNoLeaf(Cell fromCell) {
    if (cellId == null) {
      return 1;
    }
    S2PrefixTreeCell cell = (S2PrefixTreeCell) fromCell;
    return cellId.compareTo(cell.cellId);
  }

  /**
   * Check if a cell is a leaf.
   *
   * @param ref The Byteref of the leaf
   * @return true if it is a leaf, e.g last byte is the special Character.
   */
  private boolean isLeaf(BytesRef ref) {
    return (ref.bytes[ref.offset + ref.length - 1] == LEAF);
  }

  /**
   * Get the {@link S2CellId} from the {@link BytesRef} representation.
   *
   * @param ref The bytes.
   * @return the corresponding S2 cell.
   */
  private S2CellId getS2CellIdFromBytesRef(BytesRef ref) {
    int length = ref.length;
    if (isLeaf(ref)) {
      length--;
    }
    if (length == 0) {
      return null; // world cell
    }
    int face = PIXELS.get(ref.bytes[ref.offset]);
    S2CellId cellId = FACES[face];
    long id = cellId.id();
    for (int i = ref.offset + 1; i < ref.offset + length; i++) {
      int thisLevel = i - ref.offset;
      int pos = PIXELS.get(ref.bytes[i]);
      // first child at level
      id = id - (id & -id) + (1L << (2 * (S2CellId.MAX_LEVEL - thisLevel * tree.arity)));
      // next until pos
      id = id + pos * ((id & -id) << 1);
    }
    return new S2CellId(id);
  }

  /**
   * Codify a {@link S2CellId} into its {@link BytesRef} representation.
   *
   * @param cellId The S2 Cell id to codify.
   * @param bref The byteref representation.
   */
  private void getBytesRefFromS2CellId(S2CellId cellId, BytesRef bref) {
    if (cellId == null) { // world cell
      bref.length = 0;
      return;
    }
    int length = getLevel() + 1;
    byte[] b = bref.bytes.length >= length ? bref.bytes : new byte[length];
    b[0] = TOKENS[cellId.face()];
    for (int i = 1; i < getLevel(); i++) {
      int offset = 0;
      int level = tree.arity * i;
      for (int j = 1; j < tree.arity; j++) {
        offset = 4 * offset + cellId.childPosition(level - tree.arity + j);
      }
      b[i] = TOKENS[4 * offset + cellId.childPosition(level)];
    }
    bref.bytes = b;
    bref.length = getLevel();
    bref.offset = 0;
  }

  @Override
  public int getSubCellsSize() {
    if (cellId == null) { // root node
      return 6;
    }
    return (int) Math.pow(4, tree.arity);
  }

  @Override
  public int hashCode() {
    if (cellId == null) {
      return super.hashCode();
    }
    return this.cellId.hashCode();
  }

  @Override
  public boolean equals(Object o) {
    S2PrefixTreeCell cell = (S2PrefixTreeCell) o;
    return Objects.equals(cellId, cell.cellId);
  }

  @Override
  public String toString() {
    if (cellId == null) {
      return "0";
    }
    return cellId.toString();
  }
}

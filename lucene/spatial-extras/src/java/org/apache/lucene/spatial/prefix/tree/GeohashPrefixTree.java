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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.lucene.util.BytesRef;
import org.locationtech.spatial4j.context.SpatialContext;
import org.locationtech.spatial4j.io.GeohashUtils;
import org.locationtech.spatial4j.shape.Point;
import org.locationtech.spatial4j.shape.Rectangle;
import org.locationtech.spatial4j.shape.Shape;

/**
 * A {@link SpatialPrefixTree} based on <a
 * href="http://en.wikipedia.org/wiki/Geohash">Geohashes</a>. Uses {@link GeohashUtils} to do all
 * the geohash work.
 *
 * @lucene.experimental
 */
public class GeohashPrefixTree extends LegacyPrefixTree {

  /** Factory for creating {@link GeohashPrefixTree} instances with useful defaults */
  public static class Factory extends SpatialPrefixTreeFactory {

    @Override
    protected int getLevelForDistance(double degrees) {
      GeohashPrefixTree grid = new GeohashPrefixTree(ctx, GeohashPrefixTree.getMaxLevelsPossible());
      return grid.getLevelForDistance(degrees);
    }

    @Override
    protected SpatialPrefixTree newSPT() {
      return new GeohashPrefixTree(
          ctx, maxLevels != null ? maxLevels : GeohashPrefixTree.getMaxLevelsPossible());
    }
  }

  public GeohashPrefixTree(SpatialContext ctx, int maxLevels) {
    super(ctx, maxLevels);
    Rectangle bounds = ctx.getWorldBounds();
    if (bounds.getMinX() != -180)
      throw new IllegalArgumentException(
          "Geohash only supports lat-lon world bounds. Got " + bounds);
    int MAXP = getMaxLevelsPossible();
    if (maxLevels <= 0 || maxLevels > MAXP)
      throw new IllegalArgumentException("maxLevels must be [1-" + MAXP + "] but got " + maxLevels);
  }

  /** Any more than this and there's no point (double lat and lon are the same). */
  public static int getMaxLevelsPossible() {
    return GeohashUtils.MAX_PRECISION;
  }

  @Override
  public Cell getWorldCell() {
    return new GhCell(BytesRef.EMPTY_BYTES, 0, 0);
  }

  @Override
  public int getLevelForDistance(double dist) {
    if (dist == 0) return maxLevels; // short circuit
    final int level = GeohashUtils.lookupHashLenForWidthHeight(dist, dist);
    return Math.max(Math.min(level, maxLevels), 1);
  }

  @Override
  protected Cell getCell(Point p, int level) {
    return new GhCell(
        GeohashUtils.encodeLatLon(p.getY(), p.getX(), level)); // args are lat,lon (y,x)
  }

  private static byte[] stringToBytesPlus1(String token) {
    // copy ASCII token to byte array with one extra spot for eventual LEAF_BYTE if needed
    byte[] bytes = new byte[token.length() + 1];
    for (int i = 0; i < token.length(); i++) {
      bytes[i] = (byte) token.charAt(i);
    }
    return bytes;
  }

  private class GhCell extends LegacyCell {

    private String geohash; // cache; never has leaf byte, simply a geohash

    GhCell(String geohash) {
      super(stringToBytesPlus1(geohash), 0, geohash.length());
      this.geohash = geohash;
      if (isLeaf()
          && getLevel() < getMaxLevels()) // we don't have a leaf byte at max levels (an opt)
      this.geohash = geohash.substring(0, geohash.length() - 1);
    }

    GhCell(byte[] bytes, int off, int len) {
      super(bytes, off, len);
    }

    @Override
    protected GeohashPrefixTree getGrid() {
      return GeohashPrefixTree.this;
    }

    @Override
    protected int getMaxLevels() {
      return maxLevels;
    }

    @Override
    protected void readCell(BytesRef bytesRef) {
      super.readCell(bytesRef);
      geohash = null;
    }

    @Override
    public Collection<Cell> getSubCells() {
      String[] hashes = GeohashUtils.getSubGeohashes(getGeohash()); // sorted
      List<Cell> cells = new ArrayList<>(hashes.length);
      for (String hash : hashes) {
        cells.add(new GhCell(hash));
      }
      return cells;
    }

    @Override
    public int getSubCellsSize() {
      return 32; // 8x4
    }

    @Override
    protected GhCell getSubCell(Point p) {
      return (GhCell) getGrid().getCell(p, getLevel() + 1); // not performant!
    }

    @Override
    public Shape getShape() {
      if (shape == null) {
        shape = GeohashUtils.decodeBoundary(getGeohash(), getGrid().getSpatialContext());
      }
      return shape;
    }

    private String getGeohash() {
      if (geohash == null) geohash = getTokenBytesNoLeaf(null).utf8ToString();
      return geohash;
    }
  } // class GhCell
}

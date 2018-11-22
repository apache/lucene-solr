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
import java.util.List;

import com.google.common.geometry.S2CellId;
import com.google.common.geometry.S2LatLng;
import com.google.common.geometry.S2Projections;
import org.apache.lucene.util.BytesRef;
import org.locationtech.spatial4j.context.SpatialContext;
import org.locationtech.spatial4j.distance.DistanceUtils;
import org.locationtech.spatial4j.shape.Point;
import org.locationtech.spatial4j.shape.Shape;

/**
 * Spatial prefix tree for <a href="https://s2geometry.io/">S2 Geometry</a>. Shape factories
 * for the given {@link SpatialContext} must implement the interface {@link S2ShapeFactory}.
 *
 * The tree can be configured on how it divided itself by providing an arity. The default arity is 1
 * which divided every sub-cell in 4 (except the first level that is always divided by 6) . Arity 2
 * divides sub-cells in 16 and arity 3 in 64 sub-cells.
 *
 * @lucene.experimental
 */
public class S2PrefixTree extends SpatialPrefixTree {


    /**
     * Factory for creating {@link S2PrefixTree} instances with useful defaults
     */
    protected static class Factory extends SpatialPrefixTreeFactory {

        @Override
        protected int getLevelForDistance(double degrees) {
            S2PrefixTree grid = new S2PrefixTree(ctx, S2PrefixTree.getMaxLevels(1));
            return grid.getLevelForDistance(degrees);
        }

        @Override
        protected SpatialPrefixTree newSPT() {
            return new S2PrefixTree(ctx,
                maxLevels != null ? maxLevels : S2PrefixTree.getMaxLevels(1));
        }

    }

    //factory to generate S2 cell shapes
    protected final S2ShapeFactory s2ShapeFactory;
    protected final int arity;

    /**
     * Creates a S2 spatial tree with arity 1.
     *
     * @param ctx The provided spatial context. The shape factor of the spatial context
     *           must implement {@link S2ShapeFactory}
     * @param maxLevels The provided maximum level for this tree.
     */
    public S2PrefixTree(SpatialContext ctx, int maxLevels) {
        this(ctx, maxLevels, 1);
    }

    /**
     * Creates a S2 spatial tree with provided arity.
     *
     * @param ctx The provided spatial context. The shape factor of the spatial context
     *           must implement {@link S2ShapeFactory}
     * @param maxLevels The provided maximum level for this tree.
     * @param arity The arity of the tree.
     */
    public S2PrefixTree(SpatialContext ctx, int maxLevels, int arity) {
        super(ctx, maxLevels);
        if (!(ctx.getShapeFactory() instanceof S2ShapeFactory)) {
            throw new IllegalArgumentException("Spatial context does not support S2 spatial index.");
        }
        this.s2ShapeFactory = (S2ShapeFactory) ctx.getShapeFactory();
        if (arity <1 || arity > 3) {
            throw new IllegalArgumentException("Invalid value for S2 tree arity. Possible values are 1, 2 or 3. Provided value is " + arity  + ".");
        }
        this.arity = arity;
    }

    /**
     * Get max levels for this spatial tree.
     *
     * @param arity The arity of the tree.
     * @return The maximum number of levels by the provided arity.
     */
    public static int getMaxLevels(int arity) {
        return  S2CellId.MAX_LEVEL/arity + 1;
    }

    @Override
    public int getLevelForDistance(double dist) {
        if (dist == 0){
            return maxLevels;
        }
        int level =  S2Projections.MAX_WIDTH.getMinLevel(dist * DistanceUtils.DEGREES_TO_RADIANS);
        int roundLevel = level % arity != 0 ? 1 : 0;
        level = level/arity + roundLevel;
        return Math.min(maxLevels, level + 1);
    }

    @Override
    public double getDistanceForLevel(int level) {
        if (level == 0) {
            return 180;
        }
        return S2Projections.MAX_WIDTH.getValue(arity * (level - 1)) * DistanceUtils.RADIANS_TO_DEGREES;
    }

    @Override
    public Cell getWorldCell() {
        return  new S2PrefixTreeCell(this, null);
    }

    @Override
    public Cell readCell(BytesRef term, Cell scratch) {
        S2PrefixTreeCell cell = (S2PrefixTreeCell) scratch;
        if (cell == null) {
            cell = (S2PrefixTreeCell) getWorldCell();
        }
        cell.readCell(this, term);
        return cell;
    }

    @Override
    public CellIterator getTreeCellIterator(Shape shape, int detailLevel) {
        if (!(shape instanceof Point)) {
            return  super.getTreeCellIterator(shape, detailLevel);
        }
        Point p = (Point) shape;
        S2CellId id = S2CellId.fromLatLng(S2LatLng.fromDegrees(p.getY(), p.getX())).parent(arity * (detailLevel - 1));
        List<Cell> cells = new ArrayList<>(detailLevel);
        for (int i=0; i < detailLevel - 1; i++) {
            cells.add(new S2PrefixTreeCell(this, id.parent(i * arity)));
        }
        cells.add(new S2PrefixTreeCell(this, id));
        return new FilterCellIterator(cells.iterator(), null);
    }
}
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
 * Spatial prefix tree for S2 Geometry. Shape factories for the given {@link SpatialContext} must
 * implement the interface {@link S2ShapeFactory}.
 *
 * @lucene.experimental
 */
public class S2PrefixTree extends SpatialPrefixTree {

    /**
     * Factory for creating {@link S2PrefixTree} instances with useful defaults
     */
    public static class Factory extends SpatialPrefixTreeFactory {

        @Override
        protected int getLevelForDistance(double degrees) {
            S2PrefixTree grid = new S2PrefixTree(ctx, S2PrefixTree.MAX_LEVELS);
            return grid.getLevelForDistance(degrees);
        }

        @Override
        protected SpatialPrefixTree newSPT() {
            return new S2PrefixTree(ctx,
                maxLevels != null ? maxLevels : S2PrefixTree.MAX_LEVELS);
        }
    }

    //factory to generate S2 cell shapes
    protected final S2ShapeFactory s2ShapeFactory;
    public static final int MAX_LEVELS = S2CellId.MAX_LEVEL + 1;

    public S2PrefixTree(SpatialContext ctx, int maxLevels) {
        super(ctx, maxLevels);
        if (!(ctx.getShapeFactory() instanceof S2ShapeFactory)) {
            throw new IllegalArgumentException("Spatial context does not support S2 spatial index.");
        }
        this.s2ShapeFactory = (S2ShapeFactory) ctx.getShapeFactory();
    }

    @Override
    public int getLevelForDistance(double dist) {
        if (dist ==0){
            return maxLevels;
        }
        return Math.min(maxLevels, S2Projections.MAX_WIDTH.getClosestLevel(dist * DistanceUtils.DEGREES_TO_RADIANS) +1);
    }

    @Override
    public double getDistanceForLevel(int level) {
        return S2Projections.MAX_WIDTH.getValue(level -1) * DistanceUtils.RADIANS_TO_DEGREES;
    }

    @Override
    public Cell getWorldCell() {
        return  new S2PrefixTreeCell(this, null);
    }

    @Override
    public Cell readCell(BytesRef term, Cell scratch) {
        S2PrefixTreeCell cell = (S2PrefixTreeCell) scratch;
        if (cell == null)
            cell = (S2PrefixTreeCell) getWorldCell();
        cell.readCell(this, term);
        return cell;
    }

    @Override
    public CellIterator getTreeCellIterator(Shape shape, int detailLevel) {
        if (!(shape instanceof Point)) {
            return  super.getTreeCellIterator(shape, detailLevel);
        }
        Point p = (Point) shape;
        S2CellId id = S2CellId.fromLatLng(S2LatLng.fromDegrees(p.getY(), p.getX())).parent(detailLevel-1);
        List<Cell> cells = new ArrayList<>(detailLevel);
        for (int i=0; i < detailLevel -1; i++) {
            cells.add(new S2PrefixTreeCell(this, id.parent(i)));
        }
        cells.add(new S2PrefixTreeCell(this, id));
        return new FilterCellIterator(cells.iterator(), null);
    }
}

package org.apache.lucene.spatial.bbox;

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
import com.spatial4j.core.distance.DistanceCalculator;
import com.spatial4j.core.shape.Point;
import com.spatial4j.core.shape.Rectangle;
import org.apache.lucene.search.Explanation;

/**
 * Returns the distance between the center of the indexed rectangle and the
 * query shape.
 * @lucene.experimental
 */
public class DistanceSimilarity implements BBoxSimilarity {
  private final Point queryPoint;
  private final double multiplier;
  private final DistanceCalculator distCalc;
  private final double nullValue;

  public DistanceSimilarity(SpatialContext ctx, Point queryPoint, double multiplier) {
    this.queryPoint = queryPoint;
    this.multiplier = multiplier;
    this.distCalc = ctx.getDistCalc();
    this.nullValue = (ctx.isGeo() ? 180 * multiplier : Double.MAX_VALUE);
  }

  @Override
  public double score(Rectangle indexRect, Explanation exp) {
    double score;
    if (indexRect == null) {
      score = nullValue;
    } else {
      score = distCalc.distance(queryPoint, indexRect.getCenter()) * multiplier;
    }
    if (exp != null) {
      exp.setValue((float)score);
      exp.setDescription(this.getClass().getSimpleName());
      exp.addDetail(new Explanation(-1f, "" + queryPoint));
      exp.addDetail(new Explanation(-1f,""+indexRect));
      exp.addDetail(new Explanation((float)multiplier,"multiplier"));
    }
    return score;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    DistanceSimilarity that = (DistanceSimilarity) o;

    if (Double.compare(that.multiplier, multiplier) != 0) return false;
    if (Double.compare(that.nullValue, nullValue) != 0) return false;
    if (!distCalc.equals(that.distCalc)) return false;
    if (!queryPoint.equals(that.queryPoint)) return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result;
    long temp;
    result = queryPoint.hashCode();
    temp = Double.doubleToLongBits(multiplier);
    result = 31 * result + (int) (temp ^ (temp >>> 32));
    result = 31 * result + distCalc.hashCode();
    temp = Double.doubleToLongBits(nullValue);
    result = 31 * result + (int) (temp ^ (temp >>> 32));
    return result;
  }
}

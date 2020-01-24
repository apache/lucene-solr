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

package org.apache.solr.client.solrj.io.eval;

import java.io.IOException;
import java.util.Locale;

import org.apache.commons.math3.geometry.euclidean.twod.Euclidean2D;
import org.apache.commons.math3.geometry.euclidean.twod.hull.ConvexHull2D;
import org.apache.commons.math3.geometry.euclidean.twod.Vector2D;
import org.apache.commons.math3.geometry.partitioning.BoundaryProjection;
import org.apache.commons.math3.geometry.partitioning.Region;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;

public class ProjectToBorderEvaluator extends RecursiveObjectEvaluator implements TwoValueWorker {
  private static final long serialVersionUID = 1;

  public ProjectToBorderEvaluator(StreamExpression expression, StreamFactory factory) throws IOException {
    super(expression, factory);
  }

  @Override
  public Object doWork(Object value1, Object value2) throws IOException {
    if(!(value1 instanceof ConvexHull2D)){
      throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - found type %s for value, expecting a ConvexHull2D",toExpression(constructingFactory), value1.getClass().getSimpleName()));
    }

    if(!(value2 instanceof Matrix)){
      throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - found type %s for value, expecting a Matrix",toExpression(constructingFactory), value2.getClass().getSimpleName()));
    }

    ConvexHull2D convexHull2D = (ConvexHull2D)value1;
    Matrix matrix = (Matrix)value2;
    double[][] data = matrix.getData();
    Region<Euclidean2D> region = convexHull2D.createRegion();
    double[][] borderPoints = new double[data.length][2];
    int i = 0;
    for(double[] row : data) {
      BoundaryProjection<Euclidean2D> boundaryProjection = region.projectToBoundary(new Vector2D(row));
      Vector2D point = (Vector2D)boundaryProjection.getProjected();
      borderPoints[i][0] = point.getX();
      borderPoints[i][1] = point.getY();
      i++;
    }

    return new Matrix(borderPoints);

  }
}
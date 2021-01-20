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
import java.util.List;
import java.util.ArrayList;

import org.apache.commons.math3.geometry.euclidean.twod.Vector2D;
import org.apache.commons.math3.geometry.euclidean.twod.hull.ConvexHull2D;
import org.apache.commons.math3.geometry.euclidean.twod.hull.MonotoneChain;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;

public class ConvexHullEvaluator extends RecursiveObjectEvaluator implements ManyValueWorker {
  protected static final long serialVersionUID = 1L;

  public ConvexHullEvaluator(StreamExpression expression, StreamFactory factory) throws IOException{
    super(expression, factory);
  }

  @Override
  public Object doWork(Object... objects) throws IOException{

    if(objects[0] instanceof Matrix) {
      return getConvexHull((Matrix)objects[0]);
    } else {
      throw new IOException("The convexHull function operates on a matrix of 2D vectors");
    }
  }

  public static ConvexHull2D getConvexHull(Matrix matrix) throws IOException {
    double[][] data = matrix.getData();
    List<Vector2D> points = new ArrayList<>(data.length);
    if(data[0].length == 2) {
      for(double[] row : data) {
        points.add(new Vector2D(row[0], row[1]));
      }

      MonotoneChain monotoneChain = new MonotoneChain();
      ConvexHull2D convexHull2D = monotoneChain.generate(points);
      return convexHull2D;
    } else {
      throw new IOException("The convexHull function operates on a matrix of 2D vectors");
    }
  }

}

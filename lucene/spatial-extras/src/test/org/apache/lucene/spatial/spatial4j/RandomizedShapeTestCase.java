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
package org.apache.lucene.spatial.spatial4j;

import org.locationtech.spatial4j.context.SpatialContext;

import org.apache.lucene.util.LuceneTestCase;

/**
 * A base test class with utility methods to help test shapes.
 * Extends from RandomizedTest.
 */
public abstract class RandomizedShapeTestCase extends LuceneTestCase {

  protected SpatialContext ctx;//needs to be set ASAP

  public RandomizedShapeTestCase(SpatialContext ctx) {
    this.ctx = ctx;
  }

  @SuppressWarnings("unchecked")
  public static void checkShapesImplementEquals( Class<?>[] classes ) {
    for( Class<?> clazz : classes ) {
      try {
        clazz.getDeclaredMethod( "equals", Object.class );
      } catch (Exception e) {
        fail("Shape needs to define 'equals' : " + clazz.getName());
      }
      try {
        clazz.getDeclaredMethod( "hashCode" );
      } catch (Exception e) {
        fail("Shape needs to define 'hashCode' : " + clazz.getName());
      }
    }
  }

  public static double divisible(double v, double divisible) {
    return (int) (Math.round(v / divisible) * divisible);
  }

}

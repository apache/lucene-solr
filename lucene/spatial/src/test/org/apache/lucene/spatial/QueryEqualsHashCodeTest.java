package org.apache.lucene.spatial;

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
import com.spatial4j.core.shape.Shape;
import org.apache.lucene.spatial.prefix.RecursivePrefixTreeStrategy;
import org.apache.lucene.spatial.prefix.TermQueryPrefixTreeStrategy;
import org.apache.lucene.spatial.prefix.tree.QuadPrefixTree;
import org.apache.lucene.spatial.prefix.tree.SpatialPrefixTree;
import org.apache.lucene.spatial.query.SpatialArgs;
import org.apache.lucene.spatial.query.SpatialOperation;
import org.apache.lucene.spatial.vector.PointVectorStrategy;
import org.apache.lucene.util.LuceneTestCase;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;

public class QueryEqualsHashCodeTest extends LuceneTestCase {

  private final SpatialContext ctx = SpatialContext.GEO;

  @Test
  public void testEqualsHashCode() {

    final SpatialPrefixTree grid = new QuadPrefixTree(ctx,10);
    final SpatialArgs args1 = makeArgs1();
    final SpatialArgs args2 = makeArgs2();

    Collection<ObjGenerator> generators = new ArrayList<ObjGenerator>();
    generators.add(new ObjGenerator() {
      @Override
      public Object gen(SpatialArgs args) {
        return new RecursivePrefixTreeStrategy(grid, "recursive_quad").makeQuery(args);
      }
    });
    generators.add(new ObjGenerator() {
      @Override
      public Object gen(SpatialArgs args) {
        return new TermQueryPrefixTreeStrategy(grid, "termquery_quad").makeQuery(args);
      }
    });
    generators.add(new ObjGenerator() {
      @Override
      public Object gen(SpatialArgs args) {
        return new PointVectorStrategy(ctx, "pointvector").makeQuery(args);
      }
    });

    for (ObjGenerator generator : generators) {
      testStratQueryEqualsHashcode(args1, args2, generator);
    }
  }

  private void testStratQueryEqualsHashcode(SpatialArgs args1, SpatialArgs args2, ObjGenerator generator) {
    Object first = generator.gen(args1);
    Object second = generator.gen(args1);//should be the same
    assertEquals(first, second);
    assertEquals(first.hashCode(), second.hashCode());
    second = generator.gen(args2);//now should be different
    assertNotSame(args1, args2);
  }

  private SpatialArgs makeArgs1() {
    final Shape shape1 = ctx.makeRectangle(0, 0, 10, 10);
    return new SpatialArgs(SpatialOperation.Intersects, shape1);
  }

  private SpatialArgs makeArgs2() {
    final Shape shape2 = ctx.makeRectangle(0, 0, 20, 20);
    return new SpatialArgs(SpatialOperation.Intersects, shape2);
  }

  interface ObjGenerator {
    Object gen(SpatialArgs args);
  }

}

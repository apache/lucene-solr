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

import com.spatial4j.core.context.SpatialContext;
import com.spatial4j.core.distance.DistanceUtils;

import java.util.Map;

/**
 * Abstract Factory for creating {@link SpatialPrefixTree} instances with useful
 * defaults and passed on configurations defined in a Map.
 *
 * @lucene.experimental
 */
public abstract class SpatialPrefixTreeFactory {

  private static final double DEFAULT_GEO_MAX_DETAIL_KM = 0.001;//1m
  public static final String PREFIX_TREE = "prefixTree";
  public static final String MAX_LEVELS = "maxLevels";
  public static final String MAX_DIST_ERR = "maxDistErr";

  protected Map<String, String> args;
  protected SpatialContext ctx;
  protected Integer maxLevels;

  /**
   * The factory  is looked up via "prefixTree" in args, expecting "geohash" or "quad".
   * If its neither of these, then "geohash" is chosen for a geo context, otherwise "quad" is chosen.
   */
  public static SpatialPrefixTree makeSPT(Map<String,String> args, ClassLoader classLoader, SpatialContext ctx) {
    SpatialPrefixTreeFactory instance;
    String cname = args.get(PREFIX_TREE);
    if (cname == null)
      cname = ctx.isGeo() ? "geohash" : "quad";
    if ("geohash".equalsIgnoreCase(cname))
      instance = new GeohashPrefixTree.Factory();
    else if ("quad".equalsIgnoreCase(cname))
      instance = new QuadPrefixTree.Factory();
    else {
      try {
        Class c = classLoader.loadClass(cname);
        instance = (SpatialPrefixTreeFactory) c.newInstance();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
    instance.init(args,ctx);
    return instance.newSPT();
  }

  protected void init(Map<String, String> args, SpatialContext ctx) {
    this.args = args;
    this.ctx = ctx;
    initMaxLevels();
  }

  protected void initMaxLevels() {
    String mlStr = args.get(MAX_LEVELS);
    if (mlStr != null) {
      maxLevels = Integer.valueOf(mlStr);
      return;
    }

    double degrees;
    String maxDetailDistStr = args.get(MAX_DIST_ERR);
    if (maxDetailDistStr == null) {
      if (!ctx.isGeo()) {
        return;//let default to max
      }
      degrees = DistanceUtils.dist2Degrees(DEFAULT_GEO_MAX_DETAIL_KM, DistanceUtils.EARTH_MEAN_RADIUS_KM);
    } else {
      degrees = Double.parseDouble(maxDetailDistStr);
    }
    maxLevels = getLevelForDistance(degrees);
  }

  /** Calls {@link SpatialPrefixTree#getLevelForDistance(double)}. */
  protected abstract int getLevelForDistance(double degrees);

  protected abstract SpatialPrefixTree newSPT();

}

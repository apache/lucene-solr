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

import java.text.ParseException;
import java.util.Map;
import org.apache.lucene.util.Version;
import org.locationtech.spatial4j.context.SpatialContext;
import org.locationtech.spatial4j.distance.DistanceUtils;

/**
 * Abstract Factory for creating {@link SpatialPrefixTree} instances with useful defaults and passed
 * on configurations defined in a Map.
 *
 * @lucene.experimental
 */
public abstract class SpatialPrefixTreeFactory {

  private static final double DEFAULT_GEO_MAX_DETAIL_KM = 0.001; // 1m
  public static final String PREFIX_TREE = "prefixTree";
  public static final String MAX_LEVELS = "maxLevels";
  public static final String MAX_DIST_ERR = "maxDistErr";
  public static final String VERSION = "version";

  protected Map<String, String> args;
  protected SpatialContext ctx;
  protected Integer maxLevels;
  private Version version;

  /**
   * The factory is looked up via "prefixTree" in args, expecting "geohash" or "quad". If it's
   * neither of these, then "geohash" is chosen for a geo context, otherwise "quad" is chosen. The
   * "version" arg, if present, is parsed with {@link Version} and the prefix tree might be
   * sensitive to it.
   */
  public static SpatialPrefixTree makeSPT(
      Map<String, String> args, ClassLoader classLoader, SpatialContext ctx) {
    // TODO refactor to use Java SPI like how Lucene already does for codecs/postingsFormats, etc
    SpatialPrefixTreeFactory instance;
    String cname = args.get(PREFIX_TREE);
    if (cname == null) cname = ctx.isGeo() ? "geohash" : "quad";
    if ("geohash".equalsIgnoreCase(cname)) instance = new GeohashPrefixTree.Factory();
    else if ("quad".equalsIgnoreCase(cname)) instance = new QuadPrefixTree.Factory();
    else if ("packedQuad".equalsIgnoreCase(cname)) instance = new PackedQuadPrefixTree.Factory();
    else if ("s2".equalsIgnoreCase(cname)) instance = new S2PrefixTree.Factory();
    else {
      try {
        Class<?> c = classLoader.loadClass(cname);
        instance = (SpatialPrefixTreeFactory) c.getConstructor().newInstance();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
    instance.init(args, ctx);
    return instance.newSPT();
  }

  protected void init(Map<String, String> args, SpatialContext ctx) {
    this.args = args;
    this.ctx = ctx;
    initVersion();
    initMaxLevels();
  }

  protected void initVersion() {
    String versionStr = args.get(VERSION);
    try {
      setVersion(versionStr == null ? Version.LATEST : Version.parseLeniently(versionStr));
    } catch (ParseException e) {
      throw new RuntimeException(e);
    }
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
        return; // let default to max
      }
      degrees =
          DistanceUtils.dist2Degrees(DEFAULT_GEO_MAX_DETAIL_KM, DistanceUtils.EARTH_MEAN_RADIUS_KM);
    } else {
      degrees = Double.parseDouble(maxDetailDistStr);
    }
    maxLevels = getLevelForDistance(degrees);
  }

  /** Set the version of Lucene this tree should mimic the behavior for for analysis. */
  public void setVersion(Version v) {
    version = v;
  }

  /** Return the version of Lucene this tree will mimic the behavior of for analysis. */
  public Version getVersion() {
    return version;
  }

  /** Calls {@link SpatialPrefixTree#getLevelForDistance(double)}. */
  protected abstract int getLevelForDistance(double degrees);

  protected abstract SpatialPrefixTree newSPT();
}

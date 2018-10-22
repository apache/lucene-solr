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
package org.apache.solr.common.params;
/**
 *
 *
 **/
public interface SpatialParams {
  public static final String POINT = "pt";
  public static final String DISTANCE = "d";
  public static final String FIELD = "sfield";  // the field that contains the points we are measuring from "pt"
  /**
   * km - kilometers
   * mi - miles
   */
  public static final String UNITS = "units";
  /**
   * The distance measure to use.
   */
  public static final String MEASURE = "meas";
  /**
   * The radius of the sphere to use to in calculating spherical distances like Haversine
   */
  public static final String SPHERE_RADIUS = "sphere_radius";
}

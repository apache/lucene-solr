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
package org.apache.solr.search;
import org.apache.solr.schema.SchemaField;

/**
 *
 *
 **/
public class SpatialOptions {
  public String pointStr;
  public double distance;
  public SchemaField field;
  public String measStr;
  public double radius;//(planetRadius) effectively establishes the units

  /** Just do a "bounding box" - or any other quicker method / shape that
   * still encompasses all of the points of interest, but may also encompass
   * points outside.
   */ 
  public boolean bbox;

  public SpatialOptions() {
  }

  public SpatialOptions(String pointStr, double dist, SchemaField sf, String measStr, double radius) {
    this.pointStr = pointStr;
    this.distance = dist;
    this.field = sf;
    this.measStr = measStr;
    this.radius = radius;
  }
}

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
package org.apache.lucene.spatial;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.locationtech.spatial4j.context.SpatialContext;
import org.locationtech.spatial4j.shape.Shape;

// This class is modelled after SpatialTestQuery.
// Before Lucene 4.7, this was a bit different in Spatial4j as SampleData & SampleDataReader.

public class SpatialTestData {
  public String id;
  public String name;
  public Shape shape;

  /**
   * Reads the stream, consuming a format that is a tab-separated values of 3 columns: an "id", a
   * "name" and the "shape". Empty lines and lines starting with a '#' are skipped. The stream is
   * closed.
   */
  public static Iterator<SpatialTestData> getTestData(InputStream in, SpatialContext ctx)
      throws IOException {
    List<SpatialTestData> results = new ArrayList<>();
    BufferedReader bufInput = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8));
    try {
      String line;
      while ((line = bufInput.readLine()) != null) {
        if (line.length() == 0 || line.charAt(0) == '#') continue;

        SpatialTestData data = new SpatialTestData();
        String[] vals = line.split("\t");
        if (vals.length != 3)
          throw new RuntimeException(
              "bad format; expecting 3 tab-separated values for line: " + line);
        data.id = vals[0];
        data.name = vals[1];
        try {
          data.shape = ctx.readShapeFromWkt(vals[2]);
        } catch (ParseException e) {
          throw new RuntimeException(e);
        }
        results.add(data);
      }
    } finally {
      bufInput.close();
    }
    return results.iterator();
  }
}

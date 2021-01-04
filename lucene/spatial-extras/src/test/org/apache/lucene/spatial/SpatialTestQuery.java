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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;
import org.apache.lucene.spatial.query.SpatialArgs;
import org.apache.lucene.spatial.query.SpatialArgsParser;
import org.locationtech.spatial4j.context.SpatialContext;

/** Helper class to execute queries */
public class SpatialTestQuery {
  public String testname;
  public String line;
  public int lineNumber = -1;
  public SpatialArgs args;
  public List<String> ids = new ArrayList<>();

  /** Get Test Queries. The InputStream is closed. */
  public static Iterator<SpatialTestQuery> getTestQueries(
      final SpatialArgsParser parser,
      final SpatialContext ctx,
      final String name,
      final InputStream in)
      throws IOException {

    List<SpatialTestQuery> results = new ArrayList<>();

    BufferedReader bufInput = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8));
    try {
      String line;
      for (int lineNumber = 1; (line = bufInput.readLine()) != null; lineNumber++) {
        SpatialTestQuery test = new SpatialTestQuery();
        test.line = line;
        test.lineNumber = lineNumber;

        try {
          // skip a comment
          if (line.startsWith("[")) {
            int idx = line.indexOf(']');
            if (idx > 0) {
              line = line.substring(idx + 1);
            }
          }

          int idx = line.indexOf('@');
          StringTokenizer st = new StringTokenizer(line.substring(0, idx));
          while (st.hasMoreTokens()) {
            test.ids.add(st.nextToken().trim());
          }
          test.args = parser.parse(line.substring(idx + 1).trim(), ctx);
          results.add(test);
        } catch (Exception ex) {
          throw new RuntimeException("invalid query line: " + test.line, ex);
        }
      }
    } finally {
      bufInput.close();
    }
    return results.iterator();
  }

  @Override
  public String toString() {
    if (line != null) return line;
    return args.toString() + " " + ids;
  }
}

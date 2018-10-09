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
package org.apache.lucene.geo;

import java.text.ParseException;
import java.util.List;

import org.apache.lucene.util.LuceneTestCase;

import static org.apache.lucene.geo.GeoTestUtil.nextBoxNotCrossingDateline;

/** Test case for the Polygon {@link Tessellator} class */
public class TestTessellator extends LuceneTestCase {

  /** test line intersection */
  public void testLinesIntersect() {
    Rectangle rect = nextBoxNotCrossingDateline();
    // simple case; test intersecting diagonals
    // note: we don't quantize because the tessellator operates on non quantized vertices
    assertTrue(Tessellator.linesIntersect(rect.minLon, rect.minLat, rect.maxLon, rect.maxLat, rect.maxLon, rect.minLat, rect.minLon, rect.maxLat));
    // test closest encoded value
    assertFalse(Tessellator.linesIntersect(rect.minLon, rect.maxLat, rect.maxLon, rect.maxLat, rect.minLon - 1d, rect.minLat, rect.minLon - 1, rect.maxLat));
  }

  public void testSimpleTessellation() throws Exception {
    Polygon poly = GeoTestUtil.createRegularPolygon(0.0, 0.0, 1000000, 1000000);
    Polygon inner = new Polygon(new double[] {-1.0, -1.0, 0.5, 1.0, 1.0, 0.5, -1.0},
        new double[]{1.0, -1.0, -0.5, -1.0, 1.0, 0.5, 1.0});
    Polygon inner2 = new Polygon(new double[] {-1.0, -1.0, 0.5, 1.0, 1.0, 0.5, -1.0},
        new double[]{-2.0, -4.0, -3.5, -4.0, -2.0, -2.5, -2.0});
    poly = new Polygon(poly.getPolyLats(), poly.getPolyLons(), inner, inner2);
    assertTrue(Tessellator.tessellate(poly).size() > 0);
  }

  public void testLUCENE8454() throws ParseException {
    String geoJson = "{\"type\": \"Polygon\", \"coordinates\": [[[167.8752929333776, -30.078235509309092], [167.729078, -30.078368], [167.7288750679411, -29.918443128222044], [167.728949, -30.078598], [167.582239, -30.078557], [167.58234527408044, -29.9717026229659],  " +
        "[167.43547018634274, -30.030896196337487], [167.43528, -30.078575], [167.288467, -30.078185], [167.28846777961195, -30.078041819512045], [167.142089, -30.077483], [167.143635, -29.813199], [167.1450859974141, -29.567345798606294], [167.144888, -29.567345], " +
        "[167.14633281276596, -29.302953194679134], [167.146281, -29.302953], [167.147725, -29.036352], [167.292924, -29.036892], [167.2918703799358, -29.301396273146477], [167.29192460356776, -29.301396365495897], [167.292964, -29.036798], [167.4380298884901, -29.037250444489867], " +
        "[167.43803, -29.03719], [167.583317, -29.037381], [167.58331697583935, -29.03744011447325], [167.7285250024388, -29.037514998454153], [167.728525, -29.03749], [167.873835, -29.037419], [167.87383543708486, -29.037703808329873], [168.018612, -29.037121], " +
        "[168.0186121103674, -29.03714161109612], [168.163842, -29.03656], [168.1650939339767, -29.247683610268638], [168.164004, -29.036724], [168.309341, -29.036127], [168.3110870459225, -29.30068025473746], [168.311176, -29.30068], [168.312472, -29.567161], " +
        "[168.31243194795024, -29.56716111631554], [168.31443, -29.812612], [168.31388505737894, -29.812615143334597], [168.315886, -30.077081], [168.169234, -30.077883], [168.16913368505345, -30.06147402418803], [168.169224, -30.077737], [168.022447, -30.078317], " +
        "[168.02181920125142, -29.924959173336568], [168.0221, -30.078254], [167.875293, -30.078413], [167.8752929333776, -30.078235509309092]]," + //holes
        "[[167.43638852926597, -29.811913377451322], [167.43642819713568, -29.81191343893342], [167.43660948310222, -29.684470839430233], [167.43638852926597, -29.811913377451322]], " +
        "[[167.2900169281376, -29.811700260790584], [167.29007609051774, -29.811700416752192], [167.29022481985885, -29.765019899914726], [167.2900169281376, -29.811700260790584]], " +
        "[[167.72865676499967, -29.812149953736277], [167.7287401903084, -29.81214997654223], [167.72874, -29.812], [167.72893197342373, -29.81199982820994], [167.72851531939722, -29.568503012044204], [167.72851327553326, -29.568503011862287], [167.72865676499967, -29.812149953736277]], " +
        "[[167.87424106545097, -29.302014822030415], [167.87432742269175, -29.30201461402921], [167.87418553426855, -29.265830214765142], [167.87424106545097, -29.302014822030415]], " +
        "[[168.1652103335658, -29.3030088541673], [168.16605788758287, -29.446580625201833], [168.16556735186845, -29.303245228857072], [168.165381, -29.303246], [168.16537977124085, -29.303008170411644], [168.1652103335658, -29.3030088541673]], " +
        "[[168.02088551865063, -29.647294313012004], [168.02133932508806, -29.811843292379823], [168.02135614030843, -29.811843274349446], [168.021356, -29.811809], [168.02162340579383, -29.811807949652078], [168.02088551865063, -29.647294313012004]]]}";
    Polygon[] polygons =Polygon.fromGeoJSON(geoJson);
    List<Tessellator.Triangle> result = Tessellator.tessellate(polygons[0]);
    assertEquals(result.size(), 84);
  }
}
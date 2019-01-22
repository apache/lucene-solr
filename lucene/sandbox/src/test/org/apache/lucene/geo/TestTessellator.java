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
    List<Tessellator.Triangle> tessellation = Tessellator.tessellate(polygons[0]);
    assertEquals(tessellation.size(), 84);
    for (Tessellator.Triangle t : tessellation) {
      checkTriangleEdgesFromPolygon(polygons[0], t);
    }
  }

  public void testLUCENE8534() throws ParseException {
    String geoJson = "{\"type\":\"Polygon\",\"coordinates\":[[[168.412605,-32.061828],[168.41260500337557,-32.06164814731918],[168.263154,-32.061754],[168.263074,-31.795333],[168.2631866330167,-31.79533292075007],[168.26293615809584,-31.55183198959802],[168.26271862830876,-31.55183199836296]," +
        "[168.26260885857246,-31.79551898342183],[168.262799,-31.795519],[168.262922,-32.061969],[168.113391,-32.061955],[168.1136947020627,-31.797506925167987],[168.1134623401242,-31.7975067304478],[168.112867,-32.061933],[167.96342,-32.061572],[167.964447,-31.795078],[167.96462554945853,-31.79507843013861]," +
        "[167.96521264500555,-31.551376165945904],[167.965145,-31.551376],[167.9663078329189,-31.287013079577566],[167.966251,-31.287013],[167.9664724470441,-31.186852765132446],[167.966135,-31.286996],[167.96583002270634,-31.28699509215832],[167.96514242732414,-31.530648904745615],[167.96518,-31.530649]," +
        "[167.964244373485,-31.795342905910022],[167.964267,-31.795343],[167.963051,-32.06191],[167.813527,-32.061286],[167.81515841152935,-31.796764131690956],[167.815107,-31.796764],[167.8163675951437,-31.55101526478777],[167.81635023954297,-31.551015225373174],[167.814827,-31.796834]," +
        "[167.81479823247224,-31.796833898826222],[167.813495,-32.061159],[167.664068,-32.060513],[167.66581,-31.794011],[167.6658519100183,-31.794011179736117],[167.6677495759609,-31.550438401064135],[167.667432,-31.550437],[167.66930180157829,-31.286073839134556],[167.669105,-31.286073],[167.670807,-31.019532]," +
        "[167.818843,-31.020159],[167.8175723936035,-31.284543327213736],[167.81766095836642,-31.284543526532044],[167.818971,-31.020062],[167.967033,-31.020499],[167.96703262843647,-31.020609267886275],[168.114968,-31.020815],[168.1149445990616,-31.05814524188174],[168.114978,-31.020912],[168.26306,-31.021035]," +
        "[168.2631849793437,-31.203987591682104],[168.263163,-31.021002],[168.411259,-31.020914],[168.41125954741193,-31.02123593258559],[168.5589863328454,-31.020786105561243],[168.558986,-31.020705],[168.707027,-31.020199],[168.70828992266655,-31.242361611483734],[168.707298,-31.020426],[168.855538,-31.019789]," +
        "[168.85713808565947,-31.284233200286536],[168.857209,-31.284233],[168.8583969293829,-31.54547348363567],[168.86057,-31.796021],[168.86004803213373,-31.796023826818654],[168.862202,-32.060514],[168.712722,-32.061376],[168.71099229524427,-31.796760977737968],[168.7108263042178,-31.79676167516991],[168.712468,-32.061301]," +
        "[168.56291,-32.061787],[168.561684,-31.795261],[168.56198761104602,-31.795260018704994],[168.560821,-31.530975],[168.56092374559077,-31.530974570518158],[168.56001677082173,-31.287057906497665],[168.5597021283975,-31.287058866102726],[168.5607530382453,-31.530880020491022],[168.560769,-31.53088]," +
        "[168.56079128925168,-31.539754620482725],[168.560842,-31.55152],[168.56082083893278,-31.551520031401303],[168.56143311036655,-31.7953001584517],[168.561622,-31.7953],[168.562045,-32.0617],[168.412605,-32.061828]]," +
        "[[168.41212499436773,-31.68171617103951],[168.41200593405762,-31.551740860609502],[168.411912,-31.551741],[168.41154546767467,-31.416898111348704],[168.41158059852074,-31.53102923335134],[168.411729,-31.531029],[168.41212499436773,-31.68171617103951]]," +
        "[[168.7083938476212,-31.28652950649234],[168.70945084576658,-31.485690997091577],[168.70886199577689,-31.28667838236468],[168.708488,-31.28668],[168.7084873259438,-31.28652918474386],[168.7083938476212,-31.28652950649234]]," +
        "[[168.71121460687698,-31.795031659971823],[168.71136127361123,-31.79503081865431],[168.71038567290682,-31.657182838382653],[168.71121460687698,-31.795031659971823]]," +
        "[[167.81624041598312,-31.53023516975434],[167.81634270442586,-31.530235525706665],[167.81676369867318,-31.434841665952604],[167.81624041598312,-31.53023516975434]]]}";
    Polygon[] polygons =Polygon.fromGeoJSON(geoJson);
    List<Tessellator.Triangle> tessellation = Tessellator.tessellate(polygons[0]);
    assertEquals(113, tessellation.size());
    for (Tessellator.Triangle t : tessellation) {
      checkTriangleEdgesFromPolygon(polygons[0], t);
    }
  }

  public void testInvalidPolygon()  throws Exception {
    String wkt = "POLYGON((0 0, 1 1, 0 1, 1 0, 0 0))";
    Polygon polygon = (Polygon)SimpleWKTShapeParser.parse(wkt);
    expectThrows( IllegalArgumentException.class, () -> {Tessellator.tessellate(polygon); });
  }

  public void testLUCENE8550()  throws Exception {
    String wkt = "POLYGON((24.04725 59.942,24.04825 59.94125,24.04875 59.94125,24.04875 59.94175,24.048 59.9425,24.0475 59.94275,24.0465 59.94225,24.046 59.94225,24.04575 59.9425,24.04525 59.94225,24.04725 59.942))";
    Polygon polygon = (Polygon)SimpleWKTShapeParser.parse(wkt);
    List<Tessellator.Triangle> tessellation = Tessellator.tessellate(polygon);
    assertTrue(tessellation.size() == 8);
    for (Tessellator.Triangle t : tessellation) {
      checkTriangleEdgesFromPolygon(polygon, t);
    }
  }

  public void testLUCENE8559()  throws Exception {
    String wkt = "POLYGON((-0.1348674 51.7458255,-0.1345884 51.7455067,-0.1329898 51.745314,-0.1326358 51.745314,-0.1324105 51.744404,-0.131981 51.7444423,-0.1312196 51.7445102,-0.1310908 51.7456794,-0.1319706 51.7460713,-0.1343095 51.7465828,-0.1348674 51.7458255)," +
        "(-0.1322388 51.7447959,-0.1322388 51.7454336,-0.1318633 51.7457126,-0.1313912 51.7456262,-0.1318985 51.7448032,-0.1322388 51.7447959))";
    Polygon polygon = (Polygon)SimpleWKTShapeParser.parse(wkt);
    List<Tessellator.Triangle> tessellation = Tessellator.tessellate(polygon);
    assertTrue(tessellation.size() > 0);
    for (Tessellator.Triangle t : tessellation) {
      checkTriangleEdgesFromPolygon(polygon, t);
    }
  }

  public void testLUCENE8556()  throws Exception {
    String wkt ="POLYGON((-111.4765 68.321,-111.47625 68.32225,-111.4765 68.3225,-111.478 68.3225,-111.47825 68.32275,-111.479 68.32275,-111.47975 68.32325,-111.48125 68.324,-111.4815 68.32375,-111.48175 68.32375," +
        "-111.48225 68.32425,-111.48275 68.32425,-111.483 68.324,-111.4845 68.324,-111.48475 68.32425,-111.4845 68.32475,-111.48425 68.3245,-111.483 68.325,-111.4835 68.325,-111.48425 68.3255,-111.48525 68.3255,-111.4855 68.32575," +
        "-111.4855 68.32525,-111.486 68.32475,-111.48725 68.3245,-111.4875 68.32475,-111.48725 68.325,-111.487 68.325,-111.4865 68.32525,-111.487 68.32575,-111.486465 68.326385,-111.486 68.326,-111.48575 68.32625," +
        "-111.48525 68.32625,-111.485 68.326,-111.48375 68.326,-111.48225 68.3265,-111.483 68.3265,-111.48325 68.32675,-111.4835 68.3265,-111.48675 68.3265,-111.487 68.32675,-111.48675 68.32725,-111.4865 68.327," +
        "-111.48375 68.32775,-111.485 68.32775,-111.48525 68.3275,-111.4855 68.3275,-111.486 68.32775,-111.48625 68.3275,-111.48675 68.3275,-111.48725 68.327,-111.48775 68.327,-111.4875 68.32625,-111.488 68.32625," +
        "-111.48825 68.32675,-111.49025 68.327,-111.49025 68.32675,-111.4905 68.3265,-111.49075 68.3265,-111.49125 68.326,-111.492 68.32575,-111.4945 68.32575,-111.49475 68.3255,-111.49525 68.3255,-111.4955 68.32525,-111.49625 68.32525," +
        "-111.4965 68.325,-111.49775 68.32425,-111.498 68.3245,-111.4985 68.3245,-111.49875 68.32425,-111.49925 68.32425,-111.5005 68.324,-111.50075 68.32375,-111.501 68.32375,-111.501 68.323,-111.5015 68.323,-111.50175 68.32325,-111.5015 68.3235," +
        "-111.5025 68.32375,-111.50275 68.3235,-111.504 68.32375,-111.50425 68.3235,-111.50525 68.32325,-111.5055 68.3235,-111.506 68.3235,-111.50625 68.32325,-111.5065 68.3225,-111.5075 68.3225,-111.50775 68.32275,-111.50825 68.32275," +
        "-111.5085 68.3225,-111.50875 68.3225,-111.509 68.32275,-111.5125 68.32275,-111.51325 68.32225,-111.4765 68.321))";
    Polygon polygon = (Polygon) SimpleWKTShapeParser.parse(wkt);
    List<Tessellator.Triangle> tessellation = Tessellator.tessellate(polygon);
    assertTrue(tessellation.size() > 0);
    for (Tessellator.Triangle t : tessellation) {
      checkTriangleEdgesFromPolygon(polygon, t);
    }
  }

  public void testTriangle() throws Exception {
    String wkt = "POLYGON((0 0, 1 0, 1 1, 0 0))";
    Polygon polygon = (Polygon) SimpleWKTShapeParser.parse(wkt);
    List<Tessellator.Triangle> tessellation = Tessellator.tessellate(polygon);
    assertTrue(tessellation.size() == 1);
    for (Tessellator.Triangle t : tessellation) {
      checkTriangleEdgesFromPolygon(polygon, t);
    }
  }

  public void testTriangleWithHole() throws Exception {
    String wkt = "POLYGON((0 0, 1 0, 1 1, 0 0 ),(0.35 0.25, 0.85 0.75, 0.65 0.35, 0.35 0.25))";
    Polygon polygon = (Polygon) SimpleWKTShapeParser.parse(wkt);
    List<Tessellator.Triangle> tessellation = Tessellator.tessellate(polygon);
    assertTrue(tessellation.size() == 6);
    for (Tessellator.Triangle t : tessellation) {
      checkTriangleEdgesFromPolygon(polygon, t);
    }
  }

  public void testSquare() throws Exception {
    String wkt = "POLYGON((0 0, 0 1, 1 1, 1 0, 0 0))";
    Polygon polygon = (Polygon) SimpleWKTShapeParser.parse(wkt);
    List<Tessellator.Triangle> tessellation = Tessellator.tessellate(polygon);
    assertTrue(tessellation.size() == 2);
    for (Tessellator.Triangle t : tessellation) {
      checkTriangleEdgesFromPolygon(polygon, t);
    }
  }

  public void testSquareWithHole() throws Exception {
    String wkt = "POLYGON((0 0, 0 1, 1 1, 1 0, 0 0), (0.25 0.25, 0.25 0.75, 0.75 0.75, 0.75 0.25, 0.25 0.25))";
    Polygon polygon = (Polygon) SimpleWKTShapeParser.parse(wkt);
    List<Tessellator.Triangle> tessellation = Tessellator.tessellate(polygon);
    assertTrue(tessellation.size() == 8);
    for (Tessellator.Triangle t : tessellation) {
      checkTriangleEdgesFromPolygon(polygon, t);
    }
  }

  public void testEdgesFromPolygon() {
    Polygon poly = GeoTestUtil.nextPolygon();
    List<Tessellator.Triangle> tessellation = Tessellator.tessellate(poly);
    assertTrue(tessellation.size() > 0);
    for (Tessellator.Triangle t : tessellation) {
      checkTriangleEdgesFromPolygon(poly, t);
    }
  }

  public void testEdgesFromPolygonWithHoles() {
    Polygon poly = GeoTestUtil.createRegularPolygon(0.0, 0.0, 1000000, 500);
    Polygon inner = GeoTestUtil.createRegularPolygon(0.0, 0.0, 10000, 200);
    poly = new Polygon(poly.getPolyLats(), poly.getPolyLons(), inner);
    List<Tessellator.Triangle> tessellation = Tessellator.tessellate(poly);
    assertTrue(tessellation.size() > 0);
    for (Tessellator.Triangle t : tessellation) {
      checkTriangleEdgesFromPolygon(poly, t);
    }
  }

  public void testPolygonWithCoplanarPoints() {
    Polygon poly = GeoTestUtil.createRegularPolygon(0.0, 0.0, 1000000, 50);
    Polygon inner = new Polygon(new double[] {-1.0, -1.0, 0.5, 1.0, 1.0, 0.5, -1.0},
        new double[]{1.0, -1.0, -0.5, -1.0, 1.0, 0.5, 1.0});
    Polygon inner2 = new Polygon(new double[] {-1.0, -1.0, 0.5, 1.0, 1.0, 0.5, -1.0},
        new double[]{-2.0, -4.0, -3.5, -4.0, -2.0, -2.5, -2.0});
    poly = new Polygon(poly.getPolyLats(), poly.getPolyLons(), inner, inner2);
    List<Tessellator.Triangle> tessellation = Tessellator.tessellate(poly);
    assertTrue(tessellation.size() > 0);
    for (Tessellator.Triangle t : tessellation) {
      checkTriangleEdgesFromPolygon(poly, t);
    }
  }

  private void checkTriangleEdgesFromPolygon(Polygon p, Tessellator.Triangle t) {
    assertEquals(t.fromPolygon(0), edgeFromPolygon(p, t.getLon(0), t.getLat(0), t.getLon(1), t.getLat(1)));
    assertEquals(t.fromPolygon(1), edgeFromPolygon(p, t.getLon(1), t.getLat(1), t.getLon(2), t.getLat(2)));
    assertEquals(t.fromPolygon(2), edgeFromPolygon(p, t.getLon(2), t.getLat(2), t.getLon(0), t.getLat(0)));
  }

  private boolean edgeFromPolygon(Polygon p, double aLon, double aLat, double bLon, double bLat) {
    for (int i =0; i < p.getPolyLats().length - 1; i++) {
      if (p.getPolyLon(i) == aLon && p.getPolyLat(i) == aLat && p.getPolyLon(i + 1) == bLon && p.getPolyLat(i + 1) == bLat) {
        return true;
      }
      if (p.getPolyLon(i) == bLon && p.getPolyLat(i) == bLat && p.getPolyLon(i + 1) == aLon && p.getPolyLat(i + 1) == aLat) {
        return true;
      }
    }
    if (p.getHoles() != null && p.getHoles().length > 0) {
      for (Polygon hole : p.getHoles()) {
        if (edgeFromPolygon(hole, aLon, aLat, bLon, bLat)) {
          return true;
        }
      }
    }
    return false;
  }
}
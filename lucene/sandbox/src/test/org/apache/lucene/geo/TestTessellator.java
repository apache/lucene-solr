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

  public void testComplexPolygon01() throws Exception {
    String wkt = "POLYGON((58.8792517 54.9160937, 58.8762477 54.9154524, 58.8735011 54.9140217, 58.8726428 54.9127389, 58.8731146 54.9122507, 58.8741877 54.9120482, 58.8771918 54.9117028, 58.88011 54.913331, 58.8801175 54.9137036, 58.8805885 54.9143186, 58.8807109 54.9148604, 58.88011 54.915551, 58.8792517 54.9160937), " +
        "(58.8746003 54.9125589, 58.8766188 54.9137965, 58.8791419 54.9152275, 58.8798554 54.9151074, 58.8805548 54.9146087, 58.8801175 54.9137036, 58.8788867 54.9130833, 58.8790905 54.9128921, 58.8767533 54.9120561, 58.8748358 54.9122495, 58.8744557 54.9124049, 58.8746003 54.9125589))";
    Polygon polygon = (Polygon) SimpleWKTShapeParser.parse(wkt);
    List<Tessellator.Triangle> tessellation = Tessellator.tessellate(polygon);
    assertTrue(tessellation.size() > 0);
    for (Tessellator.Triangle t : tessellation) {
      checkTriangleEdgesFromPolygon(polygon, t);
    }
  }

  public void testComplexPolygon02() throws Exception {
    String wkt = "POLYGON((-0.5033651 48.7307175, -0.5036334 48.7300183, -0.5038592 48.7297349, -0.5044826 48.7295356, -0.5049852 48.72953, -0.504857 48.7301383, -0.5041382 48.7310084, -0.5033651 48.7307175), " +
        "(-0.504035 48.730838, -0.504282 48.730519, -0.504718 48.729958, -0.504778 48.72988, -0.504545 48.729797, -0.50448 48.729774, -0.503721 48.73073, -0.504035 48.730838), " +
        "(-0.50448 48.729774, -0.504545 48.729797, -0.504708 48.729597, -0.50458 48.729554, -0.504419 48.729753, -0.50448 48.729774))";
    Polygon polygon = (Polygon) SimpleWKTShapeParser.parse(wkt);
    List<Tessellator.Triangle> tessellation = Tessellator.tessellate(polygon);
    assertTrue(tessellation.size() > 0);
    for (Tessellator.Triangle t : tessellation) {
      checkTriangleEdgesFromPolygon(polygon, t);
    }
  }

  public void testComplexPolygon03() throws Exception {
    String wkt ="POLYGON((57.7258102 -20.1927474, 57.7257611 -20.192854, 57.7260971 -20.1929559, 57.726191 -20.1929232, 57.7262648 -20.1926211, 57.7262165 -20.1925544, 57.7260649 -20.1924877, 57.7259684 -20.1924678, 57.7259333 -20.1925297, 57.7258102 -20.1927474)," +
        " (57.7259333 -20.1925297, 57.7258471 -20.1927671, 57.7259774 -20.1928078, 57.7260433 -20.1925557, 57.7259333 -20.1925297))";
    Polygon polygon = (Polygon) SimpleWKTShapeParser.parse(wkt);
    List<Tessellator.Triangle> tessellation = Tessellator.tessellate(polygon);
    assertTrue(tessellation.size() > 0);
    for (Tessellator.Triangle t : tessellation) {
      checkTriangleEdgesFromPolygon(polygon, t);
    }
  }

  public void testComplexPolygon04() throws Exception {
    String wkt ="POLYGON((139.2749646 36.2742799, 139.2747468 36.2743137, 139.2747057 36.2743705, 139.2745531 36.2743918, 139.2744944 36.2743563, 139.2719227 36.2747799, 139.2719021 36.2748249, 139.2723724 36.2762706, 139.2724692 36.2765445, 139.2725362 36.2765573, 139.2754328 36.2760613, 139.2749646 36.2742799), " +
        "(139.2726473 36.2762561, 139.2726277 36.2760151, 139.2723528 36.2760297, 139.2723724 36.2762706, 139.2726473 36.2762561))";
    Polygon polygon = (Polygon) SimpleWKTShapeParser.parse(wkt);
    List<Tessellator.Triangle> tessellation = Tessellator.tessellate(polygon);
    assertTrue(tessellation.size() > 0);
    for (Tessellator.Triangle t : tessellation) {
      checkTriangleEdgesFromPolygon(polygon, t);
    }
  }

  public void testComplexPolygon05() throws Exception {
    String wkt ="POLYGON((8.6778468 49.8622443, 8.6782001 49.8622443, 8.6786272 49.8622443, 8.6790127 49.8622444, 8.6790127 49.8620355, 8.678775 49.8620355, 8.6780348 49.8620354, 8.6778468 49.8620354, 8.6778468 49.8622443)," +
        " (8.6785777 49.8621738, 8.6785775 49.8620923, 8.678253 49.8620926, 8.6782532 49.8621741, 8.6785777 49.8621738)," +
        " (8.6781491 49.8621742, 8.6781491 49.8620925, 8.6779802 49.8620925, 8.6779802 49.8621742, 8.6781491 49.8621742))";
    Polygon polygon = (Polygon) SimpleWKTShapeParser.parse(wkt);
    List<Tessellator.Triangle> tessellation = Tessellator.tessellate(polygon);
    assertTrue(tessellation.size() > 0);
    for (Tessellator.Triangle t : tessellation) {
      checkTriangleEdgesFromPolygon(polygon, t);
    }
  }

  public void testComplexPolygon06() throws Exception {
    String wkt ="POLYGON((-77.578272 38.7906104, -77.5784061 38.7901379, -77.5785349 38.7897198, -77.5786743 38.7894522, -77.5787441 38.7892306, -77.578846 38.7891679," +
        " -77.5789104 38.7891762, -77.5789747 38.789239, -77.5789747 38.7893979, -77.5789694 38.789586, -77.5789104 38.7897449, -77.5789104 38.7898494," +
        " -77.5789104 38.7900083, -77.5789157 38.7901714, -77.5789157 38.7903052, -77.5790659 38.7903972, -77.5791786 38.7905101, -77.5792215 38.7905979," +
        " -77.5789962 38.7906439, -77.5787977 38.7905268, -77.5786529 38.7904724, -77.5785027 38.7905352, -77.578272 38.7906104))";
    Polygon polygon = (Polygon) SimpleWKTShapeParser.parse(wkt);
    List<Tessellator.Triangle> tessellation = Tessellator.tessellate(polygon);
    assertTrue(tessellation.size() > 0);
    for (Tessellator.Triangle t : tessellation) {
      checkTriangleEdgesFromPolygon(polygon, t);
    }
  }

  public void testComplexPolygon07() throws Exception {
    String wkt ="POLYGON((27.481388 53.871276, 27.481388 53.870876, 27.4809477 53.870876, 27.4808096 53.870876, 27.480293 53.870876, 27.480287 53.871276, 27.481388 53.871276)," +
        " (27.481145 53.870998, 27.481145 53.871173, 27.480674 53.871173, 27.480674 53.870998, 27.481145 53.870998))";
    Polygon polygon = (Polygon) SimpleWKTShapeParser.parse(wkt);
    List<Tessellator.Triangle> tessellation = Tessellator.tessellate(polygon);
    assertTrue(tessellation.size() > 0);
    for (Tessellator.Triangle t : tessellation) {
      checkTriangleEdgesFromPolygon(polygon, t);
    }
  }

  public void testComplexPolygon08() throws Exception {
    String wkt ="POLYGON((27.473089 53.862247, 27.473089 53.86185, 27.4726752 53.86185, 27.4726755 53.8617698, 27.4725118 53.8617698, 27.4725116 53.86185, 27.471994 53.86185, 27.471994 53.862247," +
        " 27.473089 53.862247), (27.472547 53.861969, 27.472847 53.861969, 27.472852 53.862163, 27.472375 53.862163, 27.472375 53.861969, 27.472547 53.861969))";
    Polygon polygon = (Polygon) SimpleWKTShapeParser.parse(wkt);
    List<Tessellator.Triangle> tessellation = Tessellator.tessellate(polygon);
    assertTrue(tessellation.size() > 0);
    for (Tessellator.Triangle t : tessellation) {
      checkTriangleEdgesFromPolygon(polygon, t);
    }
  }

  public void testComplexPolygon09() throws Exception {
    String wkt ="POLYGON((27.4822056 53.9262047, 27.482123 53.9262047, 27.4820878 53.9262047, 27.4816412 53.9262047, 27.4816412 53.9265967, 27.4821202 53.9265967, 27.4826562 53.9265967, 27.4826562 53.9262047," +
        " 27.4823321 53.9262047, 27.4822831 53.9262047, 27.4822056 53.9262047)," +
        " (27.482419 53.9263193, 27.482419 53.9265023, 27.4821217 53.9265023, 27.481969 53.9265023, 27.481969 53.9263193, 27.482419 53.9263193))";
    Polygon polygon = (Polygon) SimpleWKTShapeParser.parse(wkt);
    List<Tessellator.Triangle> tessellation = Tessellator.tessellate(polygon);
    assertTrue(tessellation.size() > 0);
    for (Tessellator.Triangle t : tessellation) {
      checkTriangleEdgesFromPolygon(polygon, t);
    }
  }

  public void testComplexPolygon10() throws Exception {
    String wkt ="POLYGON((12.3480275 49.1830779, 12.3481411 49.1830974, 12.3481318 49.1831254, 12.3482695 49.1831485, 12.348275 49.1831181, 12.3486026 49.1831619, 12.3486007 49.1831728, 12.3486919 49.1831838, 12.3487068 49.1831254, 12.3487505 49.1831275, 12.3487501 49.1831345, 12.3487603 49.1831348, 12.3487608 49.1831278, 12.3488143 49.1831172, 12.3488222 49.1831239, 12.348831 49.183119, 12.3488231 49.1831123, 12.3488259 49.1830731," +
        " 12.3488361 49.1830697, 12.348831 49.1830637, 12.3488198 49.183067, 12.3487724 49.1830393, 12.3487724 49.1830311, 12.3487631 49.1830317, 12.3487621 49.1830399, 12.348731 49.1830323, 12.3489338 49.1823572, 12.3489617 49.1823499, 12.3489841 49.1823372, 12.3489831 49.1823171, 12.3489738 49.1823025, 12.3489543 49.1822934, 12.3489217 49.1822915," +
        " 12.3489329 49.1822447, 12.3487124 49.1822222, 12.3486965 49.18228, 12.348115 49.1822167, 12.348128 49.1821559, 12.3479326 49.182131, 12.3479233 49.1821894, 12.3479168 49.1821711, 12.3478917 49.1821638, 12.3478573 49.1821699, 12.3478387 49.1821857, 12.3478405 49.1822046, 12.3478498 49.1822167, 12.3478722 49.1822253, 12.3478833 49.1822253, 12.347713 49.1828626, 12.3480806 49.1829168, 12.3480275 49.1830779)," +
        " (12.348571 49.1828869, 12.3487052 49.182425, 12.3480373 49.1823465, 12.34791 49.1828088, 12.3482676 49.1828517, 12.348571 49.1828869)," +
        " (12.3482676 49.1828517, 12.3482341 49.1829685, 12.348537 49.1830042, 12.348571 49.1828869, 12.3482676 49.1828517))";
    Polygon polygon = (Polygon) SimpleWKTShapeParser.parse(wkt);
    List<Tessellator.Triangle> tessellation = Tessellator.tessellate(polygon);
    assertTrue(tessellation.size() > 0);
    for (Tessellator.Triangle t : tessellation) {
      checkTriangleEdgesFromPolygon(polygon, t);
    }
  }

  public void testComplexPolygon11() throws Exception {
    String wkt ="POLYGON((-95.252045 42.897609, -95.251709 42.897569, -95.251523 42.897554, -95.25137 42.897559, -95.251315 42.897561, -95.250753 42.89763, -95.25024 42.897716, -95.249356 42.897835, -95.24884 42.897905, -95.248685 42.897924, -95.248686 42.89805, -95.248691 42.89843, -95.248693 42.898557, -95.234751 42.898871, -95.234631 42.890847, -95.237959 42.890779, -95.237885 42.886205, -95.249964 42.886255, -95.249943 42.894309, -95.248836 42.894259, -95.248759 42.895872, -95.252112 42.896047, -95.252045 42.897609)," +
        " (-95.248685 42.897924, -95.248686 42.897876, -95.248693 42.897732, -95.248696 42.897685, -95.248546 42.897171, -95.248097 42.89563, -95.247977 42.895217, -95.247948 42.895117, -95.247912 42.895, -95.247876 42.894882, -95.247835 42.89475, -95.247497 42.89365, -95.247449 42.893492, -95.247238 42.893441, -95.246999 42.893542, -95.246988 42.89369, -95.246984 42.893751, -95.24728 42.894877, -95.247289 42.89491, -95.247317 42.895016, -95.247345 42.895121, -95.247366 42.895203, -95.247384 42.895273, -95.247397 42.895323," +
        " -95.24752 42.895818, -95.247927 42.897456, -95.248063 42.898003, -95.248128 42.897991, -95.248154 42.897987, -95.24843 42.897953, -95.248523 42.897943, -95.248555 42.897938, -95.248652 42.897927, -95.248685 42.897924))";
    Polygon polygon = (Polygon) SimpleWKTShapeParser.parse(wkt);
    List<Tessellator.Triangle> tessellation = Tessellator.tessellate(polygon);
    assertTrue(tessellation.size() > 0);
    for (Tessellator.Triangle t : tessellation) {
      checkTriangleEdgesFromPolygon(polygon, t);
    }
  }

  public void testComplexPolygon12() throws Exception {
    String wkt ="POLYGON((-85.418489 41.768716, -85.418482 41.767212, -85.418481 41.766867, -85.408741 41.766911, -85.408745 41.763218, -85.41744 41.763171, -85.41744 41.763335, -85.418456 41.763335, -85.418455 41.763171, -85.420528 41.763171, -85.420843 41.766839, -85.420937 41.768716, -85.418489 41.768716)," +
        " (-85.418481 41.766867, -85.419141 41.766859, -85.419173 41.766858, -85.41923 41.766313, -85.418477 41.766272, -85.418481 41.766867))";
    Polygon polygon = (Polygon) SimpleWKTShapeParser.parse(wkt);
    List<Tessellator.Triangle> tessellation = Tessellator.tessellate(polygon);
    assertTrue(tessellation.size() > 0);
    for (Tessellator.Triangle t : tessellation) {
      checkTriangleEdgesFromPolygon(polygon, t);
    }
  }

  public void testComplexPolygon13() throws Exception {
    String wkt ="POLYGON((30.6852741 59.9232998, 30.6856122 59.9236242, 30.6859407 59.9236291, 30.6863851 59.9235177, 30.6867039 59.9233144, 30.6866169 59.9231159, 30.6864044 59.9229464, 30.6860566 59.9227285, 30.6855736 59.9228496, 30.6850036 59.9228012, 30.6851775 59.9229755, 30.6851496 59.9229971, 30.6850712 59.9230578, 30.6847911 59.923019, 30.6849843 59.923174, 30.6851872 59.9232078, 30.685361 59.9232127, 30.6852741 59.9232998)," +
        " (30.6851678 59.9231308, 30.6852544 59.9231618, 30.6853904 59.923171, 30.6855264 59.9231927, 30.6856625 59.9231865, 30.6857366 59.9232113, 30.6858912 59.923171, 30.6858418 59.9231122, 30.6857366 59.9230936, 30.6857181 59.9230223, 30.6856254 59.9229541, 30.6854399 59.9229634, 30.6853409 59.9229603, 30.6853162 59.9230037, 30.6851496 59.9229971, 30.6851431 59.9230657, 30.6851678 59.9231308))";
    Polygon polygon = (Polygon) SimpleWKTShapeParser.parse(wkt);
    List<Tessellator.Triangle> tessellation = Tessellator.tessellate(polygon);
    assertTrue(tessellation.size() > 0);
    for (Tessellator.Triangle t : tessellation) {
      checkTriangleEdgesFromPolygon(polygon, t);
    }
  }

  public void testComplexPolygon14() throws Exception {
    String wkt ="POLYGON((2.3579667 48.8897478, 2.3580261 48.8897557, 2.3580623 48.889755, 2.3581993 48.8897522, 2.3582021 48.889792, 2.3580413 48.8897989, 2.3580459 48.8898732, 2.3579903 48.8898759, 2.3579873 48.8898003, 2.3578478 48.8898083, 2.3578412 48.8897569, 2.3579667 48.8897478)," +
        " (2.3578983 48.8897613, 2.3579047 48.8897905, 2.3579492 48.8897885, 2.3579862 48.8897908, 2.3579873 48.8898003, 2.3580413 48.8897989, 2.3580334 48.8897887, 2.358032 48.8897769, 2.3580209 48.889777, 2.3579664 48.8897699, 2.3579528 48.8897707, 2.357947 48.8897684, 2.3579458 48.8897585, 2.3578983 48.8897613))";
    Polygon polygon = (Polygon) SimpleWKTShapeParser.parse(wkt);
    List<Tessellator.Triangle> tessellation = Tessellator.tessellate(polygon);
    assertTrue(tessellation.size() > 0);
    for (Tessellator.Triangle t : tessellation) {
      checkTriangleEdgesFromPolygon(polygon, t);
    }
  }

  public void testComplexPolygon15() throws Exception {
    String wkt ="POLYGON((-2.7996138 53.4243001, -2.7995616 53.4243095, -2.7995084 53.4243189, -2.7994612 53.4243274, -2.7995377 53.4243807, -2.7995906 53.4243689, -2.7996138 53.4243001)," +
        " (-2.7995616 53.4243095, -2.7995429 53.4243345, -2.7995084 53.4243189, -2.7995616 53.4243095))";
    Polygon polygon = (Polygon) SimpleWKTShapeParser.parse(wkt);
    List<Tessellator.Triangle> tessellation = Tessellator.tessellate(polygon);
    assertTrue(tessellation.size() > 0);
    for (Tessellator.Triangle t : tessellation) {
      checkTriangleEdgesFromPolygon(polygon, t);
    }
  }

  public void testComplexPolygon16() throws Exception {
    String wkt ="POLYGON((5.3247527 61.4108691, 5.3247243 61.4105839, 5.3250792 61.4107616, 5.325961 61.4108553, 5.3266624 61.4110128, 5.3270003 61.4110466, 5.3274267 61.4111918, 5.3274094 61.4112734," +
        " 5.3275956 61.411337, 5.328454 61.4117214, 5.3288879 61.4117593, 5.3293803 61.4119717, 5.3292581 61.412102, 5.3294948 61.4124709, 5.3288962 61.4128764, 5.3282449 61.4129021," +
        " 5.3274134 61.4130613, 5.3271761 61.413222, 5.3263619 61.413395, 5.3263619 61.413395, 5.3258351 61.4131221, 5.3255073 61.4131218, 5.325332 61.4129946, 5.3253043 61.4127856," +
        " 5.3250305 61.4128579, 5.3245279 61.4126489, 5.3244206 61.4124399, 5.3244415 61.4122399, 5.324192 61.4118966, 5.3242034 61.4117109, 5.3244695 61.4115646, 5.3250112 61.4113443, 5.3251052 61.4111494, 5.3247527 61.4108691))";
    Polygon polygon = (Polygon) SimpleWKTShapeParser.parse(wkt);
    List<Tessellator.Triangle> tessellation = Tessellator.tessellate(polygon);
    assertTrue(tessellation.size() > 0);
    for (Tessellator.Triangle t : tessellation) {
      checkTriangleEdgesFromPolygon(polygon, t);
    }
  }

 public void testComplexPolygon17() throws Exception {
    String wkt ="POLYGON((34.6110434 62.1752511, 34.6109864 62.1751687, 34.6115575 62.1749522, 34.6112716 62.1749876, 34.6109715 62.1750879," +
        " 34.6100197 62.1751666, 34.6101212 62.1750403, 34.6120273 62.1747823, 34.6122303 62.1746507, 34.6122529 62.1745243, 34.6126928 62.1743506," +
        " 34.6127717 62.1742295, 34.6133808 62.1740189, 34.6134823 62.1737767, 34.6077526 62.174577, 34.6077301 62.1745138, 34.6133695 62.1737135, " +
        "34.6133357 62.1736451, 34.6115085 62.1734924, 34.6100986 62.1737399, 34.6094445 62.1737715, 34.6093204 62.1737293, 34.6102227 62.1735082, " +
        "34.6100535 62.1731765, 34.6099069 62.1731081, 34.6093204 62.1730133, 34.6092414 62.1733081, 34.6079556 62.1742664, 34.6077075 62.1743453, " +
        "34.6070646 62.1749034, 34.6070082 62.1751614, 34.6065683 62.1757352, 34.6063428 62.1760353, 34.6063879 62.1762669, 34.606027 62.1767986, " +
        "34.6054292 62.1772987, 34.6050795 62.1773987, 34.604572 62.1775251, 34.6046848 62.177662, 34.6052374 62.1776409, 34.605948 62.1773987, " +
        "34.6066022 62.1770671, 34.6076962 62.1765564, 34.6078654 62.1761511, 34.6080684 62.1759247, 34.6082038 62.1755667, 34.6085524 62.1755425, " +
        "34.6090384 62.1755088, 34.6110434 62.1752511)," +
        " (34.6098618 62.1749455, 34.6119935 62.1745664, 34.6120386 62.1744559, 34.6098505 62.1748665, 34.6098618 62.1749455), " +
        " (34.6098731 62.1745717, 34.6119596 62.174219, 34.6119935 62.17414, 34.6098731 62.1745085, 34.6098731 62.1745717)," +
        " (34.6086549 62.1754193, 34.6086211 62.1745717, 34.6084632 62.1746296, 34.6085309 62.1754351, 34.6086549 62.1754193)," +
        " (34.6091963 62.1753298, 34.6091286 62.174577, 34.608982 62.1745822, 34.6090723 62.1753877, 34.6091963 62.1753298)," +
        " (34.6097264 62.1751508, 34.60967 62.1745717, 34.6095347 62.1745717, 34.6095798 62.1751508, 34.6097264 62.1751508))";
    Polygon polygon = (Polygon) SimpleWKTShapeParser.parse(wkt);
    List<Tessellator.Triangle> tessellation = Tessellator.tessellate(polygon);
    assertTrue(tessellation.size() > 0);
    for (Tessellator.Triangle t : tessellation) {
      checkTriangleEdgesFromPolygon(polygon, t);
    }
  }

  public void testComplexPolygon18() throws Exception {
    String wkt ="POLYGON((12.6819289 41.8071837, 12.6819002 41.8071515, 12.6818466 41.8070893, 12.6817871 41.8070219, 12.6817529 41.8069796," +
        " 12.6817157 41.8069336, 12.6817346 41.8069215, 12.6817535 41.8069094, 12.6818622 41.8068556, 12.6819313 41.8068215, 12.6820377 41.8067689," +
        " 12.682084 41.8068084, 12.6821448 41.8068602, 12.6821699 41.8068861, 12.6822902 41.8070106, 12.6823021 41.8070228, 12.6823363 41.8070582," +
        " 12.6823168 41.8070677, 12.6822974 41.8070771, 12.6822027 41.8071231, 12.682142 41.8071526, 12.6820748 41.8071853, 12.6820128 41.8072188," +
        " 12.6819934 41.807228, 12.6819741 41.8072373, 12.6819629 41.8072243, 12.6819289 41.8071837)," +
        " (12.6819289 41.8071837, 12.6817871 41.8070219, 12.6820535 41.8068825, 12.6822076 41.8070568, 12.6819289 41.8071837))";
    Polygon polygon = (Polygon) SimpleWKTShapeParser.parse(wkt);
    List<Tessellator.Triangle> tessellation = Tessellator.tessellate(polygon);
    assertTrue(tessellation.size() > 0);
    for (Tessellator.Triangle t : tessellation) {
      checkTriangleEdgesFromPolygon(polygon, t);
    }
  }

  public void testComplexPolygon19() throws Exception {
    String wkt ="POLYGON((14.1989238 40.8274753, 14.1990593 40.8275004, 14.1991793 40.8275226, 14.1993451 40.8275478, 14.1993761 40.8275525, 14.1994599 40.8275746, 14.1996909 40.8276174, 14.1996769 40.8276728, 14.1993975 40.8277665, " +
        "14.1993717 40.8277752, 14.1992074 40.8278304, 14.1990929 40.8278688, 14.1989635 40.8279122, 14.1988594 40.8276864, 14.1989238 40.8274753), (14.1993717 40.8277752, 14.1993975 40.8277665, 14.1995864 40.8276576, 14.1994599 40.8275746," +
        " 14.1993761 40.8275525, 14.1993451 40.8275478, 14.1993073 40.8276704, 14.1993717 40.8277752), (14.1990593 40.8275004, 14.1989907 40.8276889, 14.1990929 40.8278688, 14.1992074 40.8278304, 14.1991335 40.8276763, 14.1991793 40.8275226, 14.1990593 40.8275004))";
    Polygon polygon = (Polygon) SimpleWKTShapeParser.parse(wkt);
    List<Tessellator.Triangle> tessellation = Tessellator.tessellate(polygon);
    assertTrue(tessellation.size() > 0);
    for (Tessellator.Triangle t : tessellation) {
      checkTriangleEdgesFromPolygon(polygon, t);
    }
  }

  public void testComplexPolygon20() throws Exception {
    String wkt = "POLYGON((-6.0057153 37.378144, -6.0056993 37.3781273, -6.005663 37.3781481, -6.0056241 37.3781101, -6.0056938 37.3780656, " +
        "-6.0057319 37.3781066, -6.0057619 37.3780888, -6.0057645 37.3780916, -6.0057775 37.3781049, -6.0057153 37.378144), " +
        "(-6.0056993 37.3781273, -6.0057275 37.3781093, -6.0057052 37.3780871, -6.005677 37.378105, -6.0056993 37.3781273))";
    Polygon polygon = (Polygon) SimpleWKTShapeParser.parse(wkt);
    List<Tessellator.Triangle> tessellation = Tessellator.tessellate(polygon);
    assertTrue(tessellation.size() > 0);
    for (Tessellator.Triangle t : tessellation) {
      checkTriangleEdgesFromPolygon(polygon, t);
    }
  }

  public void testComplexPolygon21() throws Exception {
    String wkt ="POLYGON((60.3629011 55.1038828,60.3686333 55.0945319, 60.3696616 55.0944318, 60.3701429 55.094269, 60.3707555 55.094269, 60.3719807 55.0942941, 60.373184 55.0941939, 60.3738841 55.0938559, 60.3741685 55.0938809, 60.3752406 55.0940311, 60.3760938 55.0940687, 60.3765751 55.0941188, 60.3789161 55.1022181, 60.3629011 55.1038828)," +
        " (60.3685348 55.098134, 60.3685348 55.0980714, 60.3681634 55.0980759, 60.3681629 55.0980119, 60.3681738 55.0979431, 60.3682285 55.0978304, 60.3681848 55.0976426, 60.3681902 55.0974298, 60.3682887 55.0972671, 60.368409 55.0972264, 60.3685786 55.0972858, 60.3687044 55.0973234, 60.3688083 55.0972639, 60.368956 55.0972639, 60.3692185 55.0972545, 60.3692294 55.0973328, 60.3692732 55.0975612, 60.369306 55.0977177, 60.369306 55.0979681, 60.3690654 55.0980682, 60.3688411 55.0981308, 60.3685348 55.098134), " +
        "(60.3680535 55.098256, 60.3685348 55.0982529, 60.3685348 55.098134, 60.3685348 55.0980714, 60.3681634 55.0980759, 60.3680261 55.0980776, 60.3680535 55.098256))";
    Polygon polygon = (Polygon) SimpleWKTShapeParser.parse(wkt);
    List<Tessellator.Triangle> tessellation = Tessellator.tessellate(polygon);
    assertTrue(tessellation.size() > 0);
    for (Tessellator.Triangle t : tessellation) {
      checkTriangleEdgesFromPolygon(polygon, t);
    }
  }

  public void testComplexPolygon22() throws Exception {
    String wkt ="POLYGON((11.4224981 52.1936005, 11.4228777 52.1934599, 11.4231781 52.1933004, 11.424204 52.192568, 11.4236675 52.1919366, 11.4234208 52.1912986, 11.4233564 52.1909237, 11.4223763 52.1907738, 11.4217128 52.1907656, 11.4216977 52.1908798, 11.421629 52.1923707, 11.422541 52.1924299, 11.422777 52.1927784, 11.4226268 52.1934559, 11.4224981 52.1936005), " +
        "(11.4230184 52.1931194, 11.4229045 52.1928061, 11.4228337 52.1924324, 11.4230153 52.192289, 11.4233217 52.1919891, 11.4233614 52.1919891, 11.4234102 52.1919891, 11.4238071 52.1925876, 11.4232185 52.1930477, 11.4230184 52.1931194), " +
        "(11.4233029 52.1918642, 11.4233614 52.1919891, 11.4233217 52.1919891, 11.423267 52.191863, 11.4230829 52.191851, 11.4228576 52.1916768, 11.4219593 52.191568, 11.4217993 52.1914605, 11.4218547 52.1910641, 11.4225773 52.1909533, 11.4229569 52.1910092, 11.423142 52.1911144, 11.4233029 52.1918642))";
    Polygon polygon = (Polygon) SimpleWKTShapeParser.parse(wkt);
    List<Tessellator.Triangle> tessellation = Tessellator.tessellate(polygon);
    assertTrue(tessellation.size() > 0);
    for (Tessellator.Triangle t : tessellation) {
      checkTriangleEdgesFromPolygon(polygon, t);
    }
  }

  public void testComplexPolygon23() throws Exception {
    String wkt ="POLYGON((-8.8695167 38.5283886, -8.8695085 38.5280497, -8.8692041 38.5280497, -8.869208 38.528419, -8.8692324 38.5284411, -8.8693155 38.5284589, -8.8694778 38.5284138, -8.8695167 38.5283886)," +
        " (-8.8693914 38.5283407, -8.8692384 38.5283407, -8.8692383 38.5281424, -8.8693914 38.5281424, -8.8693914 38.5283297, -8.8693914 38.5283407)," +
        " (-8.8694452 38.5283297, -8.8693914 38.5283297, -8.8693914 38.5281424, -8.8693914 38.5281131, -8.8694451 38.5281131, -8.8694452 38.5283297))";
    Polygon polygon = (Polygon) SimpleWKTShapeParser.parse(wkt);
    List<Tessellator.Triangle> tessellation = Tessellator.tessellate(polygon);
    assertTrue(tessellation.size() > 0);
    for (Tessellator.Triangle t : tessellation) {
      checkTriangleEdgesFromPolygon(polygon, t);
    }
  }

  public void testComplexPolygon24() throws Exception {
    String wkt ="POLYGON((64.0209362 61.4681776, 64.0210248 61.4682157, 64.0211071 61.4682512, 64.0212391 61.468193, 64.021904 61.4681913, 64.0220258 61.468256, 64.0221224 61.4682435, 64.022299 61.4682161, 64.0222172 61.4681665, 64.0221553 61.4681319, 64.0218224 61.4679859, 64.0216788 61.4679836, 64.0216784 61.4679438, 64.0216361 61.4679187, 64.0215193 61.4679187, 64.0214889 61.4679414, 64.0214838 61.4679883, 64.021362 61.4679891, 64.0209362 61.4681776)," +
        " (64.0215237 61.4680208, 64.021635 61.4680208, 64.021635 61.4680058, 64.021635 61.4679288, 64.0215237 61.4679288, 64.0215237 61.4680058, 64.0215237 61.4680208)," +
        " (64.0215227 61.46806, 64.0215227 61.4681552, 64.0216321 61.4681552, 64.0216321 61.46806, 64.021725 61.46806, 64.021725 61.4680058, 64.021635 61.4680058, 64.021635 61.4680208, 64.0215237 61.4680208, 64.0215237 61.4680058, 64.0214238 61.4680058, 64.0214238 61.46806, 64.0215227 61.46806))";
    Polygon polygon = (Polygon) SimpleWKTShapeParser.parse(wkt);
    List<Tessellator.Triangle> tessellation = Tessellator.tessellate(polygon);
    assertTrue(tessellation.size() > 0);
    for (Tessellator.Triangle t : tessellation) {
      checkTriangleEdgesFromPolygon(polygon, t);
    }
  }

  public void testComplexPolygon25() throws Exception {
    String wkt ="POLYGON((8.6778468 49.8622443, 8.6782001 49.8622443, 8.6786272 49.8622443, 8.6790127 49.8622444, 8.6790127 49.8620355, 8.678775 49.8620355, 8.6780348 49.8620354, 8.6778468 49.8620354, 8.6778468 49.8622443)," +
        " (8.6785777 49.8621738, 8.6785775 49.8620923, 8.678253 49.8620926, 8.6782532 49.8621741, 8.6785777 49.8621738), (8.6786822 49.8622191, 8.6785776 49.8622191, 8.6785777 49.8621738, 8.6785775 49.8620923, 8.6785776 49.8620566, 8.6786822 49.8620566, 8.6786822 49.8620918, 8.6786822 49.8621737, 8.6786822 49.8622191)," +
        " (8.6786822 49.8621737, 8.6788512 49.8621737, 8.6788512 49.8620918, 8.6786822 49.8620918, 8.6786822 49.8621737)," +
        " (8.6781491 49.8622195, 8.6782531 49.8622195, 8.6782532 49.8621741, 8.678253 49.8620926, 8.6782531 49.862057, 8.6781491 49.862057, 8.6781491 49.8620925, 8.6781491 49.8621742, 8.6781491 49.8622195)," +
        " (8.6781491 49.8621742, 8.6781491 49.8620925, 8.6779802 49.8620925, 8.6779802 49.8621742, 8.6781491 49.8621742))";
    Polygon polygon = (Polygon) SimpleWKTShapeParser.parse(wkt);
    List<Tessellator.Triangle> tessellation = Tessellator.tessellate(polygon);
    assertTrue(tessellation.size() > 0);
    for (Tessellator.Triangle t : tessellation) {
      checkTriangleEdgesFromPolygon(polygon, t);
    }
  }

  public void testComplexPolygon26() throws Exception {
    String wkt ="POLYGON((-123.7404617 58.3125, -123.75 58.3125, -123.75 58.375, -123.6321531 58.3749956, -123.6317004 58.3749059, -123.6314612 58.3741376, -123.6306331 58.373089, -123.6296881 58.3723157, -123.627312 58.3710664, -123.625641 58.3692493, -123.625 58.3687661, -123.625 58.3633125, -123.6283038 58.3624226, -123.6296466 58.362358, -123.6306625 58.3621921, -123.6314938 58.3618889, -123.6330286 58.3616731, -123.634273 58.3611626, -123.6349202 58.3609982, -123.6361556 58.360838, -123.6379868 58.3597587, -123.6389829 58.3594443, -123.640572 58.3586312, -123.6421679 58.3572329, -123.6423102 58.3568794, -123.6429936 58.3565215, -123.6447649 58.3562455, -123.6460215 58.3558545, -123.6483288 58.3554625, -123.6488064 58.3551774, -123.6493181 58.3550985, -123.6509318 58.3551592, -123.6521902 58.355003, -123.6527222 58.3550521, -123.6538012 58.355484, -123.6565104 58.3556346, -123.6576767 58.3553995, -123.658255 58.3554777, -123.6590797 58.3554215, -123.661566 58.3547586, -123.6622838 58.3542689, -123.6639835 58.3543382, -123.6664585 58.3541285, -123.667106 58.3539433, -123.6678853 58.3534994, -123.6687755 58.3533241, -123.6690952 58.353079, -123.6702918 58.3525596, -123.6717506 58.3522191, -123.6758233 58.3523909, -123.6775504 58.3526168, -123.6778731 58.3525654, -123.6794879 58.3529018, -123.6800906 58.352943, -123.6808703 58.3527957, -123.6823335 58.3532423, -123.6843889 58.3531901, -123.6856506 58.3528894, -123.6862467 58.352881, -123.6872887 58.3522574, -123.687291 58.351837, -123.6869332 58.3510681, -123.6854944 58.3496119, -123.6855283 58.3485283, -123.6857475 58.3482165, -123.6874383 58.3476631, -123.6881526 58.347297, -123.6891555 58.3469904, -123.6918469 58.3458955, -123.6922003 58.3455393, -123.6924054 58.345487, -123.6924623 58.3454009, -123.6921451 58.3452257, -123.6923479 58.3449385, -123.6929675 58.3445964, -123.6931148 58.3443336, -123.6929389 58.3402113, -123.6931606 58.3394585, -123.6935341 58.3389129, -123.6939308 58.3387094, -123.6945457 58.3378975, -123.6972661 58.3371942, -123.6978071 58.3368515, -123.698247 58.3361456, -123.6993662 58.335543, -123.6997906 58.3351583, -123.7002932 58.3351122, -123.7021613 58.3346463, -123.7030156 58.3343057, -123.7047178 58.3342302, -123.7065843 58.3338261, -123.7089787 58.3336356, -123.7102074 58.3333633, -123.7107549 58.333408, -123.7110472 58.3333027, -123.7133154 58.3331443, -123.7153485 58.3323084, -123.7161434 58.331143, -123.715938 58.3302021, -123.7153031 58.3295345, -123.7145757 58.3291466, -123.7131432 58.3287541, -123.71271 58.3281785, -123.7127057 58.328026, -123.7130922 58.3279172, -123.7137255 58.3279707, -123.7137126 58.3278512, -123.7130951 58.3274557, -123.7133263 58.3266082, -123.7129943 58.3263959, -123.7132633 58.3259484, -123.7135818 58.3257238, -123.7151388 58.3251321, -123.716882 58.3249537, -123.717405 58.3250354, -123.7176916 58.3251773, -123.7198942 58.3251172, -123.7224908 58.3246186, -123.7251722 58.323832, -123.7262131 58.3231875, -123.7263272 58.3229945, -123.7262397 58.3227096, -123.7265877 58.3222214, -123.7268646 58.3220995, -123.7274534 58.3220414, -123.7280048 58.3215627, -123.7287641 58.3205496, -123.7296068 58.3189724, -123.7301585 58.3184731, -123.7322935 58.3175799, -123.7340084 58.3172445, -123.7346599 58.3168323, -123.7357182 58.3164392, -123.7366318 58.315868, -123.7370013 58.3158043, -123.7378098 58.3153726, -123.7380588 58.3150939, -123.738132 58.3146245, -123.737104 58.3136785, -123.7371457 58.3132172, -123.7377165 58.3129076, -123.7381332 58.3128319, -123.7396672 58.3128703, -123.7404617 58.3125)," +
        " (-123.7169112 58.3253866, -123.7163295 58.3254736, -123.7157654 58.325812, -123.7158944 58.327008, -123.7161972 58.3267874, -123.7164083 58.3261293, -123.7169112 58.3253866)," +
        " (-123.7153958 58.3262135, -123.7152468 58.3262208, -123.7149697 58.3266806, -123.7148026 58.3274708, -123.7152792 58.3278612, -123.7155639 58.3274097, -123.7153958 58.3262135), (-123.7497313 58.350133, -123.7498506 58.3502042, -123.75 58.3502944, -123.7494451 58.3503224, -123.7490112 58.3500917, -123.7497313 58.350133))";
    Polygon polygon = (Polygon) SimpleWKTShapeParser.parse(wkt);
    List<Tessellator.Triangle> tessellation = Tessellator.tessellate(polygon);
    assertTrue(tessellation.size() > 0);
    for (Tessellator.Triangle t : tessellation) {
      checkTriangleEdgesFromPolygon(polygon, t);
    }
  }

  public void testComplexPolygon27() throws Exception {
    String wkt ="POLYGON((130.67658 33.4549747, 130.6766161 33.454976, 130.6766609 33.4549775, 130.6766642 33.454912, 130.6766212 33.4549105, 130.6766102 33.4549066, 130.6766061 33.454879, 130.6765768 33.4548779, 130.6765765 33.4548831," +
        " 130.6765691 33.4548828, 130.6765693 33.4548793, 130.6765507 33.4548786, 130.6765509 33.4548761, 130.6765281 33.4548753, 130.6765273 33.4548919, 130.6765322 33.454892, 130.6765315 33.4549065, 130.6765323 33.4549065," +
        " 130.6765321 33.4549107, 130.6765257 33.4549105, 130.6765238 33.454949, 130.6765515 33.45495, 130.6765512 33.4549572, 130.6765808 33.4549583, 130.67658 33.4549747)," +
        " (130.6765844 33.4549234, 130.6765847 33.4549188, 130.6765847 33.4549188, 130.6765844 33.4549234))";
    Polygon polygon = (Polygon) SimpleWKTShapeParser.parse(wkt);
    List<Tessellator.Triangle> tessellation = Tessellator.tessellate(polygon);
    assertTrue(tessellation.size() > 0);
    for (Tessellator.Triangle t : tessellation) {
      checkTriangleEdgesFromPolygon(polygon, t);
    }
  }

  public void testComplexPolygon28() throws Exception {
    String wkt ="POLYGON((33.3275991 -8.9353026, 33.3276122 -8.9353021, 33.3276139 -8.9353425, 33.3276095 -8.9353427, 33.3276107 -8.9353706, 33.3276074 -8.9353707, 33.3276087 -8.9354024, 33.3275766 -8.9354038, 33.3275753 -8.9353739," +
        " 33.3275354 -8.9353756, 33.3275342 -8.9353464, 33.3275184 -8.935347, 33.3275167 -8.9353066, 33.3275381 -8.9353057, 33.3275375 -8.9352901, 33.3275598 -8.9352892, 33.3275594 -8.9352808, 33.3275981 -8.9352792, 33.3275991 -8.9353026)," +
        " (33.3275601 -8.9353046, 33.3275599 -8.9352988, 33.3275601 -8.9353046, 33.3275601 -8.9353046))";
    Polygon polygon = (Polygon) SimpleWKTShapeParser.parse(wkt);
    List<Tessellator.Triangle> tessellation = Tessellator.tessellate(polygon);
    assertTrue(tessellation.size() > 0);
    for (Tessellator.Triangle t : tessellation) {
      checkTriangleEdgesFromPolygon(polygon, t);
    }
  }

  private void checkTriangleEdgesFromPolygon(Polygon p, Tessellator.Triangle t) {
    //System.out.println("LINESTRING(" +t.getLon(0) + " " + t.getLat(0)+ "," + t.getLon(1) + " " +  t.getLat(1)+ ")");
    assertEquals(t.fromPolygon(0), edgeFromPolygon(p, t.getLon(0), t.getLat(0), t.getLon(1), t.getLat(1)));
    //System.out.println("LINESTRING(" +t.getLon(1) + " " + t.getLat(1)+ "," + t.getLon(2) + " " +  t.getLat(2)+ ")");
    assertEquals(t.fromPolygon(1), edgeFromPolygon(p, t.getLon(1), t.getLat(1), t.getLon(2), t.getLat(2)));
    //System.out.println("LINESTRING(" +t.getLon(2) + " " + t.getLat(2)+ "," + t.getLon(0) + " " +  t.getLat(0)+ ")");
    assertEquals(t.fromPolygon(2), edgeFromPolygon(p, t.getLon(2), t.getLat(2), t.getLon(0), t.getLat(0)));
  }

  private boolean edgeFromPolygon(Polygon p, double aLon, double aLat, double bLon, double bLat) {
    for (int i = 0; i < p.getPolyLats().length - 1; i++) {
      if (pointInLine(p.getPolyLon(i), p.getPolyLat(i), p.getPolyLon(i + 1), p.getPolyLat(i + 1), aLon, aLat) &&
          pointInLine(p.getPolyLon(i), p.getPolyLat(i), p.getPolyLon(i + 1), p.getPolyLat(i + 1), bLon, bLat)) {
        return true;
      }
      if (p.getPolyLon(i) != p.getPolyLon(i + 1) || p.getPolyLat(i) != p.getPolyLat(i + 1)) {
        //Check for co-planar points
        int j = i + 2;
        while (j - i < p.getPolyLats().length - 2 && area(p.getPolyLon(i), p.getPolyLat(i), p.getPolyLon(i + 1), p.getPolyLat(i + 1), p.getPolyLon(getIndex(p.getPolyLats().length, j)), p.getPolyLat(getIndex(p.getPolyLats().length, j))) == 0) {
          if (pointInLine(p.getPolyLon(i), p.getPolyLat(i), p.getPolyLon(getIndex(p.getPolyLats().length, j)), p.getPolyLat(getIndex(p.getPolyLats().length, j)), aLon, aLat) &&
              pointInLine(p.getPolyLon(i), p.getPolyLat(i), p.getPolyLon(getIndex(p.getPolyLats().length, j)), p.getPolyLat(getIndex(p.getPolyLats().length, j)), bLon, bLat)) {
            return true;
          }
          j++;
        }
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

  private int getIndex(int size, int index) {
    if (index < size) {
      return index;
    }
    return index - size;
  }

  /** Compute signed area of triangle */
  private double area(final double aX, final double aY, final double bX, final double bY,
                             final double cX, final double cY) {
    return (bY - aY) * (cX - bX) - (bX - aX) * (cY - bY);
  }

  private  boolean pointInLine(final double aX, final double aY, final double bX, final double bY, double lon, double lat) {
    double dxc = lon - aX;
    double dyc = lat - aY;

    double dxl = bX - aX;
    double dyl = bY - aY;

    if (dxc * dyl - dyc * dxl == 0) {
      if (Math.abs(dxl) >= Math.abs(dyl))
        return dxl > 0 ?
            aX <= lon && lon <= bX :
            bX <= lon && lon <= aX;
      else
        return dyl > 0 ?
            aY <= lat && lat <= bY :
            bY <= lat && lat <= aY;
    }
    return false;
  }
}
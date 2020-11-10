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

import org.apache.lucene.util.SloppyMath;

/** Draws shapes on the earth surface and renders using the very cool http://www.webglearth.org.
 *
 * Just instantiate this class, add the things you want plotted, and call {@link #finish} to get the
 * resulting HTML that you should save and load with a browser. */

public class EarthDebugger {
  final StringBuilder b = new StringBuilder();
  private int nextShape;
  private boolean finished;

  public EarthDebugger() {
    b.append("<!DOCTYPE HTML>\n");
    b.append("<html>\n");
    b.append("  <head>\n");
    b.append("    <script src=\"http://www.webglearth.com/v2/api.js\"></script>\n");
    b.append("    <script>\n");
    b.append("      function initialize() {\n");
    b.append("        var earth = new WE.map('earth_div');\n");
  }

  public EarthDebugger(double centerLat, double centerLon, double altitudeMeters) {
    b.append("<!DOCTYPE HTML>\n");
    b.append("<html>\n");
    b.append("  <head>\n");
    b.append("    <script src=\"http://www.webglearth.com/v2/api.js\"></script>\n");
    b.append("    <script>\n");
    b.append("      function initialize() {\n");
    b.append("        var earth = new WE.map('earth_div', {center: [").append(centerLat).append(", ").append(centerLon).append("], altitude: ").append(altitudeMeters).append("});\n");
  }

  public void addPolygon(Polygon poly) {
    addPolygon(poly, "#00ff00");
  }

  public void addPolygon(Polygon poly, String color) {
    String name = "poly" + nextShape;
    nextShape++;

    b.append("        var ").append(name).append(" = WE.polygon([\n");
    double[] polyLats = poly.getPolyLats();
    double[] polyLons = poly.getPolyLons();
    for(int i=0;i<polyLats.length;i++) {
      b.append("          [").append(polyLats[i]).append(", ").append(polyLons[i]).append("],\n");
    }
    b.append("        ], {color: '").append(color).append("', fillColor: \"#000000\", fillOpacity: 0.0001});\n");
    b.append("        ").append(name).append(".addTo(earth);\n");

    for (Polygon hole : poly.getHoles()) {
      addPolygon(hole, "#ffffff");
    }
  }

  private static double MAX_KM_PER_STEP = 100.0;

  // Web GL earth connects dots by tunneling under the earth, so we approximate a great circle by sampling it, to minimize how deep in the
  // earth each segment tunnels:
  private int getStepCount(double minLat, double maxLat, double minLon, double maxLon) {
    double distanceMeters = SloppyMath.haversinMeters(minLat, minLon, maxLat, maxLon);
    return Math.max(1, (int) Math.round((distanceMeters / 1000.0) / MAX_KM_PER_STEP));
  }

  // first point is inclusive, last point is exclusive!
  private void drawSegment(double minLat, double maxLat, double minLon, double maxLon) {
    int steps = getStepCount(minLat, maxLat, minLon, maxLon);
    for(int i=0;i<steps;i++) {
      b.append("          [").append(minLat + (maxLat - minLat) * i / steps).append(", ").append(minLon + (maxLon - minLon) * i / steps).append("],\n");
    }
  }

  public void addRect(double minLat, double maxLat, double minLon, double maxLon) {
    addRect(minLat, maxLat, minLon, maxLon, "#ff0000");
  }

  public void addRect(double minLat, double maxLat, double minLon, double maxLon, String color) {
    String name = "rect" + nextShape;
    nextShape++;

    b.append("        // lat: ").append(minLat).append(" TO ").append(maxLat).append("; lon: ").append(minLon).append(" TO ").append(maxLon).append("\n");
    b.append("        var ").append(name).append(" = WE.polygon([\n");

    b.append("          // min -> max lat, min lon\n");
    drawSegment(minLat, maxLat, minLon, minLon);
    
    b.append("          // max lat, min -> max lon\n");
    drawSegment(maxLat, maxLat, minLon, maxLon);

    b.append("          // max -> min lat, max lon\n");
    drawSegment(maxLat, minLat, maxLon, maxLon);

    b.append("          // min lat, max -> min lon\n");
    drawSegment(minLat, minLat, maxLon, minLon);

    b.append("          // min lat, min lon\n");
    b.append("          [").append(minLat).append(", ").append(minLon).append("]\n");
    b.append("        ], {color: \"").append(color).append("\", fillColor: \"").append(color).append("\"});\n");
    b.append("        ").append(name).append(".addTo(earth);\n");
  }

  /** Draws a line a fixed latitude, spanning the min/max longitude */
  public void addLatLine(double lat, double minLon, double maxLon) {
    String name = "latline" + nextShape;
    nextShape++;

    b.append("        var ").append(name).append(" = WE.polygon([\n");
    double lon;
    int steps = getStepCount(lat, minLon, lat, maxLon);
    for(lon = minLon;lon<=maxLon;lon += (maxLon-minLon)/steps) {
      b.append("          [").append(lat).append(", ").append(lon).append("],\n");
    }
    b.append("          [").append(lat).append(", ").append(maxLon).append("],\n");
    lon -= (maxLon-minLon)/steps;
    for(;lon>=minLon;lon -= (maxLon-minLon)/steps) {
      b.append("          [").append(lat).append(", ").append(lon).append("],\n");
    }
    b.append("        ], {color: \"#ff0000\", fillColor: \"#ffffff\", opacity: 1, fillOpacity: 0.0001});\n");
    b.append("        ").append(name).append(".addTo(earth);\n");
  }

  /** Draws a line a fixed longitude, spanning the min/max latitude */
  public void addLonLine(double minLat, double maxLat, double lon) {
    String name = "lonline" + nextShape;
    nextShape++;

    b.append("        var ").append(name).append(" = WE.polygon([\n");
    double lat;
    int steps = getStepCount(minLat, lon, maxLat, lon);
    for(lat = minLat;lat<=maxLat;lat += (maxLat-minLat)/steps) {
      b.append("          [").append(lat).append(", ").append(lon).append("],\n");
    }
    b.append("          [").append(maxLat).append(", ").append(lon).append("],\n");
    lat -= (maxLat-minLat)/36;
    for(;lat>=minLat;lat -= (maxLat-minLat)/steps) {
      b.append("          [").append(lat).append(", ").append(lon).append("],\n");
    }
    b.append("        ], {color: \"#ff0000\", fillColor: \"#ffffff\", opacity: 1, fillOpacity: 0.0001});\n");
    b.append("        ").append(name).append(".addTo(earth);\n");
  }

  public void addPoint(double lat, double lon) {
    b.append("        WE.marker([").append(lat).append(", ").append(lon).append("]).addTo(earth);\n");
  }

  public void addCircle(double centerLat, double centerLon, double radiusMeters, boolean alsoAddBBox) {
    addPoint(centerLat, centerLon);
    String name = "circle" + nextShape;
    nextShape++;
    b.append("        var ").append(name).append(" = WE.polygon([\n");
    inverseHaversin(b, centerLat, centerLon, radiusMeters);
    b.append("        ], {color: '#00ff00', fillColor: \"#000000\", fillOpacity: 0.0001 });\n");
    b.append("        ").append(name).append(".addTo(earth);\n");

    if (alsoAddBBox) {
      Rectangle box = Rectangle.fromPointDistance(centerLat, centerLon, radiusMeters);
      addRect(box.minLat, box.maxLat, box.minLon, box.maxLon);
      addLatLine(Rectangle.axisLat(centerLat, radiusMeters), box.minLon, box.maxLon);
    }
  }

  public String finish() {
    if (finished) {
      throw new IllegalStateException("already finished");
    }
    finished = true;
    b.append("        WE.tileLayer('http://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png',{\n");
    b.append("          attribution: '© OpenStreetMap contributors'\n");
    b.append("        }).addTo(earth);\n");
    b.append("      }\n");
    b.append("    </script>\n");
    b.append("    <style>\n");
    b.append("      html, body{padding: 0; margin: 0;}\n");
    b.append("      #earth_div{top: 0; right: 0; bottom: 0; left: 0; position: absolute !important;}\n");
    b.append("    </style>\n");
    b.append("    <title>WebGL Earth API: Hello World</title>\n");
    b.append("  </head>\n");
    b.append("  <body onload=\"initialize()\">\n");
    b.append("    <div id=\"earth_div\"></div>\n");
    b.append("  </body>\n");
    b.append("</html>\n");

    return b.toString();
  }

  private static void inverseHaversin(StringBuilder b, double centerLat, double centerLon, double radiusMeters) {
    double angle = 0;
    int steps = 100;

    newAngle:
    while (angle < 360) {
      double x = Math.cos(Math.toRadians(angle));
      double y = Math.sin(Math.toRadians(angle));
      double factor = 2.0;
      double step = 1.0;
      int last = 0;
      double lastDistanceMeters = 0.0;
      //System.out.println("angle " + angle + " slope=" + slope);
      while (true) {
        double lat = wrapLat(centerLat + y * factor);
        double lon = wrapLon(centerLon + x * factor);
        double distanceMeters = SloppyMath.haversinMeters(centerLat, centerLon, lat, lon);

        if (last == 1 && distanceMeters < lastDistanceMeters) {
          // For large enough circles, some angles are not possible:
          //System.out.println("  done: give up on angle " + angle);
          angle += 360./steps;
          continue newAngle;
        }
        if (last == -1 && distanceMeters > lastDistanceMeters) {
          // For large enough circles, some angles are not possible:
          //System.out.println("  done: give up on angle " + angle);
          angle += 360./steps;
          continue newAngle;
        }
        lastDistanceMeters = distanceMeters;

        //System.out.println("  iter lat=" + lat + " lon=" + lon + " distance=" + distanceMeters + " vs " + radiusMeters);
        if (Math.abs(distanceMeters - radiusMeters) < 0.1) {
          b.append("          [").append(lat).append(", ").append(lon).append("],\n");
          break;
        }
        if (distanceMeters > radiusMeters) {
          // too big
          //System.out.println("    smaller");
          factor -= step;
          if (last == 1) {
            //System.out.println("      half-step");
            step /= 2.0;
          }
          last = -1;
        } else if (distanceMeters < radiusMeters) {
          // too small
          //System.out.println("    bigger");
          factor += step;
          if (last == -1) {
            //System.out.println("      half-step");
            step /= 2.0;
          }
          last = 1;
        }
      }
      angle += 360./steps;
    }
  }
  // craziness for plotting stuff :)

  private static double wrapLat(double lat) {
    //System.out.println("wrapLat " + lat);
    if (lat > 90) {
      //System.out.println("  " + (180 - lat));
      return 180 - lat;
    } else if (lat < -90) {
      //System.out.println("  " + (-180 - lat));
      return -180 - lat;
    } else {
      //System.out.println("  " + lat);
      return lat;
    }
  }

  private static double wrapLon(double lon) {
    //System.out.println("wrapLon " + lon);
    if (lon > 180) {
      //System.out.println("  " + (lon - 360));
      return lon - 360;
    } else if (lon < -180) {
      //System.out.println("  " + (lon + 360));
      return lon + 360;
    } else {
      //System.out.println("  " + lon);
      return lon;
    }
  }
}

/**
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

package org.apache.lucene.spatial.geometry.shape;

/**
 * Imported from mq java client.  No changes made.
 *
 * <p><font color="red"><b>NOTE:</b> This API is still in
 * flux and might change in incompatible ways in the next
 * release.</font>
 *
 * @deprecated This has been replaced with more accurate
 * math in {@link LLRect}. This class will be removed in a future release.
 */
@Deprecated
public class DistanceApproximation
{
  private double m_testLat;
  private double m_testLng;
  private double m_mpd;
  private static final double m_milesPerLngDeg[]={
     69.170976f, 69.160441f, 69.128838f, 69.076177f, 69.002475f,
     68.907753f, 68.792041f, 68.655373f, 68.497792f, 68.319345f,
     68.120088f, 67.900079f, 67.659387f, 67.398085f, 67.116253f,
     66.813976f, 66.491346f, 66.148462f, 65.785428f, 65.402355f,
     64.999359f, 64.576564f, 64.134098f, 63.672096f, 63.190698f,
     62.690052f, 62.170310f, 61.631630f, 61.074176f, 60.498118f,
     59.903632f, 59.290899f, 58.660106f, 58.011443f, 57.345111f,
     56.661310f, 55.960250f, 55.242144f, 54.507211f, 53.755675f,
     52.987764f, 52.203713f, 51.403761f, 50.588151f, 49.757131f,
     48.910956f, 48.049882f, 47.174172f, 46.284093f, 45.379915f,
     44.461915f, 43.530372f, 42.585570f, 41.627796f, 40.657342f,
     39.674504f, 38.679582f, 37.672877f, 36.654698f, 35.625354f,
     34.585159f, 33.534429f, 32.473485f, 31.402650f, 30.322249f,
     29.232613f, 28.134073f, 27.026963f, 25.911621f, 24.788387f,
     23.657602f, 22.519612f, 21.374762f, 20.223401f, 19.065881f,
     17.902554f, 16.733774f, 15.559897f, 14.381280f, 13.198283f,
     12.011266f, 10.820591f,  9.626619f,  8.429716f,  7.230245f,
      6.028572f,  4.825062f,  3.620083f,  2.414002f,  1.207185f,
      1.000000f};

  public static final double MILES_PER_LATITUDE   = 69.170976f;
  public static final double KILOMETERS_PER_MILE  = 1.609347f;


  public DistanceApproximation()
  {
  }

  public void setTestPoint(double lat, double lng)
  {
    m_testLat = lat;
    m_testLng = lng;
    m_mpd     = m_milesPerLngDeg[(int)(Math.abs(lat) + 0.5f)];
  }

  // Approximate arc distance between 2 lat,lng positions using miles per
  //    latitude and longitude degree
  public double getDistanceSq(double lat, double lng)
  {
    double latMiles = (lat - m_testLat) * MILES_PER_LATITUDE;

    // Approximate longitude miles using the miles per degree assuming the
    //    middle latitude/longitude.  This is less accurate at high (near
    //    polar) latitudes but no road network is present at the poles!
    //    If we ever have any roads crossing the international date we will
    //    have to worry about that case.
    double lngMiles = (lng - m_testLng) * m_mpd;

     // Find the squared distance by the Pythagorean theorem (without sqrt)
    return (latMiles * latMiles + lngMiles * lngMiles);
  }

  // Approximate arc distance between a segment (with lat,lng endpoints) and
  //    the test position
  public double getDistanceSq(double lat1, double lng1, double lat2, double lng2)
  {
     // Check if lat1,lng1 is closest point.  Construct a vector from point1
     //    to point2 (v1) and another from point 1 to the test point (v2).
     //    If dot product is negative then point 1 is the closest point
     double v1y = lat2 - lat1;
     double v1x = lng2 - lng1;
     double v2y = m_testLat - lat1;
     double v2x = m_testLng - lng1;
     double dot = v1x * v2x + v1y * v2y;
     if (dot <= 0.0f)
        return getDistanceSq(lat1, lng1);

     // Get the component of vector v2 along v1.  If component is greater
     //    than 1 then the endpoint is the closest point.
     double c = dot / (v1x * v1x + v1y * v1y);
     if (c >= 1.0f)
        return getDistanceSq(lat2, lng2);

     // Since we are working io lat,lng space we need to find the point
     //    along p1->p2 such that q->pt is perpendicular to p1->p2.  We
     //    then find the distance squared between Q and pt.
     return getDistanceSq((lat1 + v1y * c), (lng1 + v1x * c));
  }

  // Return the number of miles per degree of longitude
  public static double getMilesPerLngDeg(double lat)
  {
     return (Math.abs(lat) <= 90.0) ? m_milesPerLngDeg[(int)(Math.abs(lat) + 0.5f)] : 69.170976f;
  }
  
  public static double getMilesPerLatDeg() {
    return MILES_PER_LATITUDE;
  }
}


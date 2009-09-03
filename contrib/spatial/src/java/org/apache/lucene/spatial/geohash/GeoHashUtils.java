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

package org.apache.lucene.spatial.geohash;

import java.util.HashMap;
import java.util.Map;

/**
 * Based on http://en.wikipedia.org/wiki/Geohash
 *
 * <p><font color="red"><b>NOTE:</b> This API is still in
 * flux and might change in incompatible ways in the next
 * release.</font>
 */
public class GeoHashUtils {

	// geohash's char map
	// no a's i's l's o's
	// old MacDonal wouldn't be happy
	private static char[] _base32 = {'0','1','2','3','4','5','6','7','8','9',
							'b','c','d','e','f','g','h','j','k','m',
							'n','p','q','r','s','t','u','v','w','x',
							'y','z'} ;
	
	private final static Map<Character, Integer> _decodemap = new HashMap<Character, Integer>();
	static {
		int sz = _base32.length;
		for (int i = 0; i < sz; i++ ){
			_decodemap.put(_base32[i], i);
		}
	}
	
	private static int precision = 12;
	private static int[] bits = {16, 8, 4, 2, 1};
	
	public static void main(String[] args) {
		GeoHashUtils ghf = new GeoHashUtils();
		String gc1 = ghf.encode(30, -90.0);
		String gc2 = ghf.encode(51.4797, -0.0124);
		
		System.out.println(gc1);
		System.out.println(gc2);
		
		double [] gd1 = ghf.decode(gc1);
		double [] gd2 = ghf.decode(gc2);
		System.out.println(gd1[0]+ ", "+ gd1[1]);
		System.out.println(gd2[0]+ ", "+ gd2[1]);
		
	}
	
	public static String encode(double latitude, double longitude){
		double[] lat_interval = {-90.0 ,  90.0};
		double[] lon_interval = {-180.0, 180.0};
			
		StringBuilder geohash = new StringBuilder();
		boolean is_even = true;
		int bit = 0, ch = 0;
		
		while(geohash.length() < precision){
			double mid = 0.0;
			if(is_even){
				mid = (lon_interval[0] + lon_interval[1]) / 2;
				if (longitude > mid){
					ch |= bits[bit];
					lon_interval[0] = mid;
				} else {
					lon_interval[1] = mid;
				}
				
			} else {
				mid = (lat_interval[0] + lat_interval[1]) / 2;
				if(latitude > mid){
					ch |= bits[bit];
					lat_interval[0] = mid;
				} else {
					lat_interval[1] = mid;
				}
			}
			
			is_even = is_even ? false : true;
			
			if (bit  < 4){
				bit ++;
			} else {
				geohash.append(_base32[ch]);
				bit =0;
				ch = 0;
			}
		}
		
		return geohash.toString();
	}
	
	public static double[] decode(String geohash) {
		double[] ge = decode_exactly(geohash);
		double lat, lon, lat_err, lon_err;
		lat = ge[0];
		lon = ge[1];
		lat_err = ge[2];
		lon_err = ge[3];
		
		double lat_precision = Math.max(1, Math.round(- Math.log10(lat_err))) - 1;
		double lon_precision = Math.max(1, Math.round(- Math.log10(lon_err))) - 1;
		
		lat = getPrecision(lat, lat_precision);
		lon = getPrecision(lon, lon_precision);
		
		return new double[] {lat, lon};
	}
	
	public static double[] decode_exactly (String geohash){
		double[] lat_interval = {-90.0 , 90.0};
		double[] lon_interval = {-180.0, 180.0};
		
		double lat_err =  90.0;
		double lon_err = 180.0;
		boolean is_even = true;
		int sz = geohash.length();
		int bsz = bits.length;
		double latitude, longitude;
		for (int i = 0; i < sz; i++){
			
			int cd = _decodemap.get(geohash.charAt(i));
			
			for (int z = 0; z< bsz; z++){
				int mask = bits[z];
				if (is_even){
					lon_err /= 2;
					if ((cd & mask) != 0){
						lon_interval[0] = (lon_interval[0]+lon_interval[1])/2;
					} else {
						lon_interval[1] = (lon_interval[0]+lon_interval[1])/2;
					}
					
				} else {
					lat_err /=2;
				
					if ( (cd & mask) != 0){
						lat_interval[0] = (lat_interval[0]+lat_interval[1])/2;
					} else {
						lat_interval[1] = (lat_interval[0]+lat_interval[1])/2;
					}
				}
				is_even = is_even ? false : true;
			}
		
		}
		latitude  = (lat_interval[0] + lat_interval[1]) / 2;
		longitude = (lon_interval[0] + lon_interval[1]) / 2;

		return new double []{latitude, longitude, lat_err, lon_err};
	}
	
	static double getPrecision(double x, double precision) {
		double base = Math.pow(10,- precision);
		double diff = x % base;
		return x - diff;
	}
}

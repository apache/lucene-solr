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

package org.apache.solr.search.facet;


public class AggUtil {

  private AggUtil() {
  }

  /**
   * Computes and returns average for given sum and count
   */
  public static double avg(double sum, long count) {
    // todo: should we return NAN when count==0?
    return count == 0? 0.0d: sum / count;
  }

  /**
   * Computes and returns uncorrected standard deviation for given values
   */
  public static double uncorrectedStdDev(double sumSq, double sum, long count) {
    // todo: should we return NAN when count==0?
    return count == 0 ? 0 : Math.sqrt((sumSq / count) - Math.pow(sum / count, 2));
  }

  /**
   * Computes and returns corrected standard deviation for given values
   */
  public static double stdDev(double sumSq, double sum, long count) {
    // todo: should we return NAN when count==0?
    return count == 0 ? 0 : Math.sqrt(((count * sumSq) - (sum * sum)) / (count * (count - 1.0D)));
  }

  /**
   * Computes and returns uncorrected variance for given values
   */
  public static double uncorrectedVariance(double sumSq, double sum, long count) {
    // todo: should we return NAN when count==0?
    return count == 0 ? 0 : (sumSq / count) - Math.pow(sum / count, 2);
  }

  /**
   * Computes and returns corrected variance for given values
   */
  public static double variance(double sumSq, double sum, long count) {
    // todo: should we return NAN when count==0?
    return count == 0 ? 0 : ((count * sumSq) - (sum * sum)) / (count * (count - 1.0D));
  }
}

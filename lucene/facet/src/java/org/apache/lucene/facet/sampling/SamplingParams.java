package org.apache.lucene.facet.sampling;

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

/**
 * Parameters for sampling, dictating whether sampling is to take place and how. 
 * 
 * @lucene.experimental
 */
public class SamplingParams {

  /**
   * Default factor by which more results are requested over the sample set.
   * @see SamplingParams#getOversampleFactor()
   */
  public static final double DEFAULT_OVERSAMPLE_FACTOR = 2d;
  
  /**
   * Default ratio between size of sample to original size of document set.
   * @see Sampler#getSampleSet(org.apache.lucene.facet.search.ScoredDocIDs)
   */
  public static final double DEFAULT_SAMPLE_RATIO = 0.01;
  
  /**
   * Default maximum size of sample.
   * @see Sampler#getSampleSet(org.apache.lucene.facet.search.ScoredDocIDs)
   */
  public static final int DEFAULT_MAX_SAMPLE_SIZE = 10000;
  
  /**
   * Default minimum size of sample.
   * @see Sampler#getSampleSet(org.apache.lucene.facet.search.ScoredDocIDs)
   */
  public static final int DEFAULT_MIN_SAMPLE_SIZE = 100;
  
  /**
   * Default sampling threshold, if number of results is less than this number - no sampling will take place
   * @see SamplingParams#getSampleRatio()
   */
  public static final int DEFAULT_SAMPLING_THRESHOLD = 75000;

  private int maxSampleSize = DEFAULT_MAX_SAMPLE_SIZE;
  private int minSampleSize = DEFAULT_MIN_SAMPLE_SIZE;
  private double sampleRatio = DEFAULT_SAMPLE_RATIO;
  private int samplingThreshold = DEFAULT_SAMPLING_THRESHOLD;
  private double oversampleFactor = DEFAULT_OVERSAMPLE_FACTOR;
  
  /**
   * Return the maxSampleSize.
   * In no case should the resulting sample size exceed this value.  
   * @see Sampler#getSampleSet(org.apache.lucene.facet.search.ScoredDocIDs)
   */
  public final int getMaxSampleSize() {
    return maxSampleSize;
  }

  /**
   * Return the minSampleSize.
   * In no case should the resulting sample size be smaller than this value.  
   * @see Sampler#getSampleSet(org.apache.lucene.facet.search.ScoredDocIDs)
   */
  public final int getMinSampleSize() {
    return minSampleSize;
  }

  /**
   * @return the sampleRatio
   * @see Sampler#getSampleSet(org.apache.lucene.facet.search.ScoredDocIDs)
   */
  public final double getSampleRatio() {
    return sampleRatio;
  }
  
  /**
   * Return the samplingThreshold.
   * Sampling would be performed only for document sets larger than this.  
   */
  public final int getSamplingThreshold() {
    return samplingThreshold;
  }

  /**
   * @param maxSampleSize
   *          the maxSampleSize to set
   * @see #getMaxSampleSize()
   */
  public void setMaxSampleSize(int maxSampleSize) {
    this.maxSampleSize = maxSampleSize;
  }

  /**
   * @param minSampleSize
   *          the minSampleSize to set
   * @see #getMinSampleSize()
   */
  public void setMinSampleSize(int minSampleSize) {
    this.minSampleSize = minSampleSize;
  }

  /**
   * @param sampleRatio
   *          the sampleRatio to set
   * @see #getSampleRatio()
   */
  public void setSampleRatio(double sampleRatio) {
    this.sampleRatio = sampleRatio;
  }

  /**
   * Set a sampling-threshold
   * @see #getSamplingThreshold()
   */
  public void setSamplingThreshold(int samplingThreshold) {
    this.samplingThreshold = samplingThreshold;
  }

  /**
   * Check validity of sampling settings, making sure that
   * <ul>
   * <li> <code>minSampleSize <= maxSampleSize <= samplingThreshold </code></li>
   * <li> <code>0 < samplingRatio <= 1 </code></li>
   * </ul> 
   * 
   * @return true if valid, false otherwise
   */
  public boolean validate() {
    return 
      samplingThreshold >= maxSampleSize && 
      maxSampleSize >= minSampleSize && 
      sampleRatio > 0 &&
      sampleRatio < 1;
  }

  /**
   * Return the oversampleFactor. When sampling, we would collect that much more
   * results, so that later, when selecting top out of these, chances are higher
   * to get actual best results. Note that having this value larger than 1 only
   * makes sense when using a SampleFixer which finds accurate results, such as
   * <code>TakmiSampleFixer</code>. When this value is smaller than 1, it is
   * ignored and no oversampling takes place.
   */
  public final double getOversampleFactor() {
    return oversampleFactor;
  }

  /**
   * @param oversampleFactor the oversampleFactor to set
   * @see #getOversampleFactor()
   */
  public void setOversampleFactor(double oversampleFactor) {
    this.oversampleFactor = oversampleFactor;
  }

}
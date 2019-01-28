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
package org.apache.solr.search.similarities;

import org.apache.lucene.misc.SweetSpotSimilarity;
import org.apache.lucene.search.similarities.ClassicSimilarity; // jdoc
import org.apache.lucene.search.similarities.Similarity;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.SolrException;
import static org.apache.solr.common.SolrException.ErrorCode.*;
/**
 * <p>Factory for {@link SweetSpotSimilarity}.</p>
 * <p>
 * <code>SweetSpotSimilarity</code> is an extension of 
 * {@link ClassicSimilarity} that provides additional tuning options for 
 * specifying the "sweetspot" of optimal <code>tf</code> and 
 * <code>lengthNorm</code> values in the source data.
 * </p>
 * <p>
 * In addition to the <code>discountOverlaps</code> init param supported by 
 * {@link ClassicSimilarityFactory} The following sets of init params are 
 * supported by this factory:
 * </p>
 * <ul>
 *   <li>Length Norm Settings: <ul>
 *     <li><code>lengthNormMin</code> (int)</li>
 *     <li><code>lengthNormMax</code> (int)</li>
 *     <li><code>lengthNormSteepness</code> (float)</li>
 *   </ul></li>
 *   <li>Baseline TF Settings: <ul>
 *     <li><code>baselineTfBase</code> (float)</li>
 *     <li><code>baselineTfMin</code> (float)</li>
 *   </ul></li>
 *   <li>Hyperbolic TF Settings: <ul>
 *     <li><code>hyperbolicTfMin</code> (float)</li>
 *     <li><code>hyperbolicTfMax</code> (float)</li>
 *     <li><code>hyperbolicTfBase</code> (double)</li>
 *     <li><code>hyperbolicTfOffset</code> (float)</li>
 *   </ul></li>
 * </ul>
 * <p>
 * Note:
 * </p>
 * <ul>
 *  <li>If any individual settings from one of the above mentioned sets 
 *      are specified, then all settings from that set must be specified.
 *  </li>
 *  <li>If Baseline TF settings are specified, then Hyperbolic TF settings
 *      are not permitted, and vice versa. (The settings specified will 
 *      determine whether {@link SweetSpotSimilarity#baselineTf} or
 *      {@link SweetSpotSimilarity#hyperbolicTf} will be used.
 *  </li>
 * </ul>
 * <p>
 * Example usage...
 * </p>
 * <pre class="prettyprint">
 * &lt;!-- using baseline TF --&gt;
 * &lt;fieldType name="text_baseline" class="solr.TextField"
 *            indexed="true" stored="false"&gt;
 *   &lt;analyzer class="org.apache.lucene.analysis.standard.StandardAnalyzer"/&gt;
 *   &lt;similarity class="solr.SweetSpotSimilarityFactory"&gt;
 *     &lt;!-- TF --&gt;
 *     &lt;float name="baselineTfMin"&gt;6.0&lt;/float&gt;
 *     &lt;float name="baselineTfBase"&gt;1.5&lt;/float&gt;
 *     &lt;!-- plateau norm --&gt;
 *     &lt;int name="lengthNormMin"&gt;3&lt;/int&gt;
 *     &lt;int name="lengthNormMax"&gt;5&lt;/int&gt;
 *     &lt;float name="lengthNormSteepness"&gt;0.5&lt;/float&gt;
 *   &lt;/similarity&gt;
 * &lt;/fieldType&gt;
 * 
 * &lt;!-- using hyperbolic TF --&gt;
 * &lt;fieldType name="text_hyperbolic" class="solr.TextField"
 *            indexed="true" stored="false" &gt;
 *   &lt;analyzer class="org.apache.lucene.analysis.standard.StandardAnalyzer"/&gt;
 *   &lt;similarity class="solr.SweetSpotSimilarityFactory"&gt;
 *     &lt;float name="hyperbolicTfMin"&gt;3.3&lt;/float&gt;
 *     &lt;float name="hyperbolicTfMax"&gt;7.7&lt;/float&gt;
 *     &lt;double name="hyperbolicTfBase"&gt;2.718281828459045&lt;/double&gt; &lt;!-- e --&gt;
 *     &lt;float name="hyperbolicTfOffset"&gt;5.0&lt;/float&gt;
 *     &lt;!-- plateau norm, shallower slope --&gt;
 *     &lt;int name="lengthNormMin"&gt;1&lt;/int&gt;
 *     &lt;int name="lengthNormMax"&gt;5&lt;/int&gt;
 *     &lt;float name="lengthNormSteepness"&gt;0.2&lt;/float&gt;
 *   &lt;/similarity&gt;
 * &lt;/fieldType&gt;
 * </pre>
 * @see SweetSpotSimilarity The javadocs for the individual methods in 
 *      <code>SweetSpotSimilarity</code> for SVG diagrams showing how the 
 *      each function behaves with various settings/inputs.
 */
public class SweetSpotSimilarityFactory extends ClassicSimilarityFactory {
  private SweetSpotSimilarity sim = null;

  @Override
  public void init(SolrParams params) {
    super.init(params);

    Integer ln_min = params.getInt("lengthNormMin");
    Integer ln_max = params.getInt("lengthNormMax");
    Float ln_steep = params.getFloat("lengthNormSteepness");
    if (! allOrNoneNull(ln_min, ln_max, ln_steep)) {
      throw new SolrException(SERVER_ERROR, "Overriding default lengthNorm settings requires all to be specified: lengthNormMin, lengthNormMax, lengthNormSteepness");
    }

    Float hyper_min = params.getFloat("hyperbolicTfMin");
    Float hyper_max = params.getFloat("hyperbolicTfMax");
    Double hyper_base = params.getDouble("hyperbolicTfBase");
    Float hyper_offset = params.getFloat("hyperbolicTfOffset");
    if (! allOrNoneNull(hyper_min, hyper_max, hyper_base, hyper_offset)) {
      throw new SolrException(SERVER_ERROR, "Overriding default hyperbolicTf settings requires all to be specified: hyperbolicTfMin, hyperbolicTfMax, hyperbolicTfBase, hyperbolicTfOffset");
    }

    Float baseline_base = params.getFloat("baselineTfBase");
    Float baseline_min = params.getFloat("baselineTfMin");
    if (! allOrNoneNull(baseline_min, baseline_base)) {
      throw new SolrException(SERVER_ERROR, "Overriding default baselineTf settings requires all to be specified: baselineTfBase, baselineTfMin");
    }

    // sanity check that they aren't trying to use two diff tf impls
    if ((null != hyper_min) && (null != baseline_min)) {
      throw new SolrException(SERVER_ERROR, "Can not mix hyperbolicTf settings with baselineTf settings");
    }

    // pick Similarity impl based on whether hyper tf settings are set
    sim = (null != hyper_min) ? new HyperbolicSweetSpotSimilarity() 
      : new SweetSpotSimilarity();
    
    if (null != ln_min) {
      // overlaps already handled by super factory
      sim.setLengthNormFactors(ln_min, ln_max, ln_steep, this.discountOverlaps);
    }

    if (null != hyper_min) {
      sim.setHyperbolicTfFactors(hyper_min, hyper_max, hyper_base, hyper_offset);
    }

    if (null != baseline_min) {
      sim.setBaselineTfFactors(baseline_base, baseline_min);
    }
  }

  @Override
  public Similarity getSimilarity() {
    assert sim != null : "SweetSpotSimilarityFactory was not initialized";
    return sim;
  }
  
  /** 
   * Returns true if either: all of the specified arguments are null;
   * or none of the specified arguments are null
   */
  private static boolean allOrNoneNull(Object... args) {
    int nulls = 0;
    int objs = 0;
    for (Object o : args) {
      objs++;
      if (null == o) nulls++;
    }
    return (0 == nulls || nulls == objs);
  }

  private static final class HyperbolicSweetSpotSimilarity 
    extends SweetSpotSimilarity {
    @Override
    public float tf(float freq) {
      return hyperbolicTf(freq);
    }
  }
}

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
package org.apache.solr.util;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;
import org.apache.solr.SolrTestCaseJ4.SuppressSSL;


/**
 * Marker annotation indicating when SSL Randomization should be used for a test class, and if so what 
 * the typical odds of using SSL should for that test class.
 * @see SSLRandomizer#getSSLRandomizerForClass
 * @see SuppressSSL
 */
@Documented
@Inherited
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface RandomizeSSL {

  // we don't choose ssl that often by default because of SOLR-5776
  public static final double DEFAULT_ODDS = 0.2D;
  /** Comment to inlcude when logging details of SSL randomization */
  public String reason() default "";
  /** 
   * Odds (as ratio relative to 1) that SSL should be selected in a typical run.
   * Must either be betwen 0.0 and 1.0 (inclusively) or NaN in which case a sensible should be used.
   * Actual Odds used for randomization may be higher depending on runner options such as 
   * <code>tests.multiplier</code> or <code>tests.nightly</code>
   *
   * @see #DEFAULT_ODDS
   * @see LuceneTestCase#TEST_NIGHTLY
   * @see LuceneTestCase#RANDOM_MULTIPLIER
   */
  public double ssl() default Double.NaN;
  /** 
   * Odds (as ratio relative to 1) that SSL should be selected in a typical run.
   * Must either be betwen 0.0 and 1.0 (inclusively) or NaN in which case the effective value of
   * {@link #ssl} should be used.
   * Actual Odds used for randomization may be higher depending on runner options such as 
   * <code>tests.multiplier</code> or <code>tests.nightly</code>
   * <p>
   * NOTE: clientAuth is useless unless ssl is also in used, but we randomize it independently
   * just in case it might find bugs in our test/ssl client code (ie: attempting to use
   * SSL w/client cert to non-ssl servers)
   * </p>
   * @see #DEFAULT_ODDS
   * @see LuceneTestCase#TEST_NIGHTLY
   * @see LuceneTestCase#RANDOM_MULTIPLIER
   */
  public double clientAuth() default Double.NaN;
  /**
   * A shorthand option for controlling both {@link #ssl} and {@link #clientAuth} with a single numeric 
   * value, For example: <code>@RandomizeSSL(0.5)</code>.
   *
   * Ignored if {@link #ssl} is set explicitly.
   */
  public double value() default Double.NaN;

  /**
   * A simple data structure for encapsulating the effective values to be used when randomizing
   * SSL in a test, based on the configured values in the {@link RandomizeSSL} annotation.
   */
  public static final class SSLRandomizer {
    public final double ssl;
    public final double clientAuth;
    public final String debug;
    /** @lucene.internal */
    public SSLRandomizer(double ssl, double clientAuth, String debug) {
      this.ssl = ssl;
      this.clientAuth = clientAuth;
      this.debug = debug;
    }

    /** 
     * Randomly produces an SSLTestConfig taking into account various factors 
     *
     * @see LuceneTestCase#TEST_NIGHTLY
     * @see LuceneTestCase#RANDOM_MULTIPLIER
     * @see LuceneTestCase#random()
     */
    public SSLTestConfig createSSLTestConfig() {
      // even if we know SSL is disabled, always consume the same amount of randomness
      // that way all other test behavior should be consistent even if a user adds/removes @SuppressSSL
      
      final boolean useSSL = TestUtil.nextInt(LuceneTestCase.random(), 0, 999) <
        (int)(1000 * getEffectiveOdds(ssl, LuceneTestCase.TEST_NIGHTLY, LuceneTestCase.RANDOM_MULTIPLIER));
      final boolean useClientAuth = TestUtil.nextInt(LuceneTestCase.random(), 0, 999) <
        (int)(1000 * getEffectiveOdds(clientAuth, LuceneTestCase.TEST_NIGHTLY, LuceneTestCase.RANDOM_MULTIPLIER));

      return new SSLTestConfig(useSSL, useClientAuth);
    }
    
    /** @lucene.internal Public only for testing */
    public static double getEffectiveOdds(final double declaredOdds,
                                          final boolean nightly,
                                          final int multiplier) {
      assert declaredOdds <= 1.0D;
      assert 0.0D <= declaredOdds;

      if (declaredOdds == 0.0D || declaredOdds == 1.0D ) {
        return declaredOdds;
      }
      
      assert 0 < multiplier;
      
      // negate the odds so we can then divide it by our multipling factors
      // to increase the final odds
      return 1.0D - ((1.0D - declaredOdds)
                     / ((nightly ? 1.1D : 1.0D) * (1.0D + Math.log(multiplier))));
    }
    
    /**
     * Returns an SSLRandomizer suitable for the specified (test) class
     */
    public static final SSLRandomizer getSSLRandomizerForClass(@SuppressWarnings({"rawtypes"})Class clazz) {

      @SuppressWarnings({"unchecked"})
      final SuppressSSL suppression = (SuppressSSL) clazz.getAnnotation(SuppressSSL.class);
      if (null != suppression) {
        // Even if this class has a RandomizeSSL annotation, any usage of SuppressSSL -- even in a
        // super class -- overrules that.
        //
        // (If it didn't work this way, it would be a pain in the ass to quickly disable SSL for a
        // broad hierarchy of tests)
        return new SSLRandomizer(0.0D, 0.0D, suppression.toString());
      }

      @SuppressWarnings({"unchecked"})
      final RandomizeSSL annotation = (RandomizeSSL) clazz.getAnnotation(RandomizeSSL.class);
      
      if (null == annotation) {
        return new SSLRandomizer(0.0D, 0.0D, RandomizeSSL.class.getName() + " annotation not specified");
      }

      final double def = Double.isNaN(annotation.value()) ? DEFAULT_ODDS : annotation.value();
      if (def < 0.0D || 1.0D < def) {
        throw new IllegalArgumentException
          (clazz.getName() + ": default value is not a ratio between 0 and 1: " + annotation.toString());
      }
      final double ssl = Double.isNaN(annotation.ssl()) ? def : annotation.ssl();
      if (ssl < 0.0D || 1.0D < ssl) {
        throw new IllegalArgumentException
          (clazz.getName() + ": ssl value is not a ratio between 0 and 1: " + annotation.toString());
      }
      final double clientAuth = Double.isNaN(annotation.clientAuth()) ? ssl : annotation.clientAuth();
      if (clientAuth < 0.0D || 1 < clientAuth) {
        throw new IllegalArgumentException
          (clazz.getName() + ": clientAuth value is not a ratio between 0 and 1: " + annotation.toString());
      }
      return new SSLRandomizer(ssl, clientAuth, annotation.toString());
    }
  }
}

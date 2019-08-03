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
package org.apache.solr.index;

import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.TieredMergePolicy;
import org.apache.lucene.index.UpgradeIndexMergePolicy;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.schema.IndexSchema;

/** Unit tests for {@link WrapperMergePolicyFactory}. */
public class WrapperMergePolicyFactoryTest extends SolrTestCaseJ4 {

  private final SolrResourceLoader resourceLoader = new SolrResourceLoader();

  public void testReturnsDefaultMergePolicyIfNoneSpecified() {
    final MergePolicyFactoryArgs args = new MergePolicyFactoryArgs();
    MergePolicyFactory mpf = new DefaultingWrapperMergePolicyFactory(resourceLoader, args, null);
    assertSame(mpf.getMergePolicy(), NoMergePolicy.INSTANCE);
  }

  public void testFailsIfNoClassSpecifiedForWrappedPolicy() {
    final MergePolicyFactoryArgs args = new MergePolicyFactoryArgs();
    args.add(WrapperMergePolicyFactory.WRAPPED_PREFIX, "foo");
    expectThrows(IllegalArgumentException.class,
        () -> new DefaultingWrapperMergePolicyFactory(resourceLoader, args, null).getMergePolicy());
  }

  public void testProperlyInitializesWrappedMergePolicy() {
    final TieredMergePolicy defaultTMP = new TieredMergePolicy();
    final int testMaxMergeAtOnce = defaultTMP.getMaxMergeAtOnce() * 2;
    final double testMaxMergedSegmentMB = defaultTMP.getMaxMergedSegmentMB() * 10;

    final MergePolicyFactoryArgs args = new MergePolicyFactoryArgs();
    args.add(WrapperMergePolicyFactory.WRAPPED_PREFIX, "test");
    args.add("test.class", TieredMergePolicyFactory.class.getName());
    args.add("test.maxMergeAtOnce", testMaxMergeAtOnce);
    args.add("test.maxMergedSegmentMB", testMaxMergedSegmentMB);
    MergePolicyFactory mpf = new DefaultingWrapperMergePolicyFactory(resourceLoader, args, null) {
      @Override
      protected MergePolicy getDefaultWrappedMergePolicy() {
        throw new IllegalStateException("Should not have reached here!");
      }
    };
    final MergePolicy mp = mpf.getMergePolicy();
    assertSame(mp.getClass(), TieredMergePolicy.class);
    final TieredMergePolicy tmp = (TieredMergePolicy)mp;
    assertEquals("maxMergeAtOnce", testMaxMergeAtOnce, tmp.getMaxMergeAtOnce());
    assertEquals("maxMergedSegmentMB", testMaxMergedSegmentMB, tmp.getMaxMergedSegmentMB(), 0.0d);
  }

  public void testUpgradeIndexMergePolicyFactory() {
    final int N = 10;
    final Double wrappingNoCFSRatio = random().nextBoolean() ? null : random().nextInt(N+1)/((double)N); // must be: 0.0 <= value <= 1.0
    final Double wrappedNoCFSRatio  = random().nextBoolean() ? null : random().nextInt(N+1)/((double)N); // must be: 0.0 <= value <= 1.0
    implTestUpgradeIndexMergePolicyFactory(wrappingNoCFSRatio, wrappedNoCFSRatio);
  }

  private void implTestUpgradeIndexMergePolicyFactory(Double wrappingNoCFSRatio, Double wrappedNoCFSRatio) {
    final MergePolicyFactoryArgs args = new MergePolicyFactoryArgs();
    if (wrappingNoCFSRatio != null) {
      args.add("noCFSRatio", wrappingNoCFSRatio); // noCFSRatio for the wrapping merge policy
    }
    args.add(WrapperMergePolicyFactory.WRAPPED_PREFIX, "wrapped");
    args.add("wrapped.class", TieredMergePolicyFactory.class.getName());
    if (wrappedNoCFSRatio != null) {
      args.add("wrapped.noCFSRatio", wrappedNoCFSRatio); // noCFSRatio for the wrapped merge policy
    }

    MergePolicyFactory mpf;
    try {
      mpf = new UpgradeIndexMergePolicyFactory(resourceLoader, args, null);
      assertFalse("Should only reach here if wrapping and wrapped args don't overlap!",
          (wrappingNoCFSRatio != null && wrappedNoCFSRatio != null));

      for (int ii=1; ii<=2; ++ii) { // it should be okay to call getMergePolicy() more than once
        final MergePolicy mp = mpf.getMergePolicy();
        if (wrappingNoCFSRatio != null) {
          assertEquals("#"+ii+" wrappingNoCFSRatio", wrappingNoCFSRatio.doubleValue(), mp.getNoCFSRatio(), 0.0d);
        }
        if (wrappedNoCFSRatio != null) {
          assertEquals("#"+ii+" wrappedNoCFSRatio", wrappedNoCFSRatio.doubleValue(), mp.getNoCFSRatio(), 0.0d);
        }
        assertSame(mp.getClass(), UpgradeIndexMergePolicy.class);
      }

    } catch (IllegalArgumentException iae) {
      assertEquals("Wrapping and wrapped merge policy args overlap! [noCFSRatio]", iae.getMessage());
      assertTrue("Should only reach here if wrapping and wrapped args do overlap!",
          (wrappingNoCFSRatio != null && wrappedNoCFSRatio != null));
    }
  }

  private static class DefaultingWrapperMergePolicyFactory extends WrapperMergePolicyFactory {

    DefaultingWrapperMergePolicyFactory(SolrResourceLoader resourceLoader, MergePolicyFactoryArgs wrapperArgs, IndexSchema schema) {
      super(resourceLoader, wrapperArgs, schema);
      if (!args.keys().isEmpty()) {
        throw new IllegalArgumentException("All arguments should have been claimed by the wrapped policy but some ("+args+") remain.");
      }
    }

    @Override
    protected MergePolicy getDefaultWrappedMergePolicy() {
      return NoMergePolicy.INSTANCE;
    }

    @Override
    protected MergePolicy getMergePolicyInstance(MergePolicy wrappedMP) {
      return getWrappedMergePolicy();
    }

  }

}

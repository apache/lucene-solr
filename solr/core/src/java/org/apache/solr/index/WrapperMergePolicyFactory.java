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

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.lucene.index.MergePolicy;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.schema.IndexSchema;

/**
 * A {@link MergePolicyFactory} for wrapping additional {@link MergePolicyFactory factories}.
 */
public abstract class WrapperMergePolicyFactory extends MergePolicyFactory {

  private static final String CLASS = "class";

  protected static final String[] NO_SUB_PACKAGES = new String[0];

  static final String WRAPPED_PREFIX = "wrapped.prefix"; // not private so that test(s) can use it

  private final MergePolicyFactoryArgs wrappedMergePolicyArgs;
  private final String wrappedMergePolicyClassName;

  protected WrapperMergePolicyFactory(SolrResourceLoader resourceLoader, MergePolicyFactoryArgs args, IndexSchema schema) {
    super(resourceLoader, args, schema);
    wrappedMergePolicyArgs = filterWrappedMergePolicyFactoryArgs();
    if (wrappedMergePolicyArgs == null) {
      wrappedMergePolicyClassName = null;
    } else {
      wrappedMergePolicyClassName = (String) wrappedMergePolicyArgs.remove(CLASS);
      if (wrappedMergePolicyClassName == null) {
        throw new IllegalArgumentException("Class name not defined for wrapped MergePolicyFactory!");
      }
    }
    if (wrappedMergePolicyArgs != null) {
      final Set<String> overshadowedWrappedMergePolicyArgs = new HashSet<>(wrappedMergePolicyArgs.keys());
      overshadowedWrappedMergePolicyArgs.retainAll(args.keys());
      if (!overshadowedWrappedMergePolicyArgs.isEmpty()) {
        throw new IllegalArgumentException("Wrapping and wrapped merge policy args overlap! "+overshadowedWrappedMergePolicyArgs);
      }
    }
  }

  /**
   * Returns the default wrapped {@link MergePolicy}. This is called if the factory settings do not explicitly specify
   * the wrapped policy.
   */
  protected MergePolicy getDefaultWrappedMergePolicy() {
    final MergePolicyFactory mpf = new DefaultMergePolicyFactory();
    return mpf.getMergePolicy();
  }

  /** Returns an instance of the wrapped {@link MergePolicy} after it has been configured with all set parameters. */
  @SuppressWarnings({"rawtypes"})
  protected final MergePolicy getWrappedMergePolicy() {
    if (wrappedMergePolicyArgs == null) {
      return getDefaultWrappedMergePolicy();
    }

    final MergePolicyFactory mpf = resourceLoader.newInstance(
        wrappedMergePolicyClassName,
        MergePolicyFactory.class,
        NO_SUB_PACKAGES,
        new Class[] {SolrResourceLoader.class, MergePolicyFactoryArgs.class, IndexSchema.class},
        new Object[] {resourceLoader, wrappedMergePolicyArgs, schema});
    return mpf.getMergePolicy();
  }

  /** Returns an instance of the wrapping {@link MergePolicy} without configuring its set parameters. */
  protected abstract MergePolicy getMergePolicyInstance(MergePolicy wrappedMP);


  /** Returns a wrapping {@link MergePolicy} with its set parameters configured. */
  @Override
  public final MergePolicy getMergePolicy() {
    final MergePolicy wrappedMP = getWrappedMergePolicy();
    final MergePolicy mp = getMergePolicyInstance(wrappedMP);
    args.invokeSetters(mp);
    return mp;
  }

  /**
   * Returns a {@link MergePolicyFactoryArgs} for the wrapped {@link MergePolicyFactory}. This method also removes all
   * args from this instance's args.
   */
  private MergePolicyFactoryArgs filterWrappedMergePolicyFactoryArgs() {
    final String wrappedPolicyPrefix = (String) args.remove(WRAPPED_PREFIX);
    if (wrappedPolicyPrefix == null) {
      return null;
    }

    final String baseArgsPrefix = wrappedPolicyPrefix + '.';
    final int baseArgsPrefixLength = baseArgsPrefix.length();
    final MergePolicyFactoryArgs wrappedArgs = new MergePolicyFactoryArgs();
    for (final Iterator<String> iter = args.keys().iterator(); iter.hasNext();) {
      final String key = iter.next();
      if (key.startsWith(baseArgsPrefix)) {
        wrappedArgs.add(key.substring(baseArgsPrefixLength), args.get(key));
        iter.remove();
      }
    }
    return wrappedArgs;
  }

}

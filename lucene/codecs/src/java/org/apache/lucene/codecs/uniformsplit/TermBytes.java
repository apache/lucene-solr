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

package org.apache.lucene.codecs.uniformsplit;

import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.StringHelper;

/**
 * Term of a block line.
 *
 * <p>Contains the term bytes and the minimal distinguishing prefix (MDP) length of this term.
 *
 * <p>The MDP is the minimal prefix that distinguishes a term from its immediate previous term
 * (terms are alphabetically sorted).
 *
 * <p>The incremental encoding suffix is the suffix starting at the last byte of the MDP
 * (inclusive).
 *
 * <p>Example: For the block
 *
 * <pre>
 * client
 * color
 * company
 * companies
 * </pre>
 *
 * "color" - MDP is "co" - incremental encoding suffix is "olor". <br>
 * "company" - MDP is "com" - incremental encoding suffix is "mpany". <br>
 * "companies" - MDP is "compani" - incremental encoding suffix is "ies".
 *
 * @lucene.experimental
 */
public class TermBytes implements Accountable {

  private static final long BASE_RAM_USAGE =
      RamUsageEstimator.shallowSizeOfInstance(TermBytes.class);

  protected int mdpLength;
  protected BytesRef term;

  public TermBytes(int mdpLength, BytesRef term) {
    reset(mdpLength, term);
  }

  public TermBytes reset(int mdpLength, BytesRef term) {
    assert term.length > 0 && mdpLength > 0 || term.length == 0 && mdpLength == 0
        : "Inconsistent mdpLength=" + mdpLength + ", term.length=" + term.length;
    assert term.length == 0 || mdpLength <= term.length
        : "Too large mdpLength=" + mdpLength + ", term.length=" + term.length;
    assert term.offset == 0;
    this.mdpLength = mdpLength;
    this.term = term;
    return this;
  }

  /**
   * @return This term MDP length.
   * @see TermBytes
   */
  public int getMdpLength() {
    return mdpLength;
  }

  /** @return This term bytes. */
  public BytesRef getTerm() {
    return term;
  }

  /**
   * @return The offset of this term incremental encoding suffix.
   * @see TermBytes
   */
  public int getSuffixOffset() {
    return Math.max(mdpLength - 1, 0);
  }

  /**
   * @return The length of this term incremental encoding suffix.
   * @see TermBytes
   */
  public int getSuffixLength() {
    return term.length - getSuffixOffset();
  }

  /**
   * Computes the length of the minimal distinguishing prefix (MDP) between a current term and its
   * previous term (terms are alphabetically sorted).
   *
   * <p>Example: If previous="car" and current="cartridge", then MDP length is 4. It is the length
   * of the minimal prefix distinguishing "cartridge" from "car", that is, the length of "cart".
   *
   * @see TermBytes
   */
  public static int computeMdpLength(BytesRef previousTerm, BytesRef currentTerm) {
    int mdpLength =
        previousTerm == null ? 1 : StringHelper.sortKeyLength(previousTerm, currentTerm);
    return Math.min(mdpLength, currentTerm.length);
  }

  @Override
  public long ramBytesUsed() {
    return BASE_RAM_USAGE + RamUsageUtil.ramBytesUsed(term);
  }
}

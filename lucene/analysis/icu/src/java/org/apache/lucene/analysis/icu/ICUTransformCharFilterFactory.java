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
package org.apache.lucene.analysis.icu;

import java.util.Arrays;
import java.util.Map;
import java.io.Reader;
import org.apache.lucene.analysis.util.CharFilterFactory;

import com.ibm.icu.impl.Utility;
import com.ibm.icu.text.Transliterator;
import com.ibm.icu.text.UTF16;

/**
 * Factory for {@link ICUTransformCharFilter}.
 * <p>
 * Supports the following attributes:
 * <ul>
 * <li>id (mandatory): A Transliterator ID, one from {@link Transliterator#getAvailableIDs()}
 * <li>direction (optional): Either 'forward' or 'reverse'. Default is forward.
 * </ul>
 * 
 * @see Transliterator
 * @since 8.3.0
 * @lucene.spi {@value #NAME}
 */
public class ICUTransformCharFilterFactory extends CharFilterFactory {

  /** SPI name */
  public static final String NAME = "icuTransform";

  private final Transliterator transliterator;
  private final int maxRollbackBufferCapacity;

  // TODO: add support for custom rules
  /** Creates a new ICUTransformFilterFactory */
  public ICUTransformCharFilterFactory(Map<String,String> args) {
    super(args);
    String id = require(args, "id");
    String direction = get(args, "direction", Arrays.asList("forward", "reverse"), "forward", false);
    int dir = "forward".equals(direction) ? Transliterator.FORWARD : Transliterator.REVERSE;
    int tmpCapacityHint = getInt(args, "maxRollbackBufferCapacity", ICUTransformCharFilter.DEFAULT_MAX_ROLLBACK_BUFFER_CAPACITY);
    this.maxRollbackBufferCapacity = tmpCapacityHint == -1 ? Integer.MAX_VALUE : tmpCapacityHint;
    boolean assumeExternalUnicodeNormalization = getBoolean(args, "assumeExternalUnicodeNormalization", false);
    Transliterator stockTransliterator = Transliterator.getInstance(id, dir);
    final String modifiedRules;
    if (!assumeExternalUnicodeNormalization || ((modifiedRules = modifyRules(false, stockTransliterator)) == null)) {
      this.transliterator = stockTransliterator;
    } else {
      String baseId = stockTransliterator.getID();
      String modId = baseId.concat(baseId.lastIndexOf('/') < 0 ? "/X_NO_NORM_IO" : "_X_NO_NORM_IO");
      this.transliterator = Transliterator.createFromRules(modId, modifiedRules, Transliterator.FORWARD);
    }
    if (!args.isEmpty()) {
      throw new IllegalArgumentException("Unknown parameters: " + args);
    }
  }

  @Override
  public Reader create(Reader input) {
    return new ICUTransformCharFilter(input, transliterator, maxRollbackBufferCapacity);
  }

  private static final char ID_DELIM = ';';

  private static final String[] NORM_ID_ARR = new String[] {"NFC", "NFD", "NFKC", "NFKD", "FCC", "FCD"};

  /**
   * Return true if the specified String represents the id of a NormalizationTransliterator, otherwise false.
   */
  private static boolean isUnicodeNormalizationId(String id) {
    if (id.indexOf(';') >= 0) {
      // it's compound
      return false;
    }
    if (id.startsWith("Any-")) {
      id = id.substring("Any-".length());
    }
    for (String prefix : NORM_ID_ARR) {
      if (id.startsWith(prefix)) {
        return true;
      }
    }
    return false;
  }

  /**
   * This is based on the {@link com.ibm.icu.text.CompoundTransliterator#toRules(boolean)}
   * method, modified to return a version of rules with initial and trailing unicode
   * normalization removed. If neither leading nor trailing unicode normalization is present,
   * then no modifications are called for which this method indicates by returning null.
   *
   * Analogous to the contract for {@link com.ibm.icu.text.Transliterator#toRules(boolean)}, any
   * modified rules String returned should be sufficient to recreate a Transliterator based
   * on the specified input Transliterator, via {@link com.ibm.icu.text.Transliterator#createFromRules(String, String, int)}.
   *
   * @param escapeUnprintable escape unprintable chars
   * @param t the Transliterator to base modified rules on.
   * @return modified form of rules for input Transliterator, or null if no modification is
   * called for.
   */
  public static String modifyRules(boolean escapeUnprintable, Transliterator t) {
    final Transliterator[] trans = t.getElements();
    final int start;
    final int limit;
    if (trans.length == 1) {
      return null;
    } else {
      final int lastIndex;
      if (isUnicodeNormalizationId(trans[0].getID())) {
        start = 1;
        limit = isUnicodeNormalizationId(trans[lastIndex = trans.length - 1].getID()) ? lastIndex : trans.length;
      } else if (isUnicodeNormalizationId(trans[lastIndex = trans.length - 1].getID())) {
        start = 0;
        limit = lastIndex;
      } else {
        return null;
      }
    }
    // We do NOT call toRules() on our component transliterators, in
    // general. If we have several rule-based transliterators, this
    // yields a concatenation of the rules -- not what we want. We do
    // handle compound RBT transliterators specially -- those for which
    // compoundRBTIndex >= 0. For the transliterator at compoundRBTIndex,
    // we do call toRules() recursively.
    StringBuilder rulesSource = new StringBuilder();
    if (t.getFilter() != null) {
      // We might be a compound RBT and if we have a global
      // filter, then emit it at the top.
      rulesSource.append("::").append(t.getFilter().toPattern(escapeUnprintable)).append(ID_DELIM);
    }
    final int globalFilterEnd = rulesSource.length();
    boolean hasAnonymousRBTs = false;
    for (int i = start; i < limit; ++i) {
      String rule;

      // Anonymous RuleBasedTransliterators (inline rules and
      // ::BEGIN/::END blocks) are given IDs that begin with
      // "%Pass": use toRules() to write all the rules to the output
      // (and insert "::Null;" if we have two in a row)
      if (trans[i].getID().startsWith("%Pass")) {
        hasAnonymousRBTs = true;
        rule = trans[i].toRules(escapeUnprintable);
        if (i > start && trans[i - 1].getID().startsWith("%Pass"))
          rule = "::Null;" + rule;

        // we also use toRules() on CompoundTransliterators (which we
        // check for by looking for a semicolon in the ID)-- this gets
        // the list of their child transliterators output in the right
        // format
      } else if (trans[i].getID().indexOf(';') >= 0) {
        rule = trans[i].toRules(escapeUnprintable);

        // for everything else, use baseToRules()
      } else {
        rule = baseToRules(escapeUnprintable, trans[i]);
      }
      _smartAppend(rulesSource, '\n');
      rulesSource.append(rule);
      _smartAppend(rulesSource, ID_DELIM);
    }
    return hasAnonymousRBTs ? rulesSource.toString() : rulesSource.substring(globalFilterEnd);
  }

  /**
   * Append c to buf, unless buf is empty or buf already ends in c.
   * (convenience method copied from {@link com.ibm.icu.text.CompoundTransliterator})
   */
  private static void _smartAppend(StringBuilder buf, char c) {
    if (buf.length() != 0 &&
        buf.charAt(buf.length() - 1) != c) {
      buf.append(c);
    }
  }

  /**
   * This method is essentially copied from {@link com.ibm.icu.text.Transliterator#baseToRules(boolean)}
   * @param escapeUnprintable escape unprintable chars
   * @param t the Transliterator to dump rules for
   * @return String representing rules for the specified Transliterator
   */
  private static String baseToRules(boolean escapeUnprintable, Transliterator t) {
    // The base class implementation of toRules munges the ID into
    // the correct format. That is: foo => ::foo
    // KEEP in sync with rbt_pars
    if (escapeUnprintable) {
      StringBuffer rulesSource = new StringBuffer();
      String id = t.getID();
      for (int i = 0; i < id.length();) {
        int c = UTF16.charAt(id, i);
        if (!Utility.escapeUnprintable(rulesSource, c)) {
          UTF16.append(rulesSource, c);
        }
        i += UTF16.getCharCount(c);
      }
      rulesSource.insert(0, "::");
      rulesSource.append(ID_DELIM);
      return rulesSource.toString();
    }
    return "::" + t.getID() + ID_DELIM;
  }
}

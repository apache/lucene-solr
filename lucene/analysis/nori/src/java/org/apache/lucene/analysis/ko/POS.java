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
package org.apache.lucene.analysis.ko;

import java.util.Locale;

/**
 * Part of speech classification for Korean based on Sejong corpus classification. The list of tags
 * and their meanings is available here:
 * https://docs.google.com/spreadsheets/d/1-9blXKjtjeKZqsf4NzHeYJCrr49-nXeRF6D80udfcwY
 */
public class POS {

  /** The type of the token. */
  public enum Type {
    /** A simple morpheme. */
    MORPHEME,

    /** Compound noun. */
    COMPOUND,

    /** Inflected token. */
    INFLECT,

    /** Pre-analysis token. */
    PREANALYSIS,
  }

  /** Part of speech tag for Korean based on Sejong corpus classification. */
  public enum Tag {
    /** Verbal endings */
    E(100, "Verbal endings"),

    /** Interjection */
    IC(110, "Interjection"),

    /** Ending Particle */
    J(120, "Ending Particle"),

    /** General Adverb */
    MAG(130, "General Adverb"),

    /** Conjunctive adverb */
    MAJ(131, "Conjunctive adverb"),

    /** Determiner */
    MM(140, "Modifier"),

    /** General Noun */
    NNG(150, "General Noun"),

    /** Proper Noun */
    NNP(151, "Proper Noun"),

    /** Dependent noun (following nouns) */
    NNB(152, "Dependent noun"),

    /** Dependent noun */
    NNBC(153, "Dependent noun"),

    /** Pronoun */
    NP(154, "Pronoun"),

    /** Numeral */
    NR(155, "Numeral"),

    /** Terminal punctuation (? ! .) */
    SF(160, "Terminal punctuation"),

    /** Chinese character */
    SH(161, "Chinese Characeter"),

    /** Foreign language */
    SL(162, "Foreign language"),

    /** Number */
    SN(163, "Number"),

    /** Space */
    SP(164, "Space"),

    /** Closing brackets */
    SSC(165, "Closing brackets"),

    /** Opening brackets */
    SSO(166, "Opening brackets"),

    /** Separator (· / :) */
    SC(167, "Separator"),

    /** Other symbol */
    SY(168, "Other symbol"),

    /** Ellipsis */
    SE(169, "Ellipsis"),

    /** Adjective */
    VA(170, "Adjective"),

    /** Negative designator */
    VCN(171, "Negative designator"),

    /** Positive designator */
    VCP(172, "Positive designator"),

    /** Verb */
    VV(173, "Verb"),

    /** Auxiliary Verb or Adjective */
    VX(174, "Auxiliary Verb or Adjective"),

    /** Prefix */
    XPN(181, "Prefix"),

    /** Root */
    XR(182, "Root"),

    /** Adjective Suffix */
    XSA(183, "Adjective Suffix"),

    /** Noun Suffix */
    XSN(184, "Noun Suffix"),

    /** Verb Suffix */
    XSV(185, "Verb Suffix"),

    /** Unknown */
    UNKNOWN(999, "Unknown"),

    /** Unknown */
    UNA(-1, "Unknown"),

    /** Unknown */
    NA(-1, "Unknown"),

    /** Unknown */
    VSV(-1, "Unknown");

    private final int code;
    private final String desc;

    /** Returns the code associated with the tag (as defined in pos-id.def). */
    public int code() {
      return code;
    }

    /** Returns the description associated with the tag. */
    public String description() {
      return desc;
    }

    /**
     * Returns a new part of speech tag.
     *
     * @param code The code for the tag.
     * @param desc The description of the tag.
     */
    Tag(int code, String desc) {
      this.code = code;
      this.desc = desc;
    }
  }

  /** Returns the {@link Tag} of the provided <code>name</code>. */
  public static Tag resolveTag(String name) {
    String tagUpper = name.toUpperCase(Locale.ENGLISH);
    if (tagUpper.startsWith("J")) {
      return Tag.J;
    } else if (tagUpper.startsWith("E")) {
      return Tag.E;
    } else {
      return Tag.valueOf(tagUpper);
    }
  }

  /** Returns the {@link Tag} of the provided <code>tag</code>. */
  public static Tag resolveTag(byte tag) {
    assert tag < Tag.values().length;
    return Tag.values()[tag];
  }

  /** Returns the {@link Type} of the provided <code>name</code>. */
  public static Type resolveType(String name) {
    if ("*".equals(name)) {
      return Type.MORPHEME;
    }
    return Type.valueOf(name.toUpperCase(Locale.ENGLISH));
  }

  /** Returns the {@link Type} of the provided <code>type</code>. */
  public static Type resolveType(byte type) {
    assert type < Type.values().length;
    return Type.values()[type];
  }
}

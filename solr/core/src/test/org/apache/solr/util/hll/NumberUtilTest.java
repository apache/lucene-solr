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
package org.apache.solr.util.hll;

import org.junit.Test;
import static org.junit.Assert.*;

import java.util.Arrays;

/**
 * Tests {@link NumberUtil}
 */
public class NumberUtilTest {

  final static byte[] ALL_PRINTABLE_ASCII_CHARS = new byte[] { ' ', '!', '"', '#', '$', '%', '&', '\'', '(', ')', '*',
      '+', ',', '-', '.', '/', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', ':', ';', '<', '=', '>', '?', '@', 'A',
      'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X',
      'Y', 'Z', '[', '\\', ']', '^', '_', '`', 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n',
      'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z', '{', '|', '}', '~', '', };

  final static byte[] ALL_WORD_CHARAC_ASCII_CHARS = new byte[] { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A',
      'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X',
      'Y', 'Z' };

  final static byte[] ALL_NUMBER_CHARAC_ASCII_CHARS = new byte[] { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9'};

  final static byte[] ALL_LETTER_ASCII_CHARS = new byte[] { 'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L',
      'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z' };

  final static String ALL_NUMBER_ASCII_CHARS_IN_HEX = "30313233343536373839";
  final static String ALL_MAJ_LETTERS_ASCII_CHARS_IN_HEX = "4142434445464748494A4B4C4D4E4F" + "505152535455565758595A";
  final static String ALL_LOW_LETTERS_ASCII_CHARS_IN_HEX = "6162636465666768696A6B6C6D6E6F" + "707172737475767778797A";

  final static String ALL_PRINTABLE_ASCII_CHARS_IN_HEX = "202122232425262728292A2B2C2D2E2F"
      + ALL_NUMBER_ASCII_CHARS_IN_HEX + "3A3B3C3D3E3F" + "40" + ALL_MAJ_LETTERS_ASCII_CHARS_IN_HEX + "5B5C5D5E5F" + "60"
      + ALL_LOW_LETTERS_ASCII_CHARS_IN_HEX + "7B7C7D7E7F";

  /**
   * Test {@link NumberUtil#log2(double)}.
   */
  @Test
  public void testLog2() {
    final double log2Result = NumberUtil.log2(2d);
    assertTrue(log2Result == 1);
  }

  /**
   * Test {@link NumberUtil#toHex(byte[], int, int)}
   */
  @Test
  public void TestToHex() {
    assertTrue(ALL_PRINTABLE_ASCII_CHARS_IN_HEX
        .equals(NumberUtil.toHex(ALL_PRINTABLE_ASCII_CHARS, 0, ALL_PRINTABLE_ASCII_CHARS.length)));
  }

  /**
   * Test {@link NumberUtil#toHex(byte[], int, int)}
   */
  @Test
  public void TestToHexWithOffset() {
    assertTrue(ALL_MAJ_LETTERS_ASCII_CHARS_IN_HEX
        .equals(NumberUtil.toHex(ALL_WORD_CHARAC_ASCII_CHARS, 10, ALL_PRINTABLE_ASCII_CHARS.length)));
  }

  /**
   * Test {@link NumberUtil#toHex(byte[], int, int)}
   */
  @Test
  public void TestToHexWithCountt() {
    assertTrue(ALL_NUMBER_ASCII_CHARS_IN_HEX.equals(NumberUtil.toHex(ALL_WORD_CHARAC_ASCII_CHARS, 0, 10)));
  }

  /**
   * Test {@link NumberUtil#fromHex(String, int, int)}
   */
  @Test
  public void TestFromHex() {
    assertTrue(
        Arrays
            .equals(
                NumberUtil.fromHex(ALL_NUMBER_ASCII_CHARS_IN_HEX + ALL_MAJ_LETTERS_ASCII_CHARS_IN_HEX
                    + ALL_LOW_LETTERS_ASCII_CHARS_IN_HEX, 0, ALL_WORD_CHARAC_ASCII_CHARS.length * 2),
        ALL_WORD_CHARAC_ASCII_CHARS));
  }

  /**
   * Test {@link NumberUtil#fromHex(String, int, int)}
   */
  @Test
  public void TestFromHexWithOffset() {
    assertTrue(Arrays.equals(NumberUtil.fromHex(ALL_NUMBER_ASCII_CHARS_IN_HEX + ALL_MAJ_LETTERS_ASCII_CHARS_IN_HEX, 20,
        ALL_LETTER_ASCII_CHARS.length * 2), ALL_LETTER_ASCII_CHARS));
  }

  /**
   * Test {@link NumberUtil#fromHex(String, int, int)}
   */
  @Test
  public void TestFromHexWithCount() {
    assertTrue(Arrays.equals(NumberUtil.fromHex(ALL_NUMBER_ASCII_CHARS_IN_HEX + ALL_MAJ_LETTERS_ASCII_CHARS_IN_HEX, 0,
        20), ALL_NUMBER_CHARAC_ASCII_CHARS));
  }
}

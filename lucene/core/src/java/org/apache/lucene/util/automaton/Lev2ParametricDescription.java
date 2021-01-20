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
package org.apache.lucene.util.automaton;

// The following code was generated with the moman/finenight pkg
// This package is available under the MIT License, see NOTICE.txt
// for more details.

import org.apache.lucene.util.automaton.LevenshteinAutomata.ParametricDescription;

/** Parametric description for generating a Levenshtein automaton of degree 2 */
class Lev2ParametricDescription extends ParametricDescription {

  @Override
  int transition(int absState, int position, int vector) {
    // null absState should never be passed in
    assert absState != -1;

    // decode absState -> state, offset
    int state = absState / (w + 1);
    int offset = absState % (w + 1);
    assert offset >= 0;

    if (position == w) {
      if (state < 3) {
        final int loc = vector * 3 + state;
        offset += unpack(offsetIncrs0, loc, 1);
        state = unpack(toStates0, loc, 2) - 1;
      }
    } else if (position == w - 1) {
      if (state < 5) {
        final int loc = vector * 5 + state;
        offset += unpack(offsetIncrs1, loc, 1);
        state = unpack(toStates1, loc, 3) - 1;
      }
    } else if (position == w - 2) {
      if (state < 11) {
        final int loc = vector * 11 + state;
        offset += unpack(offsetIncrs2, loc, 2);
        state = unpack(toStates2, loc, 4) - 1;
      }
    } else if (position == w - 3) {
      if (state < 21) {
        final int loc = vector * 21 + state;
        offset += unpack(offsetIncrs3, loc, 2);
        state = unpack(toStates3, loc, 5) - 1;
      }
    } else if (position == w - 4) {
      if (state < 30) {
        final int loc = vector * 30 + state;
        offset += unpack(offsetIncrs4, loc, 3);
        state = unpack(toStates4, loc, 5) - 1;
      }
    } else {
      if (state < 30) {
        final int loc = vector * 30 + state;
        offset += unpack(offsetIncrs5, loc, 3);
        state = unpack(toStates5, loc, 5) - 1;
      }
    }

    if (state == -1) {
      // null state
      return -1;
    } else {
      // translate back to abs
      return state * (w + 1) + offset;
    }
  }

  // 1 vectors; 3 states per vector; array length = 3
  private static final long[] toStates0 = new long[] /*2 bits per value */ {0xeL};
  private static final long[] offsetIncrs0 = new long[] /*1 bits per value */ {0x0L};

  // 2 vectors; 5 states per vector; array length = 10
  private static final long[] toStates1 = new long[] /*3 bits per value */ {0x1a688a2cL};
  private static final long[] offsetIncrs1 = new long[] /*1 bits per value */ {0x3e0L};

  // 4 vectors; 11 states per vector; array length = 44
  private static final long[] toStates2 =
      new long[] /*4 bits per value */ {0x3a07603570707054L, 0x522323232103773aL, 0x352254543213L};
  private static final long[] offsetIncrs2 =
      new long[] /*2 bits per value */ {0x5555520880080000L, 0x555555L};

  // 8 vectors; 21 states per vector; array length = 168
  private static final long[] toStates3 =
      new long[] /*5 bits per value */ {
        0x7000a560180380a4L, 0xc015a0180a0194aL, 0x8032c58318a301c0L, 0x9d8350d403980318L,
        0x3006028ca73a8602L, 0xc51462640b21a807L, 0x2310c4100c62194eL, 0xce35884218ce248dL,
        0xa9285a0691882358L, 0x1046b5a86b1252b5L, 0x2110a33892521483L, 0xe62906208d63394eL,
        0xd6a29c4921d6a4a0L, 0x1aL
      };
  private static final long[] offsetIncrs3 =
      new long[] /*2 bits per value */ {
        0xf0c000c8c0080000L,
        0xca808822003f303L,
        0x5555553fa02f0880L,
        0x5555555555555555L,
        0x5555555555555555L,
        0x5555L
      };

  // 16 vectors; 30 states per vector; array length = 480
  private static final long[] toStates4 =
      new long[] /*5 bits per value */ {
        0x7000a560180380a4L, 0xa000000280e0294aL, 0x6c0b00e029000000L, 0x8c4350c59cdc6039L,
        0x600ad00c03380601L, 0x2962c18c5180e00L, 0x18c4000c6028c4L, 0x8a314603801802b4L,
        0x6328c4520c59c5L, 0x60d43500e600c651L, 0x280e339cea180a7L, 0x4039800000a318c6L,
        0xd57be96039ec3d0dL, 0xc0338d6358c4352L, 0x28c4c81643500e60L, 0x3194a028c4339d8aL,
        0x590d403980018c4L, 0xc4522d57b68e3132L, 0xc4100c6510d6538L, 0x9884218ce248d231L,
        0x318ce318c6398d83L, 0xa3609c370c431046L, 0xea3ad6958568f7beL, 0x2d0348c411d47560L,
        0x9ad43989295ad494L, 0x3104635ad431ad63L, 0x8f73a6b5250b40d2L, 0x57350eab9d693956L,
        0x8ce24948520c411dL, 0x294a398d85608442L, 0x5694831046318ce5L, 0x958460f7b623609cL,
        0xc411d475616258d6L, 0x9243ad4941cc520L, 0x5ad4529ce39ad456L, 0xb525073148310463L,
        0x27656939460f7358L, 0x1d573516L
      };
  private static final long[] offsetIncrs4 =
      new long[] /*3 bits per value */ {
        0x610600010000000L, 0x2040000000001000L, 0x1044209245200L, 0x80d86d86006d80c0L,
        0x2001b6030000006dL, 0x8200011b6237237L, 0x12490612400410L, 0x2449001040208000L,
        0x4d80820001044925L, 0x6da4906da400L, 0x9252369001360208L, 0x24924924924911b6L,
        0x9249249249249249L, 0x4924924924924924L, 0x2492492492492492L, 0x9249249249249249L,
        0x4924924924924924L, 0x2492492492492492L, 0x9249249249249249L, 0x4924924924924924L,
        0x2492492492492492L, 0x9249249249249249L, 0x24924924L
      };

  // 32 vectors; 30 states per vector; array length = 960
  private static final long[] toStates5 =
      new long[] /*5 bits per value */ {
        0x7000a560180380a4L, 0xa000000280e0294aL, 0x580600e029000000L, 0x80e0600e529c0029L,
        0x380a418c6388c631L, 0x316737180e5b02c0L, 0x300ce01806310d4L, 0xc60396c0b00e0290L,
        0xca328c4350c59cdL, 0x80e00600ad194656L, 0x28c402962c18c51L, 0x802b40018c4000c6L,
        0xe58b06314603801L, 0x8d6b48c6b580e348L, 0x28c5180e00600ad1L, 0x18ca31148316716L,
        0x3801802b4031944L, 0xc4520c59c58a3146L, 0xe61956748cab38L, 0x39cea180a760d435L,
        0xa318c60280e3L, 0x6029d8350d403980L, 0x6b5a80e060d873a8L, 0xf43500e618c638dL,
        0x10d4b55efa580e7bL, 0x3980300ce358d63L, 0x57be96039ec3d0d4L, 0x4656567598c4352dL,
        0x8c4c81643500e619L, 0x194a028c4339d8a2L, 0x590d403980018c43L, 0xe348d87628a31320L,
        0xe618d6b4d6b1880L, 0x5eda38c4c8164350L, 0x19443594e31148b5L, 0x31320590d4039803L,
        0x7160c4522d57b68eL, 0xd2310c41195674d6L, 0x8d839884218ce248L, 0x1046318ce318c639L,
        0x2108633892348c43L, 0xdebfbdef0f63b0f6L, 0xd8270dc310c41f7bL, 0x8eb5a5615a3defa8L,
        0x70c43104751d583aL, 0x58568f7bea3609c3L, 0x41f77ddb7bbeed69L, 0x9295ad4942d0348cL,
        0xad431ad639ad4398L, 0x5250b40d23104635L, 0xce0f6bd0f624a56bL, 0x348c41f7b9cd7bdL,
        0xe55a3dce9ad4942dL, 0x4755cd43aae75a4L, 0x73a6b5250b40d231L, 0xbd7bbcdd6939568fL,
        0xe24948520c41f779L, 0x4a398d856084428cL, 0x14831046318ce529L, 0xb16c2110a3389252L,
        0x1f7bdebe739c8f63L, 0xed88d82715a520c4L, 0x58589635a561183dL, 0x9c569483104751dL,
        0xc56958460f7b6236L, 0x520c41f77ddb6719L, 0x45609243ad4941ccL, 0x4635ad4529ce39adL,
        0x90eb525073148310L, 0xd6737b8f6bd16c24L, 0x941cc520c41f7b9cL, 0x95a4e5183dcd62d4L,
        0x483104755cd4589dL, 0x460f7358b5250731L, 0xf779bd6717b56939L
      };
  private static final long[] offsetIncrs5 =
      new long[] /*3 bits per value */ {
        0x610600010000000L, 0x40000000001000L, 0xb6d56da184180L, 0x824914800810000L,
        0x2002040000000411L, 0xc0000b2c5659245L, 0x6d80d86d86006d8L, 0x1b61801b60300000L,
        0x6d80c0000b5b76b6L, 0x46d88dc8dc800L, 0x6372372001b60300L, 0x400410082000b1b7L,
        0x2080000012490612L, 0x6d49241849001040L, 0x912400410082000bL, 0x402080004112494L,
        0xb2c49252449001L, 0x4906da4004d80820L, 0x136020800006daL, 0x82000b5b69241b69L,
        0x6da4948da4004d80L, 0x3690013602080004L, 0x49249249b1b69252L, 0x2492492492492492L,
        0x9249249249249249L, 0x4924924924924924L, 0x2492492492492492L, 0x9249249249249249L,
        0x4924924924924924L, 0x2492492492492492L, 0x9249249249249249L, 0x4924924924924924L,
        0x2492492492492492L, 0x9249249249249249L, 0x4924924924924924L, 0x2492492492492492L,
        0x9249249249249249L, 0x4924924924924924L, 0x2492492492492492L, 0x9249249249249249L,
        0x4924924924924924L, 0x2492492492492492L, 0x9249249249249249L, 0x4924924924924924L,
        0x2492492492492492L
      };

  // state map
  //   0 -> [(0, 0)]
  //   1 -> [(0, 1)]
  //   2 -> [(0, 2)]
  //   3 -> [(0, 1), (1, 1)]
  //   4 -> [(0, 2), (1, 2)]
  //   5 -> [(0, 1), (1, 1), (2, 1)]
  //   6 -> [(0, 2), (1, 2), (2, 2)]
  //   7 -> [(0, 1), (2, 1)]
  //   8 -> [(0, 1), (2, 2)]
  //   9 -> [(0, 2), (2, 1)]
  //   10 -> [(0, 2), (2, 2)]
  //   11 -> [(0, 2), (1, 2), (2, 2), (3, 2)]
  //   12 -> [(0, 1), (1, 1), (3, 2)]
  //   13 -> [(0, 1), (2, 2), (3, 2)]
  //   14 -> [(0, 1), (3, 2)]
  //   15 -> [(0, 2), (1, 2), (3, 1)]
  //   16 -> [(0, 2), (1, 2), (3, 2)]
  //   17 -> [(0, 2), (2, 1), (3, 1)]
  //   18 -> [(0, 2), (2, 2), (3, 2)]
  //   19 -> [(0, 2), (3, 1)]
  //   20 -> [(0, 2), (3, 2)]
  //   21 -> [(0, 2), (1, 2), (2, 2), (3, 2), (4, 2)]
  //   22 -> [(0, 2), (1, 2), (2, 2), (4, 2)]
  //   23 -> [(0, 2), (1, 2), (3, 2), (4, 2)]
  //   24 -> [(0, 2), (1, 2), (4, 2)]
  //   25 -> [(0, 2), (2, 1), (4, 2)]
  //   26 -> [(0, 2), (2, 2), (3, 2), (4, 2)]
  //   27 -> [(0, 2), (2, 2), (4, 2)]
  //   28 -> [(0, 2), (3, 2), (4, 2)]
  //   29 -> [(0, 2), (4, 2)]

  public Lev2ParametricDescription(int w) {
    super(
        w,
        2,
        new int[] {
          0, 1, 2, 0, 1, -1, 0, -1, 0, -1, 0, -1, -1, -1, -1, -2, -1, -2, -1, -2, -1, -2, -2, -2,
          -2, -2, -2, -2, -2, -2
        });
  }
}

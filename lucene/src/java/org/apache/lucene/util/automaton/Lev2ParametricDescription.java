package org.apache.lucene.util.automaton;

/**
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
    int state = absState/(w+1);
    int offset = absState%(w+1);
    assert offset >= 0;
    
    if (position == w) {
      if (state < 3) {
        final int loc = vector * 3 + state;
        offset += unpack(offsetIncrs0, loc, 1);
        state = unpack(toStates0, loc, 2)-1;
      }
    } else if (position == w-1) {
      if (state < 5) {
        final int loc = vector * 5 + state;
        offset += unpack(offsetIncrs1, loc, 1);
        state = unpack(toStates1, loc, 3)-1;
      }
    } else if (position == w-2) {
      if (state < 11) {
        final int loc = vector * 11 + state;
        offset += unpack(offsetIncrs2, loc, 2);
        state = unpack(toStates2, loc, 4)-1;
      }
    } else if (position == w-3) {
      if (state < 21) {
        final int loc = vector * 21 + state;
        offset += unpack(offsetIncrs3, loc, 2);
        state = unpack(toStates3, loc, 5)-1;
      }
    } else if (position == w-4) {
      if (state < 30) {
        final int loc = vector * 30 + state;
        offset += unpack(offsetIncrs4, loc, 3);
        state = unpack(toStates4, loc, 5)-1;
      }
    } else {
      if (state < 30) {
        final int loc = vector * 30 + state;
        offset += unpack(offsetIncrs5, loc, 3);
        state = unpack(toStates5, loc, 5)-1;
      }
    }
    
    if (state == -1) {
      // null state
      return -1;
    } else {
      // translate back to abs
      return state*(w+1)+offset;
    }
  }
    
  // 1 vectors; 3 states per vector; array length = 3
  private final static long[] toStates0 = new long[] /*2 bits per value */ {
    0x23L
  };
  private final static long[] offsetIncrs0 = new long[] /*1 bits per value */ {
    0x0L
  };
    
  // 2 vectors; 5 states per vector; array length = 10
  private final static long[] toStates1 = new long[] /*3 bits per value */ {
    0x1a68c105L
  };
  private final static long[] offsetIncrs1 = new long[] /*1 bits per value */ {
    0x3e0L
  };
    
  // 4 vectors; 11 states per vector; array length = 44
  private final static long[] toStates2 = new long[] /*4 bits per value */ {
    0x6280b80804280405L,0x2323432321608282L,0x523434543213L
  };
  private final static long[] offsetIncrs2 = new long[] /*2 bits per value */ {
    0x5555502220000800L,0x555555L
  };
    
  // 8 vectors; 21 states per vector; array length = 168
  private final static long[] toStates3 = new long[] /*5 bits per value */ {
    0x40300c0108801005L,0x80202a8208801000L,0x4021006280a0288dL,0x30482184802d8414L,
    0x5990240880010460L,0x191a28118330900L,0x310c413204c1104L,0x8625084811c4710dL,
    0xa92a398e2188231aL,0x104e351c4a508ca4L,0x21208511c8341483L,0xe6290620946a1910L,
    0xd47221423216a4a0L,0x28L
  };
  private final static long[] offsetIncrs3 = new long[] /*2 bits per value */ {
    0x33300030c2000800L,0x32828088800c3cfL,0x5555550cace32320L,0x5555555555555555L,
    0x5555555555555555L,0x5555L
  };
    
  // 16 vectors; 30 states per vector; array length = 480
  private final static long[] toStates4 = new long[] /*5 bits per value */ {
    0x80300c0108801005L,0x88210802000L,0x44200401400000L,0x7ae3b88621185c07L,
    0x101500042100404L,0x20803140501446cL,0x40100420006c2122L,0x490140511b004054L,
    0x8401f2e3c086411L,0x120861200b100822L,0x641102400081180cL,0x4802c40100001088L,
    0x8c21195607048418L,0x1421014245bc3f2L,0x23450230661200b1L,0x2108664118240803L,
    0x8c1984802c802004L,0xbc3e28c41150d140L,0xc4120102209421dL,0x7884c11c4710d031L,
    0x210842109031bc62L,0xd21484360c431044L,0x9c265293a3a6e741L,0x1cc710c41109ce70L,
    0x1bce27a846525495L,0x3105425094a108c7L,0x6f735e95254731c4L,0x9ee7a9c234a9393aL,
    0x144720d0520c4150L,0x211051bc646084c2L,0x3614831048220842L,0x93a460e742351488L,
    0xc4120a2e70a24656L,0x284642d4941cc520L,0x4094a210c51bce46L,0xb525073148310502L,
    0x24356939460f7358L,0x4098e7aaL
  };
  private final static long[] offsetIncrs4 = new long[] /*3 bits per value */ {
    0xc0602000010000L,0xa000040000000001L,0x248204041248L,0xb0180c06c3618618L,
    0x238d861860001861L,0x41040061c6e06041L,0x4004900c2402400L,0x409489001041001L,
    0x4184184004148124L,0x1041b4980c24c3L,0xd26040938d061061L,0x2492492492494146L,
    0x9249249249249249L,0x4924924924924924L,0x2492492492492492L,0x9249249249249249L,
    0x4924924924924924L,0x2492492492492492L,0x9249249249249249L,0x4924924924924924L,
    0x2492492492492492L,0x9249249249249249L,0x24924924L
  };
    
  // 32 vectors; 30 states per vector; array length = 960
  private final static long[] toStates5 = new long[] /*5 bits per value */ {
    0x80300c0108801005L,0x88210802000L,0x42200401400000L,0xa088201000300c03L,
    0x100510842108428L,0x2188461701c01108L,0x108401011eb8eeL,0x85c0700442004014L,
    0x88267ae3b886211L,0x1446c01015108842L,0xc212202080314050L,0x405440100420006L,
    0x10201c50140511b0L,0x942528423b08888L,0x240501446c010155L,0x21007cb8f0219045L,
    0x511b004054402088L,0x2e3c086411490140L,0x200b50904428823fL,0x400081180c120861L,
    0x100001088641102L,0x46030482184802c4L,0x9ce8990840980030L,0x21061200b709c210L,
    0xf0fca308465581c1L,0x802c405084050916L,0xc211956070484184L,0x9e4209ee65bc3f28L,
    0x3450230661200b70L,0x1086641182408032L,0xc1984802c8020042L,0x86098201c8d1408L,
    0xb88a22529ce399L,0x1045434502306612L,0x4088250876f0f8a3L,0xd1408c1984802c80L,
    0xee3dbc3e28c41150L,0xd0310c4188984429L,0xbc627884c11c4710L,0x1044210842109031L,
    0x21704711c4340c43L,0xbdef7bdf0c7a18b4L,0x85210d8310c41ef7L,0x994a4e8e9b9d074L,
    0x60c4310442739c27L,0x3a3a6e741d214843L,0x41ef77bdf77de529L,0x8465254951cc710cL,
    0x94a108c71bce27aL,0x5254731c43105425L,0xdb1c7a38b4a15949L,0xc710c41cf73dce7bL,
    0xe4e9bdcd7a54951cL,0x5427b9ea708d2a4L,0x735e95254731c431L,0xbd677db4a9393a6fL,
    0x4720d0520c41cf75L,0x1051bc646084c214L,0x1483104822084221L,0x193821708511c834L,
    0x1bf6fdef6f7f147aL,0xd08d45220d8520c4L,0x9c289195a4e91839L,0x488361483104828bL,
    0xe5693a460e742351L,0x520c41bf71bdf717L,0xe46284642d4941ccL,0x5024094a210c51bcL,
    0x590b525073148310L,0xce6f7b147a3938a1L,0x941cc520c41f77ddL,0xd5a4e5183dcd62d4L,
    0x48310502639ea890L,0x460f7358b5250731L,0xf779bd6717b56939L
  };
  private final static long[] offsetIncrs5 = new long[] /*3 bits per value */ {
    0xc0602000010000L,0x8000040000000001L,0xb6db6d4030180L,0x810104922800010L,
    0x248a000040000092L,0x618000b649654041L,0x861b0180c06c3618L,0x301b0d861860001L,
    0x61861800075d6ed6L,0x1871b8181048e3L,0xe56041238d861860L,0x40240041040075c6L,
    0x4100104004900c2L,0x55b5240309009001L,0x1025224004104005L,0x10410010520490L,
    0x55495240409489L,0x4980c24c34184184L,0x30d061061001041bL,0x184005556d260309L,
    0x51b4981024e34184L,0x40938d0610610010L,0x492492495546d260L,0x2492492492492492L,
    0x9249249249249249L,0x4924924924924924L,0x2492492492492492L,0x9249249249249249L,
    0x4924924924924924L,0x2492492492492492L,0x9249249249249249L,0x4924924924924924L,
    0x2492492492492492L,0x9249249249249249L,0x4924924924924924L,0x2492492492492492L,
    0x9249249249249249L,0x4924924924924924L,0x2492492492492492L,0x9249249249249249L,
    0x4924924924924924L,0x2492492492492492L,0x9249249249249249L,0x4924924924924924L,
    0x2492492492492492L
  };
  
  // state map
  //   0 -> [(0, 0)]
  //   1 -> [(0, 2)]
  //   2 -> [(0, 1)]
  //   3 -> [(0, 2), (1, 2)]
  //   4 -> [(0, 1), (1, 1)]
  //   5 -> [(0, 2), (2, 1)]
  //   6 -> [(0, 1), (2, 2)]
  //   7 -> [(0, 2), (1, 2), (2, 2)]
  //   8 -> [(0, 1), (2, 1)]
  //   9 -> [(0, 2), (2, 2)]
  //   10 -> [(0, 1), (1, 1), (2, 1)]
  //   11 -> [(0, 2), (1, 2), (2, 2), (3, 2)]
  //   12 -> [(0, 2), (2, 1), (3, 1)]
  //   13 -> [(0, 2), (3, 2)]
  //   14 -> [(0, 2), (2, 2), (3, 2)]
  //   15 -> [(0, 2), (1, 2), (3, 1)]
  //   16 -> [(0, 2), (1, 2), (3, 2)]
  //   17 -> [(0, 1), (2, 2), (3, 2)]
  //   18 -> [(0, 2), (3, 1)]
  //   19 -> [(0, 1), (3, 2)]
  //   20 -> [(0, 1), (1, 1), (3, 2)]
  //   21 -> [(0, 2), (2, 1), (4, 2)]
  //   22 -> [(0, 2), (1, 2), (4, 2)]
  //   23 -> [(0, 2), (1, 2), (3, 2), (4, 2)]
  //   24 -> [(0, 2), (2, 2), (4, 2)]
  //   25 -> [(0, 2), (2, 2), (3, 2), (4, 2)]
  //   26 -> [(0, 2), (3, 2), (4, 2)]
  //   27 -> [(0, 2), (1, 2), (2, 2), (3, 2), (4, 2)]
  //   28 -> [(0, 2), (4, 2)]
  //   29 -> [(0, 2), (1, 2), (2, 2), (4, 2)]
  
  
  public Lev2ParametricDescription(int w) {
    super(w, 2, new int[] {0,2,1,1,0,-1,0,0,-1,0,-1,-1,-2,-1,-1,-2,-1,-1,-2,-1,-1,-2,-2,-2,-2,-2,-2,-2,-2,-2});
  }
}

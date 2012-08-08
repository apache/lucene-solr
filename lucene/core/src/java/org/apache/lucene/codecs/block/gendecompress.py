#!/usr/bin/env python2
"""
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at
  
     http://www.apache.org/licenses/LICENSE-2.0
  
  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
"""

"""
Generate source code for java classes for For or PFor decompression.
"""

def bitsExpr(i, numFrameBits):
  framePos = i * numFrameBits
  intValNum = (framePos / 32)
  bitPos = framePos % 32
  bitsInInt = "intValue" + str(intValNum)
  needBrackets = 0
  if bitPos > 0:
    bitsInInt +=  " >>> " + str(bitPos)
    needBrackets = 1
  if bitPos + numFrameBits > 32:
    if needBrackets:
      bitsInInt = "(" + bitsInInt + ")"
    bitsInInt += " | (intValue" + str(intValNum+1) + " << "+ str(32 - bitPos) + ")"
    needBrackets = 1
  if bitPos + numFrameBits != 32:
    if needBrackets:
      bitsInInt = "(" + bitsInInt + ")"
    bitsInInt += " & mask"
  return bitsInInt


def genDecompress():
  className = "PackedIntsDecompress"
  fileName = className + ".java"
  imports = "import java.nio.IntBuffer;\n"
  f = open(fileName, 'w')
  w = f.write
  try:
    w("package org.apache.lucene.codecs.block;\n")
    w("""/*
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
 """)

    w("\n/* This code is generated, do not modify. See gendecompress.py */\n\n")

    w("import java.nio.IntBuffer;\n")
    w("import java.util.Arrays;\n\n")

    w("final class PackedIntsDecompress {\n")

    w('\n  // nocommit: assess perf of this to see if specializing is really needed\n')
    w('\n  // NOTE: hardwired to blockSize == 128\n\n')

    w('  public static void decode0(final IntBuffer compressedBuffer, final int[] output) {\n')
    w('    Arrays.fill(output, compressedBuffer.get());\n')
    w('  }\n')

    for numFrameBits in xrange(1, 32):
      w('  public static void decode%d(final IntBuffer compressedBuffer, final int[] output) {\n' % numFrameBits)
      w('    final int numFrameBits = %d;\n' % numFrameBits)
      w('    final int mask = (int) ((1L<<numFrameBits) - 1);\n')
      w('    int outputOffset = 0;\n')
      w('    for(int step=0;step<4;step++) {\n')

      for i in range(numFrameBits): # declare int vars and init from buffer
        w("      int intValue" + str(i) + " = compressedBuffer.get();\n")

      for i in range(32): # set output from int vars
        w("      output[" + str(i) + " + outputOffset] = " + bitsExpr(i, numFrameBits) + ";\n")

      w('      outputOffset += 32;\n')
      w('    }\n')
      w('  }\n')

    w('}\n')
      
  finally:
    f.close()

if __name__ == "__main__":
  genDecompress()

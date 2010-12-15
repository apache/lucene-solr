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
Generate source code for java classes for FOR decompression.
"""

USE_SCRATCH = False

def bitsExpr(i, numFrameBits):
  framePos = i * numFrameBits
  intValNum = (framePos / 32)
  bitPos = framePos % 32
  if USE_SCRATCH:
    bitsInInt = "inputInts[" + str(intValNum) + "]"
  else:
    bitsInInt = "intValue" + str(intValNum)
  needBrackets = 0
  if bitPos > 0:
    bitsInInt +=  " >>> " + str(bitPos)
    needBrackets = 1
  if bitPos + numFrameBits > 32:
    if needBrackets:
      bitsInInt = "(" + bitsInInt + ")"
    if USE_SCRATCH:
      bitsInInt += " | (inputInts[" + str(intValNum+1) + "] << "+ str(32 - bitPos) + ")"
    else:
      bitsInInt += " | (intValue" + str(intValNum+1) + " << "+ str(32 - bitPos) + ")"
    needBrackets = 1
  if bitPos + numFrameBits != 32:
    if needBrackets:
      bitsInInt = "(" + bitsInInt + ")"
    bitsInInt += " & mask"
  return bitsInInt

decodeAnyMethod = '''  static void decodeAnyFrame(
        final IntBuffer compressedBuffer, int inputSize, int numFrameBits,
        int[] output) {

    assert numFrameBits > 0 : numFrameBits;
    assert numFrameBits <= 31 : numFrameBits;
    final int mask = (int) ((1L<<numFrameBits) - 1);
    int intValue1 = compressedBuffer.get();
    output[outputOffset] = intValue1 & mask;
    if (--inputSize == 0) return;
    int bitPos = numFrameBits;

    do {
      while (bitPos <= (32 - numFrameBits)) {
        // No mask needed when bitPos == (32 - numFrameBits), but prefer to avoid testing for this:
        output[++outputOffset] = (intValue1 >>> bitPos) & mask;
        if (--inputSize == 0) return;
        bitPos += numFrameBits;
      }
      
      int intValue2 = compressedBuffer.get();
      output[++outputOffset] = ( (bitPos == 32)
                                  ? intValue2
                                  : ((intValue1 >>> bitPos) | (intValue2 << (32 - bitPos)))
                               ) & mask;
        
      if (--inputSize == 0) return;
      
      intValue1 = intValue2;
      bitPos += numFrameBits - 32;
    } while (true);
  }
'''

def genDecompress():
  className = "ForDecompress"
  fileName = className + ".java"
  imports = "import java.nio.IntBuffer;\n"
  f = open(fileName, 'w')
  w = f.write
  try:
    w("package org.apache.lucene.util.pfor;\n")
    w("""/**
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

    w("/* This code is generated, do not modify. See gendecompress.py */\n\n")

    w("import java.nio.IntBuffer;\n\n")

    w("final class ForDecompress {\n")

    w('\n  // nocommit: assess perf of this to see if specializing is really needed\n')

    w('  /*\n')
    w(decodeAnyMethod)
    w('  */\n\n')

    # We don't need the 32 bit case -- unused since java ints are
    # signed and we only use the unsigned range (31 bits):
    for numFrameBits in xrange(1, 32):

      w('\n  // NOTE: hardwired to blockSize == 128\n')
      if USE_SCRATCH:
        w('  public static void decode%d(final IntBuffer compressedBuffer, final int[] output, final int[] scratch) {\n' % numFrameBits)
      else:
        w('  public static void decode%d(final IntBuffer compressedBuffer, final int[] output) {\n' % numFrameBits)

      w('    final int numFrameBits = %d;\n' % numFrameBits)
      w('    final int mask = (int) ((1L<<numFrameBits) - 1);\n')
      w('    int outputOffset = 0;\n')
      
      w('    for(int step=0;step<4;step++) {\n')

      if USE_SCRATCH:
        w('      compressedBuffer.get(scratch, 0, %d);\n' % numFrameBits)
      else:
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

def genSwitch():
  for numFrameBits in xrange(1, 32):
    print '      case %d: ForDecompress.decode%d(compressedBuffer, unCompressedData); break;' % (numFrameBits, numFrameBits)

if __name__ == "__main__":
  genDecompress()
  #genSwitch()

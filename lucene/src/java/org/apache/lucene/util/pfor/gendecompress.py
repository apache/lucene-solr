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


def genDecompressClass(numFrameBits):
  className = "For" + str(numFrameBits) + "Decompress"
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
 */""")
    w("\n/* This program is generated, do not modify. See gendecompress.py */\n\n")
    w("import java.nio.IntBuffer;\n")
    w("class " + className + " extends ForDecompress {\n")
    w("  static final int numFrameBits = " + str(numFrameBits) + ";\n")
    w("  static final int mask = (int) ((1L<<numFrameBits) - 1);\n")
    w("\n")
    w("""  static void decompressFrame(FrameOfRef frameOfRef) {
    int[] output = frameOfRef.unCompressedData;
    IntBuffer compressedBuffer = frameOfRef.compressedBuffer;
    int outputOffset = frameOfRef.offset;
    //int inputSize = frameOfRef.unComprSize;\n""")
    if USE_SCRATCH:
      w('    final int[] inputInts = frameOfRef.scratch;\n')
    #w("    while (inputSize >= 32) {\n")
    w('    for(int step=0;step<4;step++) {\n')
    if USE_SCRATCH:
      w('      compressedBuffer.get(inputInts, 0, %d);\n' % numFrameBits)
    else:
      for i in range(numFrameBits): # declare int vars and init from buffer
        w("      int intValue" + str(i) + " = compressedBuffer.get();\n")

    for i in range(32): # set output from int vars
      w("      output[" + str(i) + " + outputOffset] = " + bitsExpr(i, numFrameBits) + ";\n")
    w("""      // inputSize -= 32;
      outputOffset += 32;
    }
    
    //if (inputSize > 0) {
    //  decodeAnyFrame(compressedBuffer, bufIndex, inputSize, numFrameBits, output, outputOffset);
    //}
  }
}
""")
  finally: f.close()
  
  

def genDecompressClasses():
  numFrameBits = 1
  while numFrameBits <= 31: # 32 special case, not generated.
    genDecompressClass(numFrameBits)
    numFrameBits += 1



if __name__ == "__main__":
  genDecompressClasses()

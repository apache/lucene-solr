// This file was created by `gcjh -stubs'. -*- c++ -*-
//
// This file is intended to give you a head start on implementing native
// methods using CNI.
// Be aware: running `gcjh -stubs ' once more for this class may
// overwrite any edits you have made to this file.

#include <org/apache/lucene/index/GCJTermDocs.h>
#include <org/apache/lucene/store/GCJIndexInput.h>
#include <org/apache/lucene/store/IndexInput.h>
#include <org/apache/lucene/util/BitVector.h>

#include <gcj/cni.h>
#include <gcj/array.h>

using namespace ::std;
using namespace ::java::io;
using namespace ::gnu::gcj;
using namespace ::org::apache::lucene::index;
using namespace ::org::apache::lucene::store;

#define RAW(X) reinterpret_cast< RawData*>(X)
#define BYTES(X) reinterpret_cast< jbyte *>(X)

inline jint readVInt(jbyte*& bytes) {
  jbyte b = *(bytes++);
  jint i = b & 0x7F;
  for (int shift = 7; (b & 0x80) != 0; shift += 7) {
    b = *(bytes++);
    i |= (b & 0x7F) << shift;
  }
  return i;
}

jint GCJTermDocs::read(jintArray docs, jintArray freqs) {
  jbyte* input = BYTES(((GCJIndexInput*)freqStream)->pointer);
  jint length = docs->length;
  jint i = 0;
  while (i < length && count < df) {
    unsigned int docCode = readVInt(input);
    doc__ += docCode >> 1;			  // shift off low bit
    if ((docCode & 1) != 0)			  // if low bit is set
      freq__ = 1;                                 // freq is one
    else
      freq__ = readVInt(input);                   // else read freq
    count++;

    if (deletedDocs == NULL || !deletedDocs->get(doc__)) {
      elements(docs)[i] = doc__;
      elements(freqs)[i] = freq__;
      ++i;
    }
  }
  ((GCJIndexInput*)freqStream)->pointer = RAW(input);
  return i;
}

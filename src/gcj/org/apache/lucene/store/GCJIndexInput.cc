#include <org/apache/lucene/store/GCJIndexInput.h>
#include <gnu/gcj/RawData.h>
#include <java/io/IOException.h>
#include <gcj/cni.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <errno.h>
#include <unistd.h>
#include <iostream>

using namespace ::std;
using namespace ::java::io;
using namespace ::gnu::gcj;
using namespace ::org::apache::lucene::store;

#define RAW(X) reinterpret_cast< RawData*>(X)
#define BYTES(X) reinterpret_cast< jbyte *>(X)

void GCJIndexInput::open() {

  // convert the Java String file name to a char*
  char *buf = (char *) __builtin_alloca (JvGetStringUTFLength (file) + 1);
  jsize total = JvGetStringUTFRegion (file, 0, file->length(), buf);
  buf[total] = '\0';

  // open the file
  fd = ::open (buf, O_RDONLY);
  if (fd < 0)
    throw new IOException(JvNewStringLatin1(strerror(errno)));

  // stat it
  struct stat sb;
  if (::fstat (fd, &sb))
    throw new IOException(JvNewStringLatin1(strerror(errno)));

  // get length from stat
  fileLength = sb.st_size;

  // mmap the file
  // cout << "mmapping " << buf << "\n";
  void* address = ::mmap(0, fileLength, PROT_READ, MAP_SHARED, fd, 0);
  if (address == MAP_FAILED)
    throw new IOException(JvNewStringLatin1(strerror(errno)));

  // initialize pointer to the start of the file
  data = RAW(address);
  pointer = data;
}


jbyte GCJIndexInput::readByte() {

//   if (getFilePointer() >= fileLength)
//     throw new IOException(JvNewStringLatin1("EOF"));

  //return *(BYTES(pointer)++);
  jbyte* bytes = BYTES(pointer);
  jbyte byte = *(bytes++);
  pointer = RAW(bytes);
  return byte;
}


void GCJIndexInput::readBytes(jbyteArray buffer, jint start, jint length) {
  memcpy(elements(buffer)+start, pointer, length);
  
  // BYTES(pointer) += length;
  jbyte* bytes = BYTES(pointer);
  bytes += length;
  pointer = RAW(bytes);
}

jint GCJIndexInput::readVInt() {

//   if (getFilePointer() >= fileLength)
//     throw new IOException(JvNewStringLatin1("EOF"));

  jbyte* bytes = BYTES(pointer);
  jbyte b = *(bytes++);
  jint i = b & 0x7F;
  for (int shift = 7; (b & 0x80) != 0; shift += 7) {
    b = *(bytes++);
    i |= (b & 0x7F) << shift;
  }
  pointer = RAW(bytes);
  return i;
}

void GCJIndexInput::doClose() {
  ::munmap(data, fileLength);
  ::close(fd);
}


jlong GCJIndexInput::getFilePointer() {
  return BYTES(pointer) - BYTES(data);
}


void GCJIndexInput::seek(jlong offset) {
  pointer = RAW(BYTES(data) + offset);
}

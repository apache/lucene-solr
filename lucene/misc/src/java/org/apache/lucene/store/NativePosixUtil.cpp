/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

#ifdef LINUX
  #define DIRECT_FLAG O_DIRECT | O_NOATIME
  #define LINUX
#elif __APPLE__
  #define DIRECT_FLAG 0
  #define OSX
#else
  #define DIRECT_FLAG O_DIRECT  // __unix__ is not used as even Linux falls under it.
#endif

#include <jni.h>
#include <fcntl.h>   // posix_fadvise, constants for open
#include <string.h>   // strerror
#include <errno.h>   // errno
#include <unistd.h>   // pread
#include <sys/mman.h>   // posix_madvise, madvise
#include <sys/types.h>  // constants for open
#include <sys/stat.h>  // constants for open

// java -cp .:lib/junit-4.10.jar:./build/classes/test:./build/classes/java:./build/classes/demo -Dlucene.version=2.9-dev -DtempDir=build -ea org.junit.runner.JUnitCore org.apache.lucene.index.TestDoc

#ifdef LINUX
/*
 * Class:     org_apache_lucene_store_NativePosixUtil
 * Method:    posix_fadvise
 * Signature: (Ljava/io/FileDescriptor;JJI)V
 */
extern "C"
JNIEXPORT jint JNICALL Java_org_apache_lucene_store_NativePosixUtil_posix_1fadvise(JNIEnv *env, jclass _ignore, jobject fileDescriptor, jlong offset, jlong len, jint advice)
{
  jfieldID field_fd;
  jmethodID const_fdesc;

  jclass ioex = env->FindClass("java/io/IOException");
  if (ioex == NULL) {
    return -1;
  }

  jclass fdesc = env->FindClass("java/io/FileDescriptor");
  if (fdesc == NULL) {
    return -2;
  }

  // read the int fd field
  jfieldID fdField = env->GetFieldID(fdesc, "fd", "I");
  if (fdField == NULL) {
    return -3;
  }

  int fd = env->GetIntField(fileDescriptor, fdField);
  //printf("fd=%d\n", fd);  fflush(stdout);

  int osAdvice;
  switch(advice) {

  case 0:
    osAdvice = POSIX_FADV_NORMAL;
    break;
  case 1:
    osAdvice = POSIX_FADV_SEQUENTIAL;
    break;
  case 2:
    osAdvice = POSIX_FADV_RANDOM;
    break;
  case 3:
    osAdvice = POSIX_FADV_WILLNEED;
    break;
  case 4:
    osAdvice = POSIX_FADV_DONTNEED;
    break;
  case 5:
    osAdvice = POSIX_FADV_NOREUSE;
    break;
  }

  int result = posix_fadvise(fd, (off_t) offset, (off_t) len, osAdvice);
  if (result == 0) {
    // ok
  } else {
    env->ThrowNew(ioex, strerror(errno));
    return -1;
  }

  return 0;
}
#endif

/*
 * Class:     org_apache_lucene_store_NativePosixUtil
 * Method:    open_direct
 * Signature: (Ljava/lang/String;Z)Ljava/io/FileDescriptor;
 */
extern "C"
JNIEXPORT jobject JNICALL Java_org_apache_lucene_store_NativePosixUtil_open_1direct(JNIEnv *env, jclass _ignore, jstring filename, jboolean readOnly)
{
  jfieldID field_fd;
  jmethodID const_fdesc;
  jclass class_fdesc, class_ioex;
  jobject ret;
  int fd;
  char *fname;

  class_ioex = env->FindClass("java/io/IOException");
  if (class_ioex == NULL) {
    return NULL;
  }
  class_fdesc = env->FindClass("java/io/FileDescriptor");
  if (class_fdesc == NULL) {
    return NULL;
  }

  fname = (char *) env->GetStringUTFChars(filename, NULL);

  if (readOnly) {
	fd = open(fname, O_RDONLY | DIRECT_FLAG);
	#ifdef OSX
	  fcntl(fd, F_NOCACHE, 1);
	#endif
  } else {
	fd = open(fname, O_RDWR | O_CREAT | DIRECT_FLAG, 0666);
	#ifdef OSX
	  fcntl(fd, F_NOCACHE, 1);
	#endif
  }

  //printf("open %s -> %d; ro %d\n", fname, fd, readOnly); fflush(stdout);

  env->ReleaseStringUTFChars(filename, fname);

  if (fd < 0) {
    // open returned an error. Throw an IOException with the error string
    env->ThrowNew(class_ioex, strerror(errno));
    return NULL;
  }

  // construct a new FileDescriptor
  const_fdesc = env->GetMethodID(class_fdesc, "<init>", "()V");
  if (const_fdesc == NULL) {
    return NULL;
  }
  ret = env->NewObject(class_fdesc, const_fdesc);

  // poke the "fd" field with the file descriptor
  field_fd = env->GetFieldID(class_fdesc, "fd", "I");
  if (field_fd == NULL) {
    return NULL;
  }
  env->SetIntField(ret, field_fd, fd);

  // and return it
  return ret;
}

/*
 * Class:     org_apache_lucene_store_NativePosixUtil
 * Method:    pread
 * Signature: (Ljava/io/FileDescriptor;JLjava/nio/ByteBuffer;)I
 */
extern "C"
JNIEXPORT jlong JNICALL Java_org_apache_lucene_store_NativePosixUtil_pread(JNIEnv *env, jclass _ignore, jobject jfd, jlong pos, jobject byteBuf)
{
  // get int fd:
  jclass class_fdesc = env->FindClass("java/io/FileDescriptor");
  if (class_fdesc == NULL) {
    return -1;
  }

  jfieldID field_fd = env->GetFieldID(class_fdesc, "fd", "I");
  if (field_fd == NULL) {
    return -1;
  }

  const int fd = env->GetIntField(jfd, field_fd);

  void *p = env->GetDirectBufferAddress(byteBuf);
  if (p == NULL) {
    return -1;
  }

  size_t size = (size_t) env->GetDirectBufferCapacity(byteBuf);
  if (size <= 0) {
    return -1;
  }

  size_t numBytesRead = pread(fd, p, (size_t) size, (off_t) pos);
  if (numBytesRead == -1) {
    jclass class_ioex = env->FindClass("java/io/IOException");
    if (class_ioex == NULL) {
      return -1;
    }

    env->ThrowNew(class_ioex, strerror(errno));
    return -1;
  }

  return (jlong) numBytesRead;
}

/*
 * Class:     org_apache_lucene_store_NativePosixUtil
 * Method:    posix_madvise
 * Signature: (Ljava/nio/ByteBuffer;I)I
 */
extern "C"
JNIEXPORT jint JNICALL Java_org_apache_lucene_store_NativePosixUtil_posix_1madvise(JNIEnv *env, jclass _ignore, jobject buffer, jint advice) {
  void *p = env->GetDirectBufferAddress(buffer);
  if (p == NULL) {
    return -1;
  }

  size_t size = (size_t) env->GetDirectBufferCapacity(buffer);
  if (size <= 0) {
    return -1;
  }

  int page = getpagesize();

  // round start down to start of page
  long long start = (long long) p;
  start = start & (~(page-1));

  // round end up to start of page
  long long end = start + size;
  end = (end + page-1)&(~(page-1));
  size = (end-start);

  int osAdvice;
  switch(advice) {
  case 0:
    osAdvice = POSIX_MADV_NORMAL;
    break;
  case 1:
    osAdvice = POSIX_MADV_SEQUENTIAL;
    break;
  case 2:
    osAdvice = POSIX_MADV_RANDOM;
    break;
  case 3:
    osAdvice = POSIX_MADV_WILLNEED;
    break;
  case 4:
    osAdvice = POSIX_MADV_DONTNEED;
    break;
  case 5:
    return -1;
    break;
  }

  //printf("DO posix_madvise: %lx %d\n", p, size);fflush(stdout);

  if (posix_madvise((void *) start, size, osAdvice) != 0) {
    jclass class_ioex = env->FindClass("java/io/IOException");
    if (class_ioex == NULL) {
      return -1;
    }

    env->ThrowNew(class_ioex, strerror(errno));
    return -1;
  }
  
  return 0;
}


/*
 * Class:     org_apache_lucene_store_NativePosixUtil
 * Method:    madvise
 * Signature: (Ljava/nio/ByteBuffer;I)I
 */
extern "C"
JNIEXPORT jint JNICALL Java_org_apache_lucene_store_NativePosixUtil_madvise(JNIEnv *env, jclass _ignore, jobject buffer, jint advice) {
  void *p = env->GetDirectBufferAddress(buffer);
  if (p == NULL) {
    return -1;
  }

  size_t size = (size_t) env->GetDirectBufferCapacity(buffer);
  if (size <= 0) {
    return -1;
  }

  int page = getpagesize();

  // round start down to start of page
  long long start = (long long) p;
  start = start & (~(page-1));

  // round end up to start of page
  long long end = start + size;
  end = (end + page-1)&(~(page-1));
  size = (end-start);

  int osAdvice;
  switch(advice) {
  case 0:
    osAdvice = MADV_NORMAL;
    break;
  case 1:
    osAdvice = MADV_SEQUENTIAL;
    break;
  case 2:
    osAdvice = MADV_RANDOM;
    break;
  case 3:
    osAdvice = MADV_WILLNEED;
    break;
  case 4:
    osAdvice = MADV_DONTNEED;
    break;
  case 5:
    return -1;
    break;
  }


  //printf("DO madvise: page=%d p=0x%lx 0x%lx size=0x%lx\n", page, p, start, size);fflush(stdout);

  if (madvise((void *) start, size, osAdvice) != 0) {
    jclass class_ioex = env->FindClass("java/io/IOException");
    if (class_ioex == NULL) {
      return -1;
    }

    env->ThrowNew(class_ioex, strerror(errno));
    return -1;
  }
  
  return 0;
}

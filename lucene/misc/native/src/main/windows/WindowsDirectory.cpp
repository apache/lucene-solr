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
 
#include <jni.h>
#include "windows.h"

/**
 * Windows Native IO methods.
 */
extern "C" {

/**
 * Utility to format a Windows system error code into an exception.
 */
void throwIOException(JNIEnv *env, DWORD error) 
{
  jclass ioex;
  char *msg;
  
  ioex = env->FindClass("java/io/IOException");
  
  if (ioex != NULL) {
    FormatMessage(FORMAT_MESSAGE_ALLOCATE_BUFFER | FORMAT_MESSAGE_FROM_SYSTEM | FORMAT_MESSAGE_IGNORE_INSERTS,
                  NULL, error, MAKELANGID(LANG_NEUTRAL, SUBLANG_DEFAULT), (LPTSTR) &msg, 0, NULL );
    env->ThrowNew(ioex, msg);
    LocalFree(msg);
  }
}

/**
 * Utility to throw Exceptions on bad input
 */
void throwException(JNIEnv *env, const char *clazz, const char *msg) 
{
  jclass exc = env->FindClass(clazz);
  
  if (exc != NULL) {
    env->ThrowNew(exc, msg);
  }
}

/**
 * Opens a handle to a file.
 *
 * Class:     org_apache_lucene_misc_store_WindowsDirectory
 * Method:    open
 * Signature: (Ljava/lang/String;)J
 */
JNIEXPORT jlong JNICALL Java_org_apache_lucene_misc_store_WindowsDirectory_open
  (JNIEnv *env, jclass ignored, jstring filename) 
{
  char *fname;
  HANDLE handle;
  
  if (filename == NULL) {
    throwException(env, "java/lang/NullPointerException", "filename must not be null");
    return -1;
  }
  
  fname = (char *) env->GetStringUTFChars(filename, NULL);
  
  if (fname == NULL) {
    throwException(env, "java/lang/IllegalArgumentException", "invalid filename");
    return -1;
  }
  
  handle = CreateFile(fname, GENERIC_READ, FILE_SHARE_READ | FILE_SHARE_WRITE, 
                      NULL, OPEN_EXISTING, FILE_FLAG_RANDOM_ACCESS, NULL);
  
  env->ReleaseStringUTFChars(filename, fname);
  
  if (handle == INVALID_HANDLE_VALUE) {
    throwIOException(env, GetLastError());
    return -1;
  }

  return (jlong) handle;
}

/** 
 * Reads data into the byte array, starting at offset, for length characters.
 * The read is positioned at pos.
 * 
 * Class:     org_apache_lucene_misc_store_WindowsDirectory
 * Method:    read
 * Signature: (J[BIIJ)I
 */
JNIEXPORT jint JNICALL Java_org_apache_lucene_misc_store_WindowsDirectory_read
  (JNIEnv *env, jclass ignored, jlong fd, jbyteArray bytes, jint offset, jint length, jlong pos)
{
  OVERLAPPED io = { 0 };
  DWORD numRead = -1;
  
  io.Offset = (DWORD) (pos & 0xFFFFFFFF);
  io.OffsetHigh = (DWORD) ((pos >> 0x20) & 0x7FFFFFFF);
  
  if (bytes == NULL) {
    throwException(env, "java/lang/NullPointerException", "bytes must not be null");
    return -1;
  }
  
  if (length <= 4096) {  /* For small buffers, avoid GetByteArrayElements' copy */
    char buffer[length];
  	
    if (ReadFile((HANDLE) fd, &buffer, length, &numRead, &io)) {
      env->SetByteArrayRegion(bytes, offset, numRead, (const jbyte *) buffer);
    } else {
      throwIOException(env, GetLastError());
      numRead = -1;
    }
  	
  } else {
    jbyte *buffer = env->GetByteArrayElements (bytes, NULL);
  
    if (!ReadFile((HANDLE) fd, (void *)(buffer+offset), length, &numRead, &io)) {
      throwIOException(env, GetLastError());
      numRead = -1;
    }
  	
    env->ReleaseByteArrayElements(bytes, buffer, numRead == 0 || numRead == -1 ? JNI_ABORT : 0);
  }
  
  return numRead;
}

/**
 * Closes a handle to a file
 *
 * Class:     org_apache_lucene_misc_store_WindowsDirectory
 * Method:    close
 * Signature: (J)V
 */
JNIEXPORT void JNICALL Java_org_apache_lucene_misc_store_WindowsDirectory_close
  (JNIEnv *env, jclass ignored, jlong fd) 
{
  if (!CloseHandle((HANDLE) fd)) {
    throwIOException(env, GetLastError());
  }
}

/**
 * Returns the length in bytes of a file.
 *
 * Class:     org_apache_lucene_misc_store_WindowsDirectory
 * Method:    length
 * Signature: (J)J
 */
JNIEXPORT jlong JNICALL Java_org_apache_lucene_misc_store_WindowsDirectory_length
  (JNIEnv *env, jclass ignored, jlong fd)
{
  BY_HANDLE_FILE_INFORMATION info;
    	
  if (GetFileInformationByHandle((HANDLE) fd, (LPBY_HANDLE_FILE_INFORMATION) &info)) {
    return (jlong) (((DWORDLONG) info.nFileSizeHigh << 0x20) + info.nFileSizeLow);
  } else {
    throwIOException(env, GetLastError());
    return -1;
  }
}

} /* extern "C" */

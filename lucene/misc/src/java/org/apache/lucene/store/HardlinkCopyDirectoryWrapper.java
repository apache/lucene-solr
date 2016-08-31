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

package org.apache.lucene.store;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.security.AccessController;
import java.security.PrivilegedAction;

/**
 * This directory wrapper overrides {@link Directory#copyFrom(Directory, String, String, IOContext)} in order
 * to optionally use a hard-link instead of a full byte by byte file copy if applicable. Hard-links are only used if the
 * underlying filesystem supports it and if the {@link java.nio.file.LinkPermission} "hard" is granted.
 *
 * <p><b>NOTE:</b> Using hard-links changes the copy semantics of
 * {@link Directory#copyFrom(Directory, String, String, IOContext)}. When hard-links are used changes to the source file
 * will be reflected in the target file and vice-versa. Within Lucene, files are write once and should not be modified
 * after they have been written. This directory should not be used in situations where files change after they have
 * been written.
 * </p>
 */
public final class HardlinkCopyDirectoryWrapper extends FilterDirectory {
  /**
   * Creates a new HardlinkCopyDirectoryWrapper delegating to the given directory
   */
  public HardlinkCopyDirectoryWrapper(Directory in) {
    super(in);
  }

  @Override
  public void copyFrom(Directory from, String srcFile, String destFile, IOContext context) throws IOException {
    final Directory fromUnwrapped = FilterDirectory.unwrap(from);
    final Directory toUnwrapped = FilterDirectory.unwrap(this);
    // try to unwrap to FSDirectory - we might be able to just create hard-links of these files and save copying
    // the entire file.
    Exception suppressedException = null;
    boolean tryCopy = true;
    if (fromUnwrapped instanceof FSDirectory
        && toUnwrapped instanceof FSDirectory) {
      final Path fromPath = ((FSDirectory) fromUnwrapped).getDirectory();
      final Path toPath = ((FSDirectory) toUnwrapped).getDirectory();

      if (Files.isReadable(fromPath.resolve(srcFile)) && Files.isWritable(toPath)) {
        // only try hardlinks if we have permission to access the files
        // if not super.copyFrom() will give us the right exceptions
        suppressedException = AccessController.doPrivileged((PrivilegedAction<Exception>) () -> {
          try {
            Files.createLink(toPath.resolve(destFile), fromPath.resolve(srcFile));
          } catch (FileNotFoundException | NoSuchFileException | FileAlreadyExistsException ex) {
            return ex; // in these cases we bubble up since it's a true error condition.
          } catch (IOException
              | UnsupportedOperationException // if the FS doesn't support hard-links
              | SecurityException ex // we don't have permission to use hard-links just fall back to byte copy
          ) {
            // hard-links are not supported or the files are on different filesystems
            // we could go deeper and check if their filesstores are the same and opt
            // out earlier but for now we just fall back to normal file-copy
            return ex;
          }
          return null;
        });
        tryCopy = suppressedException != null;
      }
    }
    if (tryCopy) {
      try {
        super.copyFrom(from, srcFile, destFile, context);
      } catch (Exception ex) {
        if (suppressedException != null) {
          ex.addSuppressed(suppressedException);
        }
        throw ex;
      }
    }
  }
}

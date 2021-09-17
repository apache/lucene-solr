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
package org.apache.solr.s3;

import static org.apache.solr.s3.S3BackupRepository.S3_SCHEME;

import com.adobe.testing.s3mock.junit4.S3MockRule;
import com.google.common.base.Strings;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import org.apache.commons.io.FileUtils;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.store.BufferedIndexInput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.NIOFSDirectory;
import org.apache.lucene.store.OutputStreamIndexOutput;
import org.apache.solr.cloud.api.collections.AbstractBackupRepositoryTest;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.backup.repository.BackupRepository;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

public class S3BackupRepositoryTest extends AbstractBackupRepositoryTest {

  private static final String BUCKET_NAME = S3BackupRepositoryTest.class.getSimpleName();

  @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @ClassRule
  public static final S3MockRule S3_MOCK_RULE =
      S3MockRule.builder().silent().withInitialBuckets(BUCKET_NAME)
          .withProperty("spring.autoconfigure.exclude", "org.springframework.boot.autoconfigure.solr.SolrAutoConfiguration")
          .withProperty("spring.jmx.enabled", "false")
          .withProperty("server.jetty.threads.idle-timeout", "3s")
          .build();

  @BeforeClass
  public static void setupProperties() {
    System.setProperty("aws.accessKeyId", "foo");
    System.setProperty("aws.secretKey", "bar");
  }

  /**
   * Sent by {@link org.apache.solr.handler.ReplicationHandler}, ensure we don't choke on the bare
   * URI.
   */
  @Test
  public void testURI() throws IOException {
    try (S3BackupRepository repo = getRepository()) {
      URI uri = repo.createURI("x");
      assertEquals(
          "'S3' scheme should be auto-added to the URI when not provided",
          S3_SCHEME,
          uri.getScheme());
      assertEquals("URI path should be prefixed with /", "/x", uri.getPath());
      assertEquals("s3:///x", uri.toString());

      URI directoryUri = repo.createDirectoryURI("d");
      assertEquals(
          "'S3' scheme should be auto-added to the dir URI when not provided",
          S3_SCHEME,
          directoryUri.getScheme());
      assertEquals(
          "createDirectoryURI should add a trailing slash to URI",
          "s3:///d/",
          directoryUri.toString());

      repo.createDirectory(directoryUri);
      assertTrue(repo.exists(directoryUri));
      directoryUri = repo.createDirectoryURI("d/");
      assertEquals(
          "createDirectoryURI should have a single trailing slash, even if one is provided",
          "s3:///d/",
          directoryUri.toString());

      assertEquals(
          "createDirectoryURI should have a single trailing slash, even if one is provided",
          "s3:///this_is_not_a_host/",
          repo.createURI("/this_is_not_a_host/").toString());
    }
  }

  @Test
  public void testLocalDirectoryFunctions() throws Exception {
    try (S3BackupRepository repo = getRepository()) {

      URI path = new URI("/test");
      repo.createDirectory(path);
      assertTrue(repo.exists(path));
      assertEquals(BackupRepository.PathType.DIRECTORY, repo.getPathType(path));
      assertEquals("No files should exist in dir yet", repo.listAll(path).length, 0);

      URI subDir = new URI("/test/dir");
      repo.createDirectory(subDir);
      assertTrue(repo.exists(subDir));
      assertEquals(BackupRepository.PathType.DIRECTORY, repo.getPathType(subDir));
      assertEquals("No files should exist in subdir yet", repo.listAll(subDir).length, 0);

      assertEquals(
          "subDir should now be returned when listing all in parent dir",
          repo.listAll(path).length,
          1);

      repo.deleteDirectory(path);
      assertFalse(repo.exists(path));
      assertFalse(repo.exists(subDir));
    }
  }

  /** Check resolving paths. */
  @Test
  public void testResolve() throws Exception {
    try (S3BackupRepository repo = getRepository()) {
      // Add single element to root
      assertEquals(new URI("s3:/root/path"), repo.resolve(new URI("s3:/root"), "path"));

      // Root ends with '/'
      assertEquals(new URI("s3://root/path"), repo.resolve(new URI("s3://root/"), "path"));
      assertEquals(new URI("s3://root/path"), repo.resolve(new URI("s3://root///"), "path"));

      // Add to a sub-element
      assertEquals(
          new URI("s3://root/path1/path2"), repo.resolve(new URI("s3://root/path1"), "path2"));

      // Add two elements to root
      assertEquals(
          new URI("s3://root/path1/path2"), repo.resolve(new URI("s3://root"), "path1", "path2"));

      // Add compound elements
      assertEquals(
          new URI("s3:/root/path1/path2/path3"),
          repo.resolve(new URI("s3:/root"), "path1/path2", "path3"));

      // Check URIs with an authority
      assertEquals(new URI("s3://auth/path"), repo.resolve(new URI("s3://auth"), "path"));
      assertEquals(
          new URI("s3://auth/path1/path2"), repo.resolve(new URI("s3://auth/path1"), "path2"));
    }
  }

  /** Check - pushing a file to the repo (backup). - pulling a file from the repo (restore). */
  @Test
  public void testCopyFiles() throws Exception {

    // basic test with a small file
    String content = "Test to push a backup";
    doTestCopyFileFrom(content);
    doTestCopyFileTo(content);

    // copy a 10Mb file
    content += Strings.repeat("1234567890", 1024 * 1024);
    doTestCopyFileFrom(content);
    doTestCopyFileTo(content);
  }

  /** Check copying a file to the repo (backup). Specified content is used for the file. */
  private void doTestCopyFileFrom(String content) throws Exception {
    try (S3BackupRepository repo = getRepository()) {

      // A file on the local disk
      File tmp = temporaryFolder.newFolder();
      try (OutputStream os = FileUtils.openOutputStream(new File(tmp, "from-file"));
          IndexOutput indexOutput = new OutputStreamIndexOutput("", "", os, content.length())) {
        byte[] bytes = content.getBytes(StandardCharsets.UTF_8);
        indexOutput.writeBytes(bytes, bytes.length);

        CodecUtil.writeFooter(indexOutput);
      }

      Directory sourceDir = new NIOFSDirectory(tmp.toPath());
      repo.copyIndexFileFrom(sourceDir, "from-file", new URI("s3://to-folder"), "to-file");

      // Sanity check: we do have different files
      File actualSource = new File(tmp, "from-file");
      File actualDest = pullObject("to-folder/to-file");
      assertNotEquals(actualSource, actualDest);

      // Check the copied content
      assertTrue(actualDest.isFile());
      assertTrue(FileUtils.contentEquals(actualSource, actualDest));
    }
  }

  /** Check retrieving a file from the repo (restore). Specified content is used for the file. */
  private void doTestCopyFileTo(String content) throws Exception {

    try (S3BackupRepository repo = getRepository()) {

      // Local folder for destination
      File tmp = temporaryFolder.newFolder();
      Directory destDir = new NIOFSDirectory(tmp.toPath());

      // Directly create a file on S3
      pushObject("from-file", content);

      repo.copyIndexFileTo(new URI("s3:///"), "from-file", destDir, "to-file");

      // Sanity check: we do have different files
      File actualSource = pullObject("from-file");
      File actualDest = new File(tmp, "to-file");
      assertNotEquals(actualSource, actualDest);

      // Check the copied content
      assertTrue(actualDest.isFile());
      assertTrue(FileUtils.contentEquals(actualSource, actualDest));
    }
  }

  /** Check reading input with random access stream. */
  @Test
  public void testRandomAccessInput() throws Exception {

    // Test with a short text that fills in the buffer
    String content = "This is the content of my blob";
    doRandomAccessTest(content, content.indexOf("content"));

    // Large text, we force to refill the buffer
    String blank = Strings.repeat(" ", 5 * BufferedIndexInput.BUFFER_SIZE);
    content = "This is a super large" + blank + "content";
    doRandomAccessTest(content, content.indexOf("content"));
  }

  /**
   * Check implementation of {@link S3BackupRepository#openInput(URI, String, IOContext)}. Open an
   * index input and seek to an absolute position.
   *
   * <p>We use specified text. It must has the word "content" at given position.
   */
  private void doRandomAccessTest(String content, int position) throws Exception {

    try (S3BackupRepository repo = getRepository()) {
      File tmp = temporaryFolder.newFolder();

      // Open an index input on a file
      pushObject("/my-repo/content", content);
      IndexInput input = repo.openInput(new URI("s3://my-repo"), "content", IOContext.DEFAULT);

      byte[] buffer = new byte[100];

      // Read 4 bytes
      input.readBytes(buffer, 0, 4);
      assertEquals(
          "Reading from beginning of buffer should return 'This'",
          "This",
          new String(buffer, 0, 4, StandardCharsets.UTF_8));

      // Seek to the work 'content' and read it
      input.seek(position);
      input.readBytes(buffer, 0, 7);
      assertEquals(
          "Seeking to pos " + position + " in buffer should return 'content'",
          "content",
          new String(buffer, 0, 7, StandardCharsets.UTF_8));
    }
  }

  /** Check we gracefully fail when seeking before current position of the stream. */
  @Test
  public void testBackwardRandomAccess() throws Exception {

    try (S3BackupRepository repo = getRepository()) {

      // Open an index input on a file
      String blank = Strings.repeat(" ", 5 * BufferedIndexInput.BUFFER_SIZE);
      String content = "This is the file " + blank + "content";

      pushObject("/content", content);
      IndexInput input = repo.openInput(new URI("s3:///"), "content", IOContext.DEFAULT);

      // Read twice the size of the internal buffer, so first bytes are not in the buffer anymore
      byte[] buffer = new byte[BufferedIndexInput.BUFFER_SIZE * 2];
      input.readBytes(buffer, 0, BufferedIndexInput.BUFFER_SIZE * 2);

      // Seek back to the 5th byte.
      // It is not any more in the internal buffer, so we should fail
      IOException exception = assertThrows(IOException.class, () -> input.seek(5));
      assertEquals("Cannot seek backward", exception.getMessage());
    }
  }

  @Override
  protected S3BackupRepository getRepository() {
    System.setProperty("aws.accessKeyId", "foo");
    System.setProperty("aws.secretAccessKey", "bar");

    NamedList<Object> args = getBaseBackupRepositoryConfiguration();

    S3BackupRepository repo = new S3BackupRepository();
    repo.init(args);

    return repo;
  }

  @Override
  protected URI getBaseUri() throws URISyntaxException {
    return new URI("s3:/");
  }

  @Override
  protected NamedList<Object> getBaseBackupRepositoryConfiguration() {
    NamedList<Object> args = new NamedList<>();
    args.add(S3BackupRepositoryConfig.REGION, Region.US_EAST_1.id());
    args.add(S3BackupRepositoryConfig.BUCKET_NAME, BUCKET_NAME);
    args.add(S3BackupRepositoryConfig.ENDPOINT, "http://localhost:" + S3_MOCK_RULE.getHttpPort());
    return args;
  }

  private void pushObject(String path, String content) {
    try (S3Client s3 = S3_MOCK_RULE.createS3ClientV2()) {
      s3.putObject(b -> b.bucket(BUCKET_NAME).key(path), RequestBody.fromString(content));
    }
  }

  private File pullObject(String path) throws IOException {
    try (S3Client s3 = S3_MOCK_RULE.createS3ClientV2()) {
      File file = temporaryFolder.newFile();
      InputStream input = s3.getObject(b -> b.bucket(BUCKET_NAME).key(path));
      FileUtils.copyInputStreamToFile(input, file);
      return file;
    }
  }
}

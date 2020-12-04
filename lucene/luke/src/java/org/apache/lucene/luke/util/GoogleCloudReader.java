 package org.apache.lucene.luke.util;
 import com.google.api.gax.paging.Page;
 import com.google.cloud.ReadChannel;
 import com.google.cloud.storage.Blob;
 import com.google.cloud.storage.Bucket;
 import com.google.cloud.storage.Storage;
 import com.google.cloud.storage.StorageOptions;

 import java.io.File;
 import java.io.FileOutputStream;
 import java.io.IOException;
 import java.nio.file.Path;
 import java.nio.file.Paths;
 import java.util.Arrays;
 import java.util.List;
 import java.util.stream.Collectors;

 public class GoogleCloudReader {
   final static String PROJECT_ID = "shopify-cloud-es";
   final static String BUCKET_NAME = "elasticsearch-hq-wang";

     public static String readFromGoogleCloud(String uri) throws IOException {

         String[] folderStructure = uri.substring(uri.indexOf(BUCKET_NAME) + BUCKET_NAME.length()).split("/");

         StorageOptions options = StorageOptions.newBuilder()
                 .setProjectId(PROJECT_ID).build();
         List<String> paths = Arrays.asList(folderStructure);
         paths = paths.stream().filter(x -> x != null && !x.isEmpty()).collect(Collectors.toList());

         Storage storage = options.getService();
         Bucket bucket = storage.get(BUCKET_NAME);
         String prefix = String.join("/", paths);

         String[] structure = uri.substring(uri.indexOf(BUCKET_NAME)).split("/");
         StringBuilder folders = new StringBuilder();

         for (String s: structure){
             folders.append(s);
             folders.append("/");
             File directory = new File(folders.toString());
             directory.mkdir();
         }

         Page<Blob> blobs = bucket.list(Storage.BlobListOption.prefix(prefix));
         for (Blob blob : blobs.iterateAll()) {
             String path = BUCKET_NAME + "/" + blob.getName();
             if (!(new File(path)).isDirectory()){
                 ReadChannel readChannel = blob.reader();
                 FileOutputStream fileOutputStream = new FileOutputStream(path);
                 fileOutputStream.getChannel().transferFrom(readChannel, 0, Long.MAX_VALUE);
                 fileOutputStream.close();
             }
         }
         Path currentPath = Paths.get(System.getProperty("user.dir"));
         Path filePath = Paths.get(currentPath.toString(), folders.toString());
         return filePath.toString();
     }
 }
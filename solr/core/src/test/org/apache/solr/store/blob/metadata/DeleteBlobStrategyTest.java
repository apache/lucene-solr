package org.apache.solr.store.blob.metadata;

import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.solr.core.CoreContainer;
import org.junit.Assert;
import org.junit.Test;

import org.apache.solr.store.blob.client.BlobCoreMetadata;
import org.apache.solr.store.blob.client.BlobCoreMetadataBuilder;

import java.util.HashSet;
import java.util.Set;

/**
 * Unit tests for {@link CorePushPull} in the context of deciding to delete no longer needed blob files.
 * This doesn't test the actual deletes but the application of the appropriate delete strategy.
 *
 * @author iginzburg
 * @since 214/solr.6
 */
public class DeleteBlobStrategyTest {

    /**
     * Recently deleted files should not be removed from the Blob store. Files deleted long ago are removed.
     */
    @Test
    public void doNotRemoveFreshDeletes() throws Exception {

        final MutableInt attemptedDeleteEnqueues = new MutableInt(0);
        final Set<BlobCoreMetadata.BlobFileToDelete> enqueuedBlobsToDelete = new HashSet<>();

        // Create an instance of CorePushPull that considers that deleted file at a timestamp before 10000 should be
        // hard deleted, and those at or after this timestamp should not, and for which deletes always work.
        // Also implement a collector to track which deletes have been called.
        CorePushPull deleteBeforeDate10000 = new CorePushPull(null, null, null , null) {
            @Override
            protected CoreContainer getContainerFromServerMetadata(ServerSideCoreMetadata solrServerMetadata) {
                // Needed so CorePushPull constructor doesn't throw NPE
                return null;
            }

            @Override
            protected boolean okForHardDelete(BlobCoreMetadata.BlobFileToDelete file) {
                return file.getDeletedAt() < 10000L;
            }

            @Override
            protected boolean enqueueForDelete(String coreName, Set<BlobCoreMetadata.BlobFileToDelete> blobFiles) {
                attemptedDeleteEnqueues.increment();
                enqueuedBlobsToDelete.addAll(blobFiles);
                // enqueue always succeeds (assuming physical delete eventually happens, not tested here)
                return true;
            }
        };

        // Now create a metadata builder with a set of files to delete and verify the right ones are deleted.
        BlobCoreMetadataBuilder bcmBuilder = new BlobCoreMetadataBuilder("randomCoreName", 0L,0L);

        // file 1 deleted at 5000 (should be removed)
        BlobCoreMetadata.BlobFileToDelete blobFile1 = new BlobCoreMetadata.BlobFileToDelete("solrFile1", "BlobFile1", 1234L, 5000L);
        // file 2 deleted at 15000 (should NOT be removed)
        BlobCoreMetadata.BlobFileToDelete blobFile2 = new BlobCoreMetadata.BlobFileToDelete("solrFile2", "BlobFile2", 1234L, 15000L);
        // file 3 deleted at 1000 (should be removed)
        BlobCoreMetadata.BlobFileToDelete blobFile3 = new BlobCoreMetadata.BlobFileToDelete("solrFile3", "BlobFile3", 1234L, 1000L);
        // file 4 deleted at 1000000000 (should not be removed)
        BlobCoreMetadata.BlobFileToDelete blobFile4 = new BlobCoreMetadata.BlobFileToDelete("solrFile4", "BlobFile4", 1234L, 1000000000L);
        // file 5 deleted at 1000000000 (should not be removed)
        BlobCoreMetadata.BlobFileToDelete blobFile5 = new BlobCoreMetadata.BlobFileToDelete("solrFile5", "BlobFile5", 1234L, 1000000000L);
        
        bcmBuilder.addFileToDelete(blobFile1);
        bcmBuilder.addFileToDelete(blobFile2);
        bcmBuilder.addFileToDelete(blobFile3);
        bcmBuilder.addFileToDelete(blobFile4);
        bcmBuilder.addFileToDelete(blobFile5);

        // Call the delete code
        deleteBeforeDate10000.enqueueForHardDelete(bcmBuilder);

        // Verify delete enqueue attempted only on the two files to hard delete
        Assert.assertEquals("Expected one delete enqueue attempts", 1, attemptedDeleteEnqueues.intValue());
        Assert.assertEquals("Expected two files enqueued for deleted", 2, enqueuedBlobsToDelete.size());
        Assert.assertTrue("BlobFile1 should have been enqueued for delete", enqueuedBlobsToDelete.contains(blobFile1));
        Assert.assertTrue("BlobFile3 should have been enqueued for delete", enqueuedBlobsToDelete.contains(blobFile3));

        // Verify the blob metadata got updated in the deleted files section.
        BlobCoreMetadata bcm = bcmBuilder.build();

        Set<String> remainingFilesToDelete = new HashSet<>();
        for (BlobCoreMetadata.BlobFileToDelete bftd : bcm.getBlobFilesToDelete()) {
            remainingFilesToDelete.add(bftd.getBlobName());
        }

        Assert.assertEquals("blob core metadata should have only three remaining files to delete", 3, remainingFilesToDelete.size());
        Assert.assertTrue("BlobFile2 should still be present in files to delete", remainingFilesToDelete.contains("BlobFile2"));
        Assert.assertTrue("BlobFile4 should still be present in files to delete", remainingFilesToDelete.contains("BlobFile4"));
        Assert.assertTrue("BlobFile5 should still be present in files to delete", remainingFilesToDelete.contains("BlobFile5"));
    }


    /**
     * When delete enqueue fails, no files are removed.
     */
    @Test
    public void noDeleteWhenEnqueueFails() throws Exception {

        // Create an instance of CorePushPull that considers all deleted files should be hard deleted but have delete enqueue
        // always fail...
        CorePushPull deleteButFailToEnqueue = new CorePushPull(null, null, null , null) {
            @Override
            protected CoreContainer getContainerFromServerMetadata(ServerSideCoreMetadata solrServerMetadata) {
                // Needed so CorePushPull constructor doesn't throw NPE
                return null;
            }

            @Override
            protected boolean okForHardDelete(BlobCoreMetadata.BlobFileToDelete file) {
                // All files should be hard deleted...
                return true;
            }

            @Override
            protected boolean enqueueForDelete(String coreName, Set<BlobCoreMetadata.BlobFileToDelete> blobFiles) {
                // ... but enqueue always fails
                return false;
            }
        };

        // Now create a metadata builder with a set of files to delete
        BlobCoreMetadataBuilder bcmBuilder = new BlobCoreMetadataBuilder("randomCoreName", 0L, 0L);

        bcmBuilder.addFileToDelete(new BlobCoreMetadata.BlobFileToDelete("solrFile1", "BlobFile1", 1234L, 123456L));
        bcmBuilder.addFileToDelete(new BlobCoreMetadata.BlobFileToDelete("solrFile2", "BlobFile2", 1234L, 234567L));
        bcmBuilder.addFileToDelete(new BlobCoreMetadata.BlobFileToDelete("solrFile3", "BlobFile3", 1234L, 987654321L));

        // Call the delete code
        deleteButFailToEnqueue.enqueueForHardDelete(bcmBuilder);

        // Now verify no files have been removed from the delete section of the core metadata since all enqueues failed.
        // (note it happens that we try only one enqueue then give up)
        BlobCoreMetadata bcm = bcmBuilder.build();

        Set<String> remainingFilesToDelete = new HashSet<>();
        for (BlobCoreMetadata.BlobFileToDelete bftd : bcm.getBlobFilesToDelete()) {
            remainingFilesToDelete.add(bftd.getBlobName());
        }

        Assert.assertEquals("blob core metadata should still have its three files to delete", 3, remainingFilesToDelete.size());
    }
}

package org.apache.solr.store.blob.process;

import java.lang.invoke.MethodHandles;

import org.apache.solr.core.CoreContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utils related to blob background process e.g. init/shutdown of blob's background processes
 *
 * @author mwaheed
 * @since 218/solr.7
 */
public class BlobProcessUtil {
  
  private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private volatile CorePullerFeeder runningFeeder = null;
  
  public BlobProcessUtil(CoreContainer coreContainer) {
    // Start the Blob store sync core push and async core pull machinery
    runningFeeder = initializeCorePullerFeeder(coreContainer);
  }
  
  /**
   * Shutdown background blob puller process
   */
  public void shutdown() {
    shutdownCorePullerFeeder();
  }
  
  /**
   * Initializes the CorePullerFeeder and starts running thread
   * @param cores CoreContainer
   * @return CorePullerFeeder 
   */
  private CorePullerFeeder initializeCorePullerFeeder(CoreContainer cores) {
    CorePullerFeeder cpf = new CorePullerFeeder(cores);
    Thread t = new Thread(cpf);
    t.setName("blobPullerFeeder-" + t.getName());
    t.start();
    
    logger.info("CorePullerFeeder initialized : " + t.getName());
    
    return cpf;
  }

  /**
   * Closes down the CorePullerFeeder thread
   */
  private void shutdownCorePullerFeeder() {
    final CoreSyncFeeder rf = runningFeeder;
    runningFeeder = null;
    if (rf != null) {
      rf.close();
    }
  }
}

package org.apache.solr.cluster.events;

/**
 *
 */
public interface ScheduledEventListener extends ClusterEventListener {

  /**
   * Return the schedule that this listener needs. This is used when registering the
   * listener to properly configure the scheduler.
   */
  Schedule getSchedule();
}

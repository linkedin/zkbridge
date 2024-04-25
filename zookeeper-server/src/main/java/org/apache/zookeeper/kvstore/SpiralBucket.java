package org.apache.zookeeper.kvstore;

/**
 * Represents the buckets present within the ZKBridge.
 */
public enum SpiralBucket {

  SHARED_TRANSACTION_LOG ("shared_transaction_log"),
  SESSIONS("sessions"),
  INTERNAL_STATE("internal_state"),
  LAST_PROCESSED_OFFSET("last_processed_offset"),
  SNAPSHOT_STATUS("snapshot_status");


  private final String name;

  private SpiralBucket(String name) {
    this.name = name;
  }

  public String getBucketName() {
    return name;
  }
}

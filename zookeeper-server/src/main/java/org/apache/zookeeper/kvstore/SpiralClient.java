package org.apache.zookeeper.kvstore;

import java.util.List;
import com.linkedin.spiral.PaginationContext;
import proto.com.linkedin.spiral.ScanResponse;


public interface SpiralClient {

  void initialize();

  void createNamespace(String namespace);

  void createBucket(String bucketName);

  boolean containsKey(String bucketName, String key);

  byte[] get(String bucketName, String key);

  byte[] asyncGet(String bucketName, String key);

  void updateLastProcessedTxn(long serverId, long zxid);

  Long generateTransactionId();

  Long getCurrentLatestTransactionId();

  void put(String bucketName, String key, byte[] value);

  ScanResponse scanBucket(String bucketName, PaginationContext paginationContext);

  void delete(String bucketName, String key);

  List<String> listBuckets();
}

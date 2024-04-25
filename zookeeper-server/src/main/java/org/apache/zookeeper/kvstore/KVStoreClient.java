/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.zookeeper.kvstore;

/**
 * Interface for a key-value store client.
 * This KV-store client is used to interact with the KV-store server. 
 */

public interface KVStoreClient<K, V, C> {
  // Get the value associated with the key from the KV-store.
  V get(C context, K key);

  // Put the key-value pair into the KV-store.
  void put(C context, K key, V value);

  // Delete the key-value pair from the KV-store.
  void delete(C context, K key);
}

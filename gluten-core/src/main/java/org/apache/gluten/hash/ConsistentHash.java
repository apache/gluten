/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.gluten.hash;

import com.google.common.base.Preconditions;
import org.apache.commons.codec.digest.MurmurHash3;

import javax.annotation.concurrent.ThreadSafe;

import java.util.*;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * A consistent hash ring implementation. the class is thread-safe.
 *
 * <p>It is mainly used to request across multiple nodes in a distributed system in a more balanced
 * and stable way, minimizing the impact when nodes are added or removed.
 *
 * <p>The ConsistentHash support virtual nodes by replication of the physical nodes, called
 * partition. It allocates the partition to the ring by hashing the partition key to a slot in the
 * ring.
 *
 * @param <T> the type of node to be used in the ring.
 */
@ThreadSafe
public class ConsistentHash<T extends ConsistentHash.Node> {

  // read-write lock for the ring.
  private final ReadWriteLock lock = new ReentrantReadWriteLock(true);

  // keep the mapping of node to its partitions, each partition is a slot in the ring.
  private final Map<T, Set<Partition<T>>> nodes = new HashMap<>();

  // keep the mapping of slot to partition, the partition actually is a virtual node.
  private final SortedMap<Long, Partition<T>> ring = new TreeMap<>();

  // the number of virtual nodes for each physical node.
  private final int replicate;

  // the hasher function to hash the key.
  private final Hasher hasher;

  public ConsistentHash(int replicate) {
    Preconditions.checkArgument(replicate > 0, "HashRing require positive replicate number.");
    this.replicate = replicate;
    this.hasher =
        (key, seed) -> {
          byte[] data = key.getBytes();
          return MurmurHash3.hash32x86(data, 0, data.length, seed);
        };
  }

  public ConsistentHash(int replicate, Hasher hasher) {
    Preconditions.checkArgument(replicate > 0, "HashRing require positive replicate number.");
    Preconditions.checkArgument(hasher != null, "HashRing require non-null hasher.");
    this.replicate = replicate;
    this.hasher = hasher;
  }

  /**
   * Add a node to the ring, the node will be replicated to `replicate` virtual nodes.
   *
   * @param node the node to be added to the ring.
   * @return true if the node is added successfully, false otherwise.
   */
  public boolean addNode(T node) {
    lock.writeLock().lock();
    try {
      return add(node);
    } finally {
      lock.writeLock().unlock();
    }
  }

  /**
   * Remove the node from the ring, all the virtual nodes will be removed.
   *
   * @param node the node to be removed.
   * @return true if the node is removed successfully, false otherwise.
   */
  public boolean removeNode(T node) {
    lock.writeLock().lock();
    boolean removed = false;
    try {
      if (nodes.containsKey(node)) {
        Set<Partition<T>> partitions = nodes.remove(node);
        partitions.forEach(p -> ring.remove(p.getSlot()));
        removed = true;
      }
    } finally {
      lock.writeLock().unlock();
    }
    return removed;
  }

  /**
   * Allocate the node by the key, the number of nodes to be located is specified by the count.
   *
   * @param key the key to locate the node.
   * @param count the number of nodes to be located.
   * @return a set of nodes located by the key.
   */
  public Set<T> allocateNodes(String key, int count) {
    lock.readLock().lock();
    try {
      Set<T> res = new HashSet<>();
      if (key != null && count > 0) {
        if (count < nodes.size()) {
          long slot = hasher.hash(key, 0);
          Iterator<Partition<T>> it = new AllocateIterator(slot);
          while (it.hasNext() && res.size() < count) {
            Partition<T> part = it.next();
            res.add(part.getNode());
          }
        } else {
          res.addAll(nodes.keySet());
        }
      }
      return res;
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * Get all the nodes in the ring.
   *
   * @return a set of nodes in the ring.
   */
  public Set<T> getNodes() {
    lock.readLock().lock();
    try {
      return new HashSet<>(nodes.keySet());
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * Get the partitions of the node. return null if the node is not exists.
   *
   * @param node the node to get the partitions.
   * @return a set of partitions of the node.
   */
  public Set<Partition<T>> getPartition(T node) {
    lock.readLock().lock();
    try {
      // Return a defensive copy: the map value is internal mutable state, and getNodes() copies
      // for the same reason. Callers get a snapshot they can't use to mutate the ring.
      Set<Partition<T>> partitions = nodes.get(node);
      return partitions == null ? null : new HashSet<>(partitions);
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * Check if the node is in the ring.
   *
   * @param node the node to be checked.
   * @return true if the node is in the ring, false otherwise.
   */
  public boolean contains(T node) {
    lock.readLock().lock();
    try {
      return nodes.containsKey(node);
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * Check if the ring contains the slot.
   *
   * @param slot the slot to be checked.
   * @return true if the slot is in the ring, false otherwise.
   */
  public boolean ringContain(long slot) {
    lock.readLock().lock();
    try {
      return ring.containsKey(slot);
    } finally {
      lock.readLock().unlock();
    }
  }

  private boolean add(T node) {
    if (node == null) {
      return false;
    }
    // Validate the key before any map lookup: nodes.containsKey() invokes the node's
    // hashCode()/equals(), which may themselves dereference key() and NPE on a null key, masking
    // the intended fail-fast here.
    Preconditions.checkArgument(node.key() != null, "Node key must not be null: %s", node);
    if (nodes.containsKey(node)) {
      return false;
    }
    Set<Partition<T>> partitions =
        IntStream.range(0, replicate)
            .mapToObj(idx -> new Partition<T>(node, idx))
            .collect(Collectors.toSet());
    nodes.put(node, partitions);

    // allocate slot.
    for (Partition<T> partition : partitions) {
      long slot;
      int seed = 0;
      do {
        slot = this.hasher.hash(partition.getPartitionKey(), seed++);
      } while (ring.containsKey(slot));

      partition.setSlot(slot);
      ring.put(slot, partition);
    }
    return true;
  }

  public static class Partition<T extends ConsistentHash.Node> {
    private final T node;

    private final int index;

    // Hash the node by its logical key() so the ring is stable and reproducible across JVMs (avoid
    // node.toString(), which may fall back to the identity hash). Computed once: the key is
    // immutable and re-read on every collision-retry in add().
    private final String partitionKey;

    private long slot;

    public Partition(T node, int index) {
      this.node = node;
      this.index = index;
      this.partitionKey = node.key() + ":" + index;
    }

    public String getPartitionKey() {
      return partitionKey;
    }

    public T getNode() {
      return node;
    }

    // Non-public on purpose: only ConsistentHash.add() assigns the slot, during ring construction
    // (accessible as a nestmate). Keeping it out of the public API stops callers of getPartition()
    // from mutating ring state through a returned Partition (e.g. changing a slot so removeNode()
    // drops the wrong entry).
    private void setSlot(long slot) {
      this.slot = slot;
    }

    public long getSlot() {
      return this.slot;
    }
  }

  /** Base interface for the node in the ring. */
  public interface Node {
    /**
     * A stable, non-null identifier for this node. The ring hashes each virtual node by this key,
     * so it MUST be deterministic across JVMs and process restarts (e.g. derived from a host or
     * executor id) and consistent with {@link Object#equals}: two equal nodes must return the same
     * key. Returning identity- or {@code toString()}-based values breaks ring stability.
     */
    String key();
  }

  /** Base interface for the hash function. */
  public interface Hasher {
    long hash(String key, int seed);
  }

  private class AllocateIterator implements Iterator<Partition<T>> {
    private final Iterator<Partition<T>> head;
    private final Iterator<Partition<T>> tail;

    AllocateIterator(long slot) {
      this.head = ring.headMap(slot).values().iterator();
      this.tail = ring.tailMap(slot).values().iterator();
    }

    @Override
    public boolean hasNext() {
      return head.hasNext() || tail.hasNext();
    }

    @Override
    public Partition<T> next() {
      return tail.hasNext() ? tail.next() : head.next();
    }
  }
}

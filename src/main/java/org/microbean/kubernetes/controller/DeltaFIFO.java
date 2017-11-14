/* -*- mode: Java; c-basic-offset: 2; indent-tabs-mode: nil; coding: utf-8-unix -*-
 *
 * Copyright © 2017 MicroBean.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */
package org.microbean.kubernetes.controller;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.Set;
import java.util.Objects;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import java.util.function.BiConsumer;
import java.util.function.Function;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.ObjectMeta;

import org.microbean.kubernetes.controller.Delta.Type;

import static org.microbean.kubernetes.controller.Delta.Type.ADDED;
import static org.microbean.kubernetes.controller.Delta.Type.DELETED;
import static org.microbean.kubernetes.controller.Delta.Type.SYNC;
import static org.microbean.kubernetes.controller.Delta.Type.UPDATED;

// See https://github.com/kubernetes/client-go/blob/master/tools/cache/delta_fifo.go
public class DeltaFIFO<T> implements QueueStore<T, List<Delta<T>>> {

  private static final long serialVersionUID = 1L;

  private final Function<T, String> keyFunction;
  
  private final Delta.Compressor<T> compressor;

  private final Map<String, T> knownObjects;

  private final Map<String, List<Delta<T>>> deltasByKey;

  private final Queue<String> queue;

  private final ReentrantReadWriteLock lock;

  private final Lock readLock;

  private final Lock writeLock;

  private final Condition queueIsNotEmpty;

  private boolean populated;

  private int initialPopulationCount;

  public DeltaFIFO() {
    super();
    this.lock = new ReentrantReadWriteLock();
    this.readLock = this.lock.readLock();
    this.writeLock = this.lock.writeLock();
    this.queueIsNotEmpty = this.writeLock.newCondition();
    this.deltasByKey = new HashMap<>();
    this.queue = new LinkedList<>();
    this.keyFunction = DeltaFIFO::deletionHandlingNamespaceKeyFunction;
    this.compressor = null;
    this.knownObjects = null;
  }
    
  public DeltaFIFO(final Function<T, String> keyFunction,
                   final Delta.Compressor<T> compressor,
                   final Map<String, T> knownObjects) {
    super();
    this.lock = new ReentrantReadWriteLock();
    this.readLock = this.lock.readLock();
    this.writeLock = this.lock.writeLock();
    this.queueIsNotEmpty = this.writeLock.newCondition();
    this.deltasByKey = new HashMap<>();
    this.queue = new LinkedList<>();
    this.keyFunction = Objects.requireNonNull(keyFunction);
    this.compressor = compressor;
    if (knownObjects == null) {
      this.knownObjects = null;
    } else if (knownObjects.isEmpty()) {
      this.knownObjects = Collections.emptyMap();
    } else {
      this.knownObjects = Collections.unmodifiableMap(new HashMap<>(knownObjects));
    }

  }

  @Override
  public void close() {
    // TODO: figure out what this means given that Java queues don't close
  }

  public boolean isClosed() {
    return false;
  }

  @Override
  public boolean hasSynced() {
    this.readLock.lock();
    try {
      return this.populated && this.initialPopulationCount == 0;
    } finally {
      this.readLock.unlock();
    }
  }

  @Override
  public boolean contains(final T object) {
    boolean returnValue = false;
    if (object != null) {
      this.readLock.lock();
      try {
        returnValue = this.deltasByKey.containsKey(this.getKey(object));
      } finally {
        this.readLock.unlock();
      }
    }
    return returnValue;
  }

  @Override
  public boolean containsKey(final String key) {
    this.readLock.lock();
    try {
      return key != null && this.deltasByKey.containsKey(key);
    } finally {
      this.readLock.unlock();
    }
  }

  @Override
  public List<T> list() {
    final List<T> returnValue;
    this.readLock.lock();
    try {
      if (this.deltasByKey.isEmpty()) {
        returnValue = Collections.emptyList();
      } else {
        final Collection<List<Delta<T>>> deltasCollection = this.deltasByKey.values();
        assert deltasCollection != null;
        assert !deltasCollection.isEmpty();
        returnValue = new ArrayList<>();
        for (final List<Delta<T>> deltas : deltasCollection) {
          assert deltas != null;
          final Delta<T> newest = deltas.get(deltas.size() -1);
          assert newest != null;
          returnValue.add(newest.getObject());
        }
      }
    } finally {
      this.readLock.unlock();
    }
    return Collections.unmodifiableList(returnValue);
  }

  @Override
  public Set<String> listKeys() {
    final Set<String> returnValue;
    this.readLock.lock();
    try {
      if (this.deltasByKey.isEmpty()) {
        returnValue = Collections.emptySet();
      } else {
        returnValue = Collections.unmodifiableSet(new HashSet<>(this.deltasByKey.keySet()));
      }
    } finally {
      this.readLock.unlock();
    }
    return returnValue;
  }
    
  @Override
  public List<Delta<T>> get(final T object) {
    final List<Delta<T>> returnValue;
    if (object == null) {
      returnValue = null;
    } else {
      returnValue = this.getByKey(this.getKey(object));
    }
    return returnValue;
  }

  @Override
  public List<Delta<T>> getByKey(final String key) {
    final List<Delta<T>> returnValue;
    if (key == null) {
      returnValue = null;
    } else {
      this.readLock.lock();
      try {
        final List<Delta<T>> deltas = this.deltasByKey.get(key);
        if (deltas == null) {
          returnValue = null;
        } else if (deltas.isEmpty()) {
          returnValue = Collections.emptyList();
        } else {
          returnValue = new ArrayList<>(deltas);
        }
      } finally {
        this.readLock.unlock();
      }
    }
    return returnValue;
  }
    
  @Override
  public void add(final T object) {
    this.writeLock.lock();
    try {
      this.populated = true;
      this.enqueue(ADDED, object);
    } finally {
      this.writeLock.unlock();
    }
  }

  @Override
  public void addIfNotPresent(final List<Delta<T>> deltas) {
    Objects.requireNonNull(deltas);
    final Delta<T> newest = deltas.get(deltas.size() - 1);
    final String key = this.getKey(newest);
    if (key == null) {
      throw new IllegalArgumentException("Unable to get key for " + newest);
    }
    this.addIfNotPresent(key, deltas);
  }

  private final void addIfNotPresent(final String key, final List<Delta<T>> deltas) {
    Objects.requireNonNull(key);
    Objects.requireNonNull(deltas);
    this.writeLock.lock();
    try {
      this.populated = true;
      if (!this.deltasByKey.containsKey(key)) {        
        this.deltasByKey.put(key, deltas);
        this.queue.add(key);
        this.queueIsNotEmpty.signal();
      }
    } finally {
      this.writeLock.unlock();
    }
  }

  @Override
  public void update(final T object) {
    this.writeLock.lock();
    try {
      this.populated = true;
      this.enqueue(UPDATED, object);
    } finally {
      this.writeLock.unlock();
    }
  }

  @Override
  public void replace(final Collection<? extends T> objects, final String ignoredResourceVersion) {
    Objects.requireNonNull(objects);

    this.writeLock.lock();
    try {

      final Set<String> keys = new HashSet<>();
      for (final T object : objects) {
        final String key = this.getKey(object);
        if (key == null) {
          throw new IllegalArgumentException("Unable to get key for " + object);
        }
        keys.add(key);
      }

      if (this.knownObjects == null) {
        final Collection<Entry<String, List<Delta<T>>>> entrySet = this.deltasByKey.entrySet();
        if (entrySet != null && !entrySet.isEmpty()) {
          for (final Entry<String, List<Delta<T>>> entry : entrySet) {
            assert entry != null;
            final String key = entry.getKey();
            if (!keys.contains(key)) {
              final List<Delta<T>> deltas = entry.getValue();
              assert deltas != null;
              final Delta<T> newestDelta = deltas.get(deltas.size() - 1);
              assert newestDelta != null;
              final T deletedObject = newestDelta.getObject();
              assert deletedObject != null;
              this.enqueue(new DeletedFinalStateUnknown<>(key, deletedObject));
              if (!this.populated) {
                this.populated = true;
                this.initialPopulationCount = objects.size();
              }
            }
          }
        }
      } else {
        int queuedDeletions = 0;
        final Set<Entry<String, T>> entrySet = this.knownObjects.entrySet();
        if (entrySet != null && !entrySet.isEmpty()) {
          for (final Entry<String, T> entry : entrySet) {
            assert entry != null;
            final String knownKey = entry.getKey();
            assert knownKey != null;
            if (!keys.contains(knownKey)) {
              final T deletedObject = entry.getValue();
              queuedDeletions++;
              this.enqueue(new DeletedFinalStateUnknown<>(knownKey, deletedObject));
            }
          }
        }
        if (!this.populated) {
          this.populated = true;
          this.initialPopulationCount = objects.size() + queuedDeletions;
        }
      }
    } finally {
      this.writeLock.unlock();
    }
  }

  @Override
  public void delete(final T object) {
    Objects.requireNonNull(object);
    final String key = this.getKey(object);
    this.writeLock.lock();
    try {
      this.populated = true;
      if (this.deltasByKey.containsKey(key) || (this.knownObjects != null && this.knownObjects.containsKey(key))) {
        this.enqueue(DELETED, object);
      }
    } finally {
      this.writeLock.unlock();
    }
  }

  @Override
  public void resync() {
    this.writeLock.lock();
    try {
      if (this.knownObjects != null && !this.knownObjects.isEmpty()) {
        final Set<Entry<String, T>> entrySet = this.knownObjects.entrySet();
        if (entrySet != null && !entrySet.isEmpty()) {
          for (final Entry<String, T> entry : entrySet) {
            assert entry != null;
            final String knownKey = entry.getKey();
            assert knownKey != null;
            final T knownObject = entry.getValue();
            assert knownObject != null;
            final String key = this.getKey(knownObject);
            final List<Delta<T>> deltas = this.deltasByKey.get(key);
            if (deltas == null || deltas.isEmpty()) {
              this.enqueue(SYNC, knownObject);
            }
          }
        }
      }
    } finally {
      this.writeLock.unlock();
    }
  }

  @Override
  public List<Delta<T>> popAndProcessUsing(final BiConsumer<QueueStore<T, List<Delta<T>>>, List<Delta<T>>> processor) throws InterruptedException {
    Objects.requireNonNull(processor);
    List<Delta<T>> returnValue = null;
    this.writeLock.lock();
    try {
      PROCESSING_LOOP:
      while (returnValue == null) {
        while (this.queue.isEmpty()) {
          if (this.isClosed()) {
            break PROCESSING_LOOP;
          }
          this.queueIsNotEmpty.await();
        }
        final String key = this.queue.remove(); // will throw NoSuchElementException if the queue is empty; we've guarded against that
        final List<Delta<T>> deltas = this.deltasByKey.get(key);
        if (this.initialPopulationCount > 0) {
          this.initialPopulationCount--;
        }
        if (deltas != null) {
          this.deltasByKey.remove(key);
          try {
            processor.accept(this, Collections.unmodifiableList(deltas));
          } catch (final RequeueException requeueException) {
            this.addIfNotPresent(key, deltas);
            final Throwable cause = requeueException.getCause();
            if (cause instanceof RuntimeException) {
              throw (RuntimeException)cause;
            } else if (cause instanceof InterruptedException) {
              Thread.currentThread().interrupt();
              throw (InterruptedException)cause;
            } else if (cause instanceof Error) {
              throw (Error)cause;
            } else if (cause instanceof Exception) {
              throw new IllegalStateException(cause.getMessage(), cause);
            }
          }
          returnValue = deltas;
          assert returnValue != null;
        }
      }
    } finally {
      this.writeLock.unlock();
    }
    return returnValue;
  }

  public String getKey(Object object) {
    String returnValue = null;
    if (this.keyFunction != null) {
      if (object instanceof List) {
        final List<?> objects = (List<?>)object;
        object = objects.get(objects.size() - 1);
      }
      if (object instanceof Delta) {
        @SuppressWarnings("unchecked")
        final Delta<T> delta = (Delta<T>)object;
        final DeletedFinalStateUnknown<T> deletedObject = delta.getDeletedFinalStateUnknown();
        if (deletedObject == null) {
          returnValue = this.keyFunction.apply(delta.getObject());
        } else {
          returnValue = deletedObject.getKey();
        }
      } else if (object instanceof DeletedFinalStateUnknown) {
        returnValue = ((DeletedFinalStateUnknown)object).getKey();
      }
    }
    return returnValue;
  }

  private final void enqueue(final Type type, final T object) {
    Objects.requireNonNull(type);
    Objects.requireNonNull(object);
    this.enqueue(new Delta<T>(type, object));
  }

  private final void enqueue(final DeletedFinalStateUnknown<T> deletedObject) {
    Objects.requireNonNull(deletedObject);
    this.enqueue(new Delta<T>(DELETED, deletedObject));
  }

  private final void enqueue(final Delta<T> delta) {
    Objects.requireNonNull(delta);
    assert this.lock.isWriteLockedByCurrentThread();

    final Type type = delta.getType();
    assert type != null;

    final T object = delta.getObject();
    assert object != null;

    final String key = this.getKey(object);
    if (key == null) {
      throw new IllegalArgumentException("Unable to get key for " + object);
    }

    if (SYNC.equals(type) && willObjectBeDeleted(key)) {
      // EXIT POINT
      return;
    }

    List<Delta<T>> deltas = this.deltasByKey.get(key);
    final boolean exists;
    if (deltas == null) {
      exists = false;
      deltas = new ArrayList<>();
    } else {
      exists = true;
    }
    deltas.add(delta);
    deltas = deduplicateDeltas(deltas);
    if (this.compressor != null) {
      deltas = this.compressor.compress(deltas);
    }

    if (deltas != null && !deltas.isEmpty()) {
      this.deltasByKey.put(key, deltas);
      if (!exists) {
        this.queue.add(key);
        this.queueIsNotEmpty.signal();
      }
    } else if (exists) {
      this.deltasByKey.remove(key);
    }
  }

  private List<Delta<T>> deduplicateDeltas(final List<Delta<T>> deltas) {
    final List<Delta<T>> returnValue;
    final int size = deltas == null ? 0 : deltas.size();
    if (size < 2) {
      returnValue = deltas;
    } else {
      final Delta<T> lastDelta = deltas.get(size - 1);
      final Delta<T> nextToLastDelta = deltas.get(size - 2);
      final Delta<T> delta = arbitrateDuplicates(lastDelta, nextToLastDelta);
      if (delta == null) {
        returnValue = deltas;
      } else {
        returnValue = new ArrayList<>();
        returnValue.addAll(deltas.subList(0, size - 2));
        returnValue.add(delta);
      }
    }
    return returnValue;
  }

  private Delta<T> arbitrateDuplicates(final Delta<T> a, final Delta<T> b) {
    return arbitrateDeletionDuplicates(a, b);    
  }
  
  private final Delta<T> arbitrateDeletionDuplicates(final Delta<T> a, final Delta<T> b) {
    Delta<T> returnValue = null;
    if (a != null && b != null && DELETED.equals(a.getType()) && DELETED.equals(b.getType())) {
      if (b.getDeletedFinalStateUnknown() == null) {
        returnValue = b;
      } else {
        returnValue = a;
      }
    }
    return returnValue;
  }
  
  private final boolean willObjectBeDeleted(final String key) {
    assert this.lock.isWriteLockedByCurrentThread();
    final List<Delta<T>> deltas = this.deltasByKey.get(key);
    return deltas != null && !deltas.isEmpty() && deltas.get(deltas.size() - 1).getType().equals(DELETED);
  }

  public static final String deletionHandlingNamespaceKeyFunction(final Object object) {
    Objects.requireNonNull(object);
    String returnValue = null;
    if (object instanceof DeletedFinalStateUnknown) {
      returnValue = ((DeletedFinalStateUnknown<?>)object).getKey();
    } else {
      returnValue = namespaceKeyFunction(object);
    }
    return returnValue;
  }

  public static final String namespaceKeyFunction(final Object object) {
    Objects.requireNonNull(object);
    String returnValue = null;
    if (object instanceof HasMetadata) {
      final ObjectMeta metadata = ((HasMetadata)object).getMetadata();
      if (metadata != null) {
        final String namespace = metadata.getNamespace();
        if (namespace == null || namespace.isEmpty()) {
          returnValue = metadata.getName();
        } else {
          returnValue = new StringBuilder(namespace).append("/").append(metadata.getName()).toString();
        }
      }
    }
    return returnValue;
  }
  
}

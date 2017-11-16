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
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import java.util.function.Function;

final class ThreadSafeMap<T> implements ThreadSafeStore<T> {

  private final ReentrantReadWriteLock lock;

  private final Lock readLock;

  private final Lock writeLock;

  private final Map<String, T> items;
  
  private final Map<String, Function<T, Collection<String>>> indexers;

  private final Map<String, Map<String, Set<String>>> indices;
  
  public ThreadSafeMap(final Map<String, Function<T, Collection<String>>> indexers,
                       final Map<String, Map<String, Set<String>>> indices) {
    super();
    this.lock = new ReentrantReadWriteLock();
    this.readLock = this.lock.readLock();
    this.writeLock = this.lock.writeLock();
    this.items = new HashMap<>();
    this.indexers = indexers;
    this.indices = indices;
  }

  @Override
  public Collection<String> listIndexFuncValues(final String indexName) {
    Collection<String> returnValue = null;
    this.readLock.lock();
    try {
      // Note that the Go code does not check to see if indexName
      // actually identifies an index.  But it does in indexKeys().
      // We follow suit.
      final Map<String, Set<String>> index = this.indices.get(indexName);
      if (index != null && !index.isEmpty()) {
        returnValue = index.keySet();
      }
    } finally {
      this.readLock.unlock();
    }
    if (returnValue == null || returnValue.isEmpty()) {
      returnValue = Collections.emptySet();
    } else {
      returnValue = Collections.unmodifiableCollection(returnValue);
    }
    return returnValue;
  }

  @Override
  public Map<String, Function<T, Collection<String>>> getIndexers() {
    this.readLock.lock();
    try {
      return this.indexers;
    } finally {
      this.readLock.unlock();
    }
  }

  @Override
  public void addIndexers(final Map<String, Function<T, Collection<String>>> newIndexers) {
    if (newIndexers != null && !newIndexers.isEmpty()) {
      this.writeLock.lock();
      try {
        if (!this.items.isEmpty()) {
          throw new IllegalStateException();
        }
        final Set<String> oldKeys = this.indexers.keySet();
        assert oldKeys != null;
        final Set<String> newKeys = newIndexers.keySet();
        assert newKeys != null;
        if (!Collections.disjoint(oldKeys, newKeys)) {
          throw new IllegalStateException("indexer conflict: " + new HashSet<>(oldKeys).retainAll(newKeys));
        }
        this.indexers.putAll(newIndexers);
      } finally {
        this.writeLock.unlock();
      }
    }
  }

  @Override
  public void add(final String key, final T object) {
    if (key != null && object != null) {
      this.writeLock.lock();
      try {
        this.updateIndices(this.items.put(key, object), object, key);
      } finally {
        this.writeLock.unlock();
      }
    }
  }

  @Override
  public void update(final String key, final T object) {
    // The Go code has duplicated code.
    this.add(key, object);
  }

  @Override
  public void delete(final String key) {
    if (key != null) {
      this.writeLock.lock();
      try {
        this.deleteFromIndices(this.items.remove(key), key);
      } finally {
        this.writeLock.unlock();
      }
    }
  }

  @Override
  public T get(final String key) {
    T returnValue = null;
    if (key != null) {
      this.readLock.lock();
      try {
        returnValue = this.items.get(key);
      } finally {
        this.readLock.unlock();
      }
    }
    return returnValue;
  }

  @Override
  public Collection<T> list() {
    Collection<T> returnValue = null;
    this.readLock.lock();
    try {
      returnValue = this.items.values();
    } finally {
      this.readLock.unlock();
    }
    if (returnValue == null || returnValue.isEmpty()) {
      returnValue = Collections.emptySet();
    } else {
      returnValue = Collections.unmodifiableCollection(returnValue);
    }
    return returnValue;
  }

  @Override
  public Set<String> listKeys() {
    Set<String> returnValue = null;
    this.readLock.lock();
    try {
      returnValue = this.items.keySet();
    } finally {
      this.readLock.unlock();
    }
    if (returnValue == null || returnValue.isEmpty()) {
      returnValue = Collections.emptySet();
    } else {
      returnValue = Collections.unmodifiableSet(returnValue);
    }
    return returnValue;
  }

  @Override
  public void replace(final Map<String, T> items, final String ignoredResourceVersion) {
    this.writeLock.lock();
    try {
      this.items.clear();
      if (items != null && !items.isEmpty()) {
        this.items.putAll(items);
        final Set<Entry<String, T>> entrySet = items.entrySet();
        assert entrySet != null;
        assert !entrySet.isEmpty();
        for (final Entry<String, T> entry : entrySet) {
          this.updateIndices(null, entry.getValue(), entry.getKey());
        }
      }
    } finally {
      this.writeLock.unlock();
    }
  }

  @Override
  public Collection<T> index(final String indexName, final T object) {
    Collection<T> returnValue = null;
    if (indexName != null && object != null) {
      this.readLock.lock();
      try {
        final Function<T, Collection<String>> indexFunc = this.indexers.get(indexName);
        if (indexFunc != null) {
          final Collection<String> indexKeys = indexFunc.apply(object);
          if (indexKeys != null && !indexKeys.isEmpty()) {                      
            final Map<String, Set<String>> index = this.indices.get(indexName);
            if (index != null && !index.isEmpty()) {
              final Set<String> returnKeySet = new HashSet<>();
              for (final String indexKey : indexKeys) {
                assert indexKey != null;
                final Set<String> set = index.get(indexKey);
                if (set != null && !set.isEmpty()) {
                  returnKeySet.addAll(set);
                }
              }
              if (!returnKeySet.isEmpty()) {
                returnValue = new ArrayList<>();
                for (final String key : returnKeySet) {
                  assert key != null;
                  returnValue.add(this.items.get(key));
                }
              }
            }
          }
        }
      } finally {
        this.readLock.unlock();
      }
    }
    if (returnValue == null || returnValue.isEmpty()) {
      return Collections.emptySet();
    } else {
      returnValue = Collections.unmodifiableCollection(returnValue);
    }
    return returnValue;
  }

  @Override
  public Collection<T> byIndex(final String indexName, final String indexKey) {
    Collection<T> returnValue = null;
    if (indexName != null && indexKey != null) {
      this.readLock.lock();
      try {
        if (this.indexers.containsKey(indexName)) {
          final Map<String, Set<String>> index = this.indices.get(indexName);
          if (index != null && !index.isEmpty()) {
            final Set<String> set = index.get(indexKey);
            if (set != null && !set.isEmpty()) {
              returnValue = new ArrayList<>();
              for (final String key : set) {
                assert key != null;
                returnValue.add(this.items.get(key));
              }
            }
          }
        }
      } finally {
        this.readLock.unlock();
      }
    }
    if (returnValue == null || returnValue.isEmpty()) {
      returnValue = Collections.emptySet();
    } else {
      returnValue = Collections.unmodifiableCollection(returnValue);
    }
    return returnValue;
  }

  @Override
  public Collection<String> indexKeys(final String indexName, final String indexKey) {
    Collection<String> returnValue = null;
    this.readLock.lock();
    try {
      if (this.indexers.containsKey(indexName)) {
        final Map<String, Set<String>> index = this.indices.get(indexName);
        if (index != null && !index.isEmpty()) {
          returnValue = index.get(indexKey);
        }
      }
    } finally {
      this.readLock.unlock();
    }
    if (returnValue == null || returnValue.isEmpty()) {
      returnValue = Collections.emptySet();
    } else {
      returnValue = Collections.unmodifiableCollection(returnValue);
    }
    return returnValue;
  }

  private final void updateIndices(final T oldObject, final T newObject, final String key) {
    assert this.lock.isWriteLockedByCurrentThread();
    if (oldObject != null) {
      this.deleteFromIndices(oldObject, key);
    }
    final Set<Entry<String, Function<T, Collection<String>>>> entrySet = this.indexers.entrySet();
    assert entrySet != null;
    for (final Entry<String, Function<T, Collection<String>>> entry : entrySet) {
      assert entry != null;
      final String name = entry.getKey();
      assert name != null;
      final Function<T, Collection<String>> indexFunction = entry.getValue();
      assert indexFunction != null;
      final Collection<String> indexValues = indexFunction.apply(newObject);
      if (indexValues != null && !indexValues.isEmpty()) {
        final Map<String, Set<String>> index = this.indices.get(name);
        if (index != null && !index.isEmpty()) {
          for (final String indexValue : indexValues) {
            if (indexValue != null) {
              Set<String> set = index.get(indexValue);
              if (set == null) {
                set = new HashSet<>();
                index.put(indexValue, set);
              }
              assert set != null;
              set.add(key);
            }
          }
        }
      }
    }
  }

  private final void deleteFromIndices(final T object, final String key) {
    if (object != null && key != null) {
      assert this.lock.isWriteLockedByCurrentThread();
      final Set<Entry<String, Function<T, Collection<String>>>> entrySet = this.indexers.entrySet();
      assert entrySet != null;
      for (final Entry<String, Function<T, Collection<String>>> entry : entrySet) {
        assert entry != null;
        final String name = entry.getKey();
        assert name != null;
        final Function<T, Collection<String>> indexFunction = entry.getValue();
        assert indexFunction != null;
        final Collection<String> indexValues = indexFunction.apply(object);
        if (indexValues != null && !indexValues.isEmpty()) {
          final Map<String, Set<String>> index = this.indices.get(name);
          if (index != null && !index.isEmpty()) {
            for (final String indexValue : indexValues) {
              if (indexValue != null) {
                final Set<String> set = index.get(indexValue);
                if (set != null) {
                  set.remove(key);
                }
              }
            }
          }
        }
      }
    }
  }

  @Override
  public void resync() {
    // Nothing to do
  }


  
}

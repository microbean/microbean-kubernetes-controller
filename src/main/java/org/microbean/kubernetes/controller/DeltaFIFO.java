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
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.Objects;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import java.util.function.Function;

import org.microbean.kubernetes.controller.Delta.Type;

import static org.microbean.kubernetes.controller.Delta.Type.ADDED;
import static org.microbean.kubernetes.controller.Delta.Type.DELETED;
import static org.microbean.kubernetes.controller.Delta.Type.SYNC;
import static org.microbean.kubernetes.controller.Delta.Type.UPDATED;

// See https://github.com/kubernetes/client-go/blob/master/tools/cache/delta_fifo.go
public abstract class DeltaFIFO<T> implements QueueStore<T> {

  private static final long serialVersionUID = 1L;

  private final Function<T, String> keyFunction;
  
  private final Delta.Compressor<T> compressor;

  private final Map<String, T> knownObjects;

  private final Map<String, List<Delta<T>>> deltasByKey;

  private final BlockingQueue<String> queue;

  private final Object lock;

  private boolean populated;

  private int initialPopulationCount;
  
  public DeltaFIFO(final Function<T, String> keyFunction,
                   final Delta.Compressor<T> compressor,
                   final Map<String, T> knownObjects) {
    super();
    this.lock = new byte[0];
    this.deltasByKey = new HashMap<>();
    this.queue = new LinkedBlockingQueue<>();
    this.keyFunction = Objects.requireNonNull(keyFunction);
    this.compressor = compressor;
    if (knownObjects == null || knownObjects.isEmpty()) {
      this.knownObjects = Collections.emptyMap();
    } else {
      this.knownObjects = new HashMap<>(knownObjects);
    }

  }

  @Override
  public void add(final T object) {
    synchronized (lock) {
      this.populated = true;
      this.enqueue(ADDED, object);
    }    
  }

  @Override
  public void addIfNotPresent(final List<Delta<T>> deltas) {
    Objects.requireNonNull(deltas);
    final Delta<T> newest = deltas.get(deltas.size() - 1);
    final String key = this.getKey(newest);
    synchronized (lock) {
      this.populated = true;
      if (!this.deltasByKey.containsKey(key)) {
        this.deltasByKey.put(key, deltas);
        this.queue.add(key);
      }
    }
  }

  @Override
  public void update(final T object) {
    synchronized (lock) {
      this.populated = true;
      this.enqueue(UPDATED, object);
    }
  }

  @Override
  public void replace(final Collection<? extends T> objects, final String ignoredResourceVersion) {
    Objects.requireNonNull(objects);
    Objects.requireNonNull(resourceVersion);
    synchronized (lock) {

      final Set<String> keys = new HashSet<>();
      for (final T object : objects) {
        final String key = this.getKey(object);
        keys.add(key);
      }

      if (this.knownObjects == null || this.knownObjects.isEmpty()) {
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
    }
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
    assert Thread.holdsLock(this.lock);
    final Type type = delta.getType();
    assert type != null;
    final T object = delta.getObject();
    assert object != null;
    final String key = this.getKey(object);
    if (SYNC.equals(type) && willObjectBeDeleted(key)) {
      return;
    }

    List<Delta<T>> deltas = this.deltasByKey.get(key);
    if (deltas == null) {
      deltas = new ArrayList<>();
    }
    deltas = deduplicateDeltas(deltas);
    if (this.compressor != null) {
      deltas = this.compressor.compress(deltas);
    }

    final boolean exists = this.deltasByKey.containsKey(key);
    if (deltas != null && !deltas.isEmpty()) {
      this.deltasByKey.put(key, deltas);
      if (!exists) {
        this.queue.add(key);
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
    assert Thread.holdsLock(this.lock);
    final List<Delta<T>> deltas = this.deltasByKey.get(key);
    return deltas != null && !deltas.isEmpty() && deltas.get(deltas.size() - 1).getType().equals(DELETED);
  }

  /*
// queueActionLocked appends to the delta list for the object, calling
// f.deltaCompressor if needed. Caller must lock first.
func (f *DeltaFIFO) queueActionLocked(actionType DeltaType, obj interface{}) error {
	id, err := f.KeyOf(obj)
	if err != nil {
		return KeyError{obj, err}
	}

	// If object is supposed to be deleted (last event is Deleted),
	// then we should ignore Sync events, because it would result in
	// recreation of this object.
	if actionType == Sync && f.willObjectBeDeletedLocked(id) {
		return nil
	}

	newDeltas := append(f.items[id], Delta{actionType, obj})
	newDeltas = dedupDeltas(newDeltas)
	if f.deltaCompressor != nil {
		newDeltas = f.deltaCompressor.Compress(newDeltas)
	}

	_, exists := f.items[id]
	if len(newDeltas) > 0 {
		if !exists {
			f.queue = append(f.queue, id)
		}
		f.items[id] = newDeltas
		f.cond.Broadcast()
	} else if exists {
		// The compression step removed all deltas, so
		// we need to remove this from our map (extra items
		// in the queue are ignored if they are not in the
		// map).
		delete(f.items, id)
	}
	return nil
}
  */
  
}

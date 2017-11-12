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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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

  private final Map<String, List<Delta<T>>> deltasByKey;

  private final BlockingQueue<String> queue;

  private final Object lock;
  
  public DeltaFIFO(final Function<T, String> keyFunction,
                   final Delta.Compressor<T> compressor) {
    super();
    this.lock = new byte[0];
    this.deltasByKey = new HashMap<>();
    this.queue = new LinkedBlockingQueue<>();
    this.keyFunction = Objects.requireNonNull(keyFunction);
    this.compressor = compressor;

  }

  @Override
  public void add(final T object) {
    synchronized (lock) {
      this.enqueue(ADDED, object);
    }    
  }

  @Override
  public void update(final T object) {
    synchronized (lock) {
      this.enqueue(UPDATED, object);
    }
  }

  public String getKey(final T object) {
    String returnValue = null;
    if (this.keyFunction != null) {
      returnValue = this.keyFunction.apply(object);
    }
    return returnValue;
  }

  private final void enqueue(final Type type, final T object) {
    assert Thread.holdsLock(this.lock);
    final String key = this.getKey(object);
    if (SYNC.equals(type) && willObjectBeDeleted(key)) {
      return;
    }

    final Delta<T> delta = new Delta<>(type, object);
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

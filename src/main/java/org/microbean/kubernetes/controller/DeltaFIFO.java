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

import java.util.ArrayDeque;

// See https://github.com/kubernetes/client-go/blob/master/tools/cache/delta_fifo.go
public class DeltaFIFO<T> extends ArrayDeque<Delta<T>> {

  private static final long serialVersionUID = 1L;
  
  private final Delta.Compressor<T> compressor;
  
  public DeltaFIFO(final Delta.Compressor<T> compressor) {
    super();
    this.compressor = compressor;
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

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

import java.util.List;
import java.util.Objects;

// See https://github.com/kubernetes/client-go/blob/master/tools/cache/delta_fifo.go
public class Delta<T> {

  private final Type type;
  
  private final T object;

  private final DeletedFinalStateUnknown deletedFinalStateUnknown;

  public Delta(final Type type, final T object) {
    super();
    this.type = Objects.requireNonNull(type);
    this.object = object;
    this.deletedFinalStateUnknown = null;
  }

  Delta(final Type type, final DeletedFinalStateUnknown deletedFinalStateUnknown) {
    super();
    this.type = Objects.requireNonNull(type);
    this.object = null;
    this.deletedFinalStateUnknown = deletedFinalStateUnknown;
  }

  public final Type getType() {
    return this.type;
  }
  
  public final T getObject() {
    return this.object;
  }

  final DeletedFinalStateUnknown getDeletedFinalStateUnknown() {
    return this.deletedFinalStateUnknown;
  }

  public static interface Compressor<T> {

    public List<Delta<T>> compress(final List<Delta<T>> deltas);
    
  }
  
  public static enum Type {
    ADDED,
    UPDATED,
    DELETED,

    /**
     * Indicates a watch has expired and a new cycle has started, or you've turned on periodic syncs.
     */
    SYNC;
  }

}

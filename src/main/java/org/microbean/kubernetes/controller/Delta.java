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

import java.util.Collection;
import java.util.Objects;

// See https://github.com/kubernetes/client-go/blob/master/tools/cache/delta_fifo.go
public class Delta<T> {

  private final Type type;
  
  private final T object;

  public Delta(final Type type, final T object) {
    super();
    this.type = Objects.requireNonNull(type);
    this.object = object;
  }

  public static interface Compressor<T> {

    public Collection<Delta<T>> compress(final Collection<Delta<T>> deltas);
    
  }
  
  public static enum Type {
    ADDED,
    UPDATED,
    DELETED,
    SYNC;
  }

}

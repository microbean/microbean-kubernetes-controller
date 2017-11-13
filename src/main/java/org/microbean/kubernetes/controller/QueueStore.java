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
import java.util.List;
import java.util.Set;

import java.util.function.BiConsumer;

import org.microbean.development.annotation.Experimental;

@Experimental
// See https://github.com/kubernetes/client-go/blob/master/tools/cache/fifo.go
public interface QueueStore<T> extends Store<T> {

  public T popAndProcessUsing(final BiConsumer<QueueStore<T>, T> processor);

  public void addIfNotPresent(final List<Delta<T>> object);

  public boolean hasSynced();

  public void close();
  
}

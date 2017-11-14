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
import java.util.Set;

import org.microbean.development.annotation.Experimental;

@Experimental
// See https://github.com/kubernetes/client-go/blob/master/tools/cache/store.go
public interface Store<T, R> {

  public void add(final T object);

  public void update(final T object);

  public void delete(final T object); // remove

  public Collection<T> list(); // values

  public Set<String> listKeys(); // keySet

  public R get(final T object);

  public R getByKey(final String key);
  
  public boolean contains(final T object);

  public boolean containsKey(final String key);

  public void replace(final Collection<? extends T> objects, final String resourceVersion);

  public void resync();

}

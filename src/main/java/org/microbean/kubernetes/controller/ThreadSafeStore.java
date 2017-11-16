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
import java.util.Map;
import java.util.Set;

import java.util.function.Function;

interface ThreadSafeStore<T> {

  public void add(final String key, final T object);

  public void update(final String key, final T object);

  public void delete(final String key);

  public T get(final String key);

  public Collection<T> list();

  public Set<String> listKeys();

  public void replace(final Map<String, T> items, final String resourceVersion);

  public Collection<T> index(final String indexName, final T object);

  public Collection<String> indexKeys(final String indexName, final String indexKey);

  public Collection<String> listIndexFuncValues(final String name);

  public Collection<T> byIndex(final String indexName, final String indexKey);

  public Map<String, Function<T, Collection<String>>> getIndexers();

  public void addIndexers(final Map<String, Function<T, Collection<String>>> newIndexers);

  public void resync();
  
}

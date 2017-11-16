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
import java.util.Map;
import java.util.Set;

import java.util.function.Function;

public interface Indexer<T, L> extends Store<T, L> {

  public List<T> index(final String indexName, final Object something);

  public Set<String> indexKeys(final String indexName, final String indexKey);

  public List<String> listIndexFuncValues(final String indexName);

  public List<T> byIndex(final String indexName, final String indexKey);

  public Map<String, Function<T, Collection<String>>> getIndexers();

  public void addIndexers(final Map<String, Function<T, Collection<String>>> indexers);
  
}

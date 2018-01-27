/* -*- mode: Java; c-basic-offset: 2; indent-tabs-mode: nil; coding: utf-8-unix -*-
 *
 * Copyright © 2017-2018 microBean.
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

import io.fabric8.kubernetes.api.model.HasMetadata;

public interface EventCache<T extends HasMetadata> {

  /**
   * Adds the supplied {@link Event} to this {@link EventCache}
   * implementation.
   *
   * @param event the event to add; must not be {@code null}
   *
   * @return {@code true} if an addition actually occurred, {@code
   * false} otherwise
   *
   * @exception NullPointerException if {@code event} is {@code null}
   *
   * @excepiton IllegalArgumentException if {@code event} is
   * unsuitable in some other way
   */
  public boolean add(final Event<T> event);

  /**
   * Replaces all internal state with new state derived from the
   * supplied {@link Collection} of resources.
   *
   * @param incomingResources the resources comprising the new state;
   * must not be {@code null}
   *
   * @param resourceVersion the notional version of the supplied
   * {@link Collection}; may be {@code null}
   *
   * @exception NullPointerException if {@code incomingResources} is
   * {@code null}
   */
  public void replace(final Collection<? extends T> incomingResources, final Object resourceVersion);

  /**
   * Synchronizes this {@link EventCache} implementation with its
   * source.
   *
   * <p>Not all {@link EventCache} implementations need support
   * resynchronization.</p>
   */
  public void resync();
  
}

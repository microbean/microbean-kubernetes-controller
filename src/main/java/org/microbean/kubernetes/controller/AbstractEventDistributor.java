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

import java.util.Map;
import java.util.Objects;

import io.fabric8.kubernetes.api.model.HasMetadata;

/**
 * An abstract {@link ResourceTrackingEventQueueConsumer} whose
 * implementations are particularly suitable for supplying to the
 * {@link EventQueueCollection#start(Consumer)} method and can
 * distribute {@link Event}s in some fashion.
 *
 * @author <a href="https://about.me/lairdnelson"
 * target="_parent">Laird Nelson</a>
 *
 * @see #fireEvent(Event)
 *
 * @see EventQueue
 *
 * @see EventQueueCollection#start(Consumer)
 */
public abstract class AbstractEventDistributor<T extends HasMetadata> extends ResourceTrackingEventQueueConsumer<T> {


  /*
   * Constructors.
   */


  /**
   * Creates a new {@link AbstractEventDistributor}.
   *
   * @param knownObjects a mutable {@link Map} of {@link HasMetadata}
   * objects indexed by their keys (often a pairing of namespace and
   * name); may be {@code null}, but this will result in inaccurate
   * deletion tracking and the inability to keep track of the
   * {@linkplain Event#getPriorResource() prior state of resources};
   * if non-{@code null} <strong>will have its contents
   * changed</strong> by this {@link AbstractEventDistributor}'s
   * {@link #accept(EventQueue)} method; if non-{@code null}
   * <strong>will be synchronized on</strong> by this {@link
   * AbstractEventDistributor}'s {@link #accept(EventQueue)} method
   *
   * @see #accept(EventQueue)
   */
  protected AbstractEventDistributor(final Map<Object, T> knownObjects) {
    super(knownObjects);
  }


  /*
   * Instance methods.
   */
  

  /**
   * Implements the {@link
   * ResourceTrackingEventQueueConsumer#accept(Event.Type,
   * HasMetadata, HasMetadata)} method by creating an {@link Event}
   * and calling the {@link #fireEvent(Event)} method with it.
   *
   * @param eventType the {@link Event.Type} describing the new {@link
   * Event}; must not be {@code null}
   *
   * @param priorResource a {@link HasMetadata} representing the
   * <em>prior state</em> of the {@linkplain Event#getResource() Kubernetes
   * resource the new <code>Event</code> will primarily concern}; may
   * be&mdash;<strong>and often is</strong>&mdash;null
   *
   * @param resource a {@link HasMetadata} representing a Kubernetes
   * resource; must not be {@code null}
   *
   * @exception NullPointerException if {@code eventType} or {@code
   * resource} is {@code null}
   */
  @Override
  protected void accept(final Event.Type eventType, final T priorResource, final T resource) {
    this.fireEvent(new Event<T>(this, Objects.requireNonNull(eventType), priorResource, Objects.requireNonNull(resource)));
  }
  
  /**
   * Distributes the supplied {@link Event} in some way.
   *
   * <p>Implementations of this method should be relatively fast as
   * this method dictates the speed of {@link EventQueue}
   * processing.</p>
   *
   * @param event the {@link Event} to distribute; must not be {@code null}
   *
   * @exception NullPointerException if {@code event} is {@code null}
   */
  protected abstract void fireEvent(final Event<T> event);
  
}

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

import java.io.Serializable; // for javadoc only

import java.util.EventObject;

import io.fabric8.kubernetes.api.model.HasMetadata;

/**
 * An {@link AbstractEvent} that describes an {@link EventCache}
 * synchronization event.
 *
 * @param <T> a type of Kubernetes resource
 *
 * @author <a href="https://about.me/lairdnelson"
 * target="_parent">Laird Nelson</a>
 *
 * @see EventCache
 */
public class SynchronizationEvent<T extends HasMetadata> extends AbstractEvent<T> {


  /*
   * Static fields.
   */


  /**
   * The version of this class for {@linkplain Serializable
   * serialization purposes}.
   *
   * @see Serializable
   */
  private static final long serialVersionUID = 1L;


  /*
   * Constructors.
   */


  /**
   * Creates a new {@link SynchronizationEvent}.
   *
   * @param source the creator; must not be {@code null}
   *
   * @param type the {@link Type} of this {@link
   * SynchronizationEvent}; must not be {@code null}; must not be
   * {@link Type#DELETION}
   *
   * @param priorResource a {@link HasMetadata} representing the
   * <em>prior state</em> of the {@linkplain #getResource() Kubernetes
   * resource this <code>Event</code> primarily concerns}; may
   * be&mdash;<strong>and often is</strong>&mdash;null
   *
   * @param resource a {@link HasMetadata} representing a Kubernetes
   * resource; must not be {@code null}
   *
   * @exception NullPointerException if {@code source}, {@code type}
   * or {@code resource} is {@code null}
   *
   * @exception IllegalArgumentException if {@link Type#DELETION} is
   * equal to {@code type}
   *
   * @see Type
   *
   * @see EventObject#getSource()
   */
  public SynchronizationEvent(final Object source, final Type type, final T priorResource, final T resource) {
    super(source, type, priorResource, resource);
    if (Type.DELETION.equals(type)) {
      throw new IllegalArgumentException("DELETION.equals(type): " + type);
    }
  }


  /*
   * Instance methods.
   */


  /**
   * Returns {@code true} if the supplied {@link Object} is also a
   * {@link SynchronizationEvent} and is equal in every respect to
   * this one.
   *
   * @param other the {@link Object} to test; may be {@code null} in
   * which case {@code false} will be returned
   *
   * @return {@code true} if the supplied {@link Object} is also a
   * {@link SynchronizationEvent} and is equal in every respect to
   * this one; {@code false} otherwise
   */
  @Override
  public boolean equals(final Object other) {
    if (other == this) {
      return true;
    } else if (other instanceof SynchronizationEvent) {

      final boolean superEquals = super.equals(other);
      if (!superEquals) {
        return false;
      }

      return true;
    } else {
      return false;
    }
  }

}

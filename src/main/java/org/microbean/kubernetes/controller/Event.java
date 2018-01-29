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
import java.util.Objects;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.ObjectMeta;

/**
 * An {@link EventObject} that represents another event that has
 * occurred to a Kubernetes resource, usually as found in an {@link
 * EventCache} implementation.
 *
 * @param <T> a type of Kubernetes resource
 *
 * @author <a href="https://about.me/lairdnelson"
 * target="_parent">Laird Nelson</a>
 *
 * @see EventCache
 */
public class Event<T extends HasMetadata> extends EventObject {


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
   * Instance fields.
   */
  

  /**
   * The key that identifies this {@link Event}'s {@linkplain
   * #getResource() resource} <strong>only when its final state is
   * unknown</strong>.
   *
   * <p>This field can be&mdash;and often is&mdash;{@code null}.</p>
   *
   * @see #getKey()
   *
   * @see #setKey(Object)
   */
  private volatile Object key;

  /**
   * The {@link Type} describing the type of this {@link Event}.
   *
   * <p>This field is never {@code null}.</p>
   *
   * @see #getType()
   */
  private final Type type;

  /**
   * A Kubernetes resource representing the <em>prior</em> state of
   * the resource returned by this {@link Event}'s {@link
   * #getResource()} method.
   *
   * <p>This field may be {@code null}.</p>
   *
   * <p>The prior state of a given Kubernetes resource is often not
   * known, so this field is often {@code null}.</p>
   *
   * @see #getResource()
   */
  private final T priorResource;
  
  /**
   * A Kubernetes resource representing its state at the time of this
   * event.
   *
   * <p>This field is never {@code null}.</p>
   *
   * @see #getResource()
   */
  private final T resource;


  /*
   * Constructors.
   */


  /**
   * Creates a new {@link Event}.
   *
   * @param source the creator; must not be {@code null}
   *
   * @param type the {@link Type} of this {@link Event}; must not be
   * {@code null}
   *
   * @param priorResource a {@link HasMetadata} representing the
   * <em>prior state</em> of the {@linkplain #getResource() Kubernetes
   * resource this <code>Event</code> primarily concerns}; may
   * be&mdash;<strong>and often is</strong>&mdash;null
   *
   * @param resource a {@link HasMetadata} representing a Kubernetes
   * resource; must not be {@code null}
   *
   * @see Type
   *
   * @see EventObject#getSource()
   */
  protected Event(final Object source, final Type type, final T priorResource, final T resource) {
    super(source);
    this.type = Objects.requireNonNull(type);
    this.priorResource = priorResource;
    this.resource = Objects.requireNonNull(resource);
  }


  /*
   * Instance methods.
   */


  /**
   * Returns a {@link Type} representing the type of this {@link
   * Event}.
   *
   * <p>This method never returns {@code null}.</p>
   *
   * @return a non-{@code null} {@link Type}
   *
   * @see Type
   */
  public final Type getType() {
    return this.type;
  }

  /**
   * Returns a {@link HasMetadata} representing the <em>prior
   * state</em> of the Kubernetes resource this {@link Event}
   * primarily concerns.
   *
   * <p>This method may return {@code null}, and often does.</p>
   *
   * <p>The prior state of a Kubernetes resource is often not known at
   * {@link Event} construction time so it is common for this method
   * to return {@code null}.
   *
   * @return a {@link HasMetadata} representing the <em>prior
   * state</em> of the {@linkplain #getResource() Kubernetes resource
   * this <code>Event</code> primarily concerns}, or {@code null}
   *
   * @see #getResource()
   */
  public final T getPriorResource() {
    return this.priorResource;
  }

  /**
   * Returns a {@link HasMetadata} representing the Kubernetes
   * resource this {@link Event} concerns.
   *
   * <p>This method never returns {@code null}.</p>
   *
   * @return a non-{@code null} Kubernetes resource
   */
  public final T getResource() {
    return this.resource;
  }

  /**
   * Returns {@code true} if this {@link Event}'s {@linkplain
   * #getResource() resource} is an accurate representation of its
   * last known state.
   *
   * @return {@code true} if this {@link Event}'s {@linkplain
   * #getResource() resource} is an accurate representation of its
   * last known state; {@code false} otherwise
   */
  public final boolean isFinalStateKnown() {
    return this.key == null;
  }

  /**
   * Sets the key identifying the Kubernetes resource this {@link
   * Event} describes.
   *
   * @param key the new key; may be {@code null}
   *
   * @see #getKey()
   */
  final void setKey(final Object key) {
    this.key = key;
  }

  /**
   * Returns a key that can be used to unambiguously identify this
   * {@link Event}'s {@linkplain #getResource() resource}.
   *
   * <p>This method may return {@code null} in exceptional cases, but
   * normally does not.</p>
   *
   * <p>Overrides of this method must not return {@code null} except
   * in exceptional cases.</p>
   *
   * @return a key for this {@link Event}, or {@code null}
   */
  public Object getKey() {
    Object returnValue = this.key;
    if (returnValue == null) {
      final T resource = this.getResource();
      assert resource != null;
      final ObjectMeta metadata = resource.getMetadata();
      if (metadata != null) {
        final String namespace = metadata.getNamespace();
        if (namespace == null || namespace.isEmpty()) {
          returnValue = metadata.getName();
        } else {
          returnValue = new StringBuilder(namespace).append("/").append(metadata.getName()).toString();
        }
      }
    }
    return returnValue;
  }

  /**
   * Returns a hashcode for this {@link Event}.
   *
   * @return a hashcode for this {@link Event}
   */
  @Override
  public int hashCode() {
    int hashCode = 37;
    
    final Object source = this.getSource();
    int c = source == null ? 0 : source.hashCode();
    hashCode = hashCode * 17 + c;
    
    final Object key = this.getKey();
    c = key == null ? 0 : key.hashCode();
    hashCode = hashCode * 17 + c;
    
    final Object type = this.getType();
    c = type == null ? 0 : type.hashCode();
    hashCode = hashCode * 17 + c;
    
    final Object resource = this.getResource();
    c = resource == null ? 0 : resource.hashCode();
    hashCode = hashCode * 17 + c;

    final Object priorResource = this.getPriorResource();
    c = priorResource == null ? 0 : priorResource.hashCode();
    hashCode = hashCode * 17 + c;
    
    return hashCode;
  }

  /**
   * Returns {@code true} if the supplied {@link Object} is also an
   * {@link Event} and is equal in every respect to this one.
   *
   * @param other the {@link Object} to test; may be {@code null} in
   * which case {@code false} will be returned
   *
   * @return {@code true} if the supplied {@link Object} is also an
   * {@link Event} and is equal in every respect to this one; {@code
   * false} otherwise
   */
  @Override
  public boolean equals(final Object other) {
    if (other == this) {
      return true;
    } else if (other instanceof Event) {
      
      final Event<?> her = (Event<?>)other;
      
      final Object source = this.getSource();
      if (source == null) {
        if (her.getSource() != null) {
          return false;
        }
      } else if (!source.equals(her.getSource())) {
        return false;
      }
      
      final Object key = this.getKey();
      if (key == null) {
        if (her.getKey() != null) {
          return false;
        }
      } else if (!key.equals(her.getKey())) {
        return false;
      }
      
      final Object type = this.getType();
      if (type == null) {
        if (her.getType() != null) {
          return false;
        }
      } else if (!type.equals(her.getType())) {
        return false;
      }
      
      final Object resource = this.getResource();
      if (resource == null) {
        if (her.getResource() != null) {
          return false;
        }
      } else if (!resource.equals(her.getResource())) {
        return false;
      }

      final Object priorResource = this.getPriorResource();
      if (priorResource == null) {
        if (her.getPriorResource() != null) {
          return false;
        }
      } else if (!priorResource.equals(her.getPriorResource())) {
        return false;
      }

      
      return true;
    } else {
      return false;
    }
  }

  /**
   * Returns a {@link String} representation of this {@link Event}.
   *
   * <p>This method never returns {@code null}.</p>
   *
   * <p>Overrides of this method must not return {@code null}.</p>
   *
   * @return a non-{@code null} {@link String} representation of this
   * {@link Event}
   */
  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder().append(this.getType()).append(": ");
    final Object priorResource = this.getPriorResource();
    if (priorResource != null) {
      sb.append(priorResource).append(" --> ");
    }
    sb.append(this.getResource());
    return sb.toString();
  }


  /*
   * Inner and nested classes.
   */


  /**
   * The type of an {@link Event}.
   *
   * @author <a href="https://about.me/lairdnelson"
   * target="_parent">Laird Nelson</a>
   */
  public static enum Type {

    /**
     * A {@link Type} representing the addition of a resource.
     */
    ADDITION,

    /**
     * A {@link Type} representing the modification of a resource.
     */
    MODIFICATION,

    /**
     * A {@link Type} representing the deletion of a resource.
     */
    DELETION,

    /**
     * A {@link Type} representing the desired synchronization of a
     * resource.
     */
    SYNCHRONIZATION      
  }
  
}

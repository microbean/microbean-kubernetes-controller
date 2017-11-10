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

public abstract class Controller {

  /*
   * The Go code's Run function takes a channel, but the channel is
   * just used to receive a message to notify the controller that it
   * should close its associated Queue.  There is I'm sure a much more
   * idiomatic way to do it in Java; Controller maybe should be an
   * ExecutorService.
   */
  public abstract void run();

  public abstract boolean hasSynced();

  public abstract String getLastSyncResourceVersion();
  
}

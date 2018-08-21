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

import java.util.concurrent.Future;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestInterruptionBehavior {

  public TestInterruptionBehavior() {
    super();
  }

  @Test
  public void testInterruptionAndScheduledThreadPoolExecutorInteraction() throws Exception {
    final ScheduledThreadPoolExecutor e = new ScheduledThreadPoolExecutor(1);
    final Future<?> task = e.scheduleWithFixedDelay(() -> {
        assertFalse(Thread.currentThread().isInterrupted());
        while (!Thread.currentThread().isInterrupted()) {
          try {
            synchronized (TestInterruptionBehavior.this) {
              TestInterruptionBehavior.this.wait();
            }
            assertFalse(Thread.currentThread().isInterrupted());
          } catch (final InterruptedException interruptedException) {
            Thread.currentThread().interrupt();
            assertTrue(Thread.currentThread().isInterrupted());
          } catch (final RuntimeException runtimeException) {
            runtimeException.printStackTrace();
            throw runtimeException;
          }
        }
        assertTrue(Thread.currentThread().isInterrupted());
      }, 0L, 1L, TimeUnit.MILLISECONDS);
    assertTrue(task.cancel(true)); // should interrupt
    e.shutdown();
    assertTrue(e.awaitTermination(2L, TimeUnit.SECONDS));
    e.shutdownNow();
    assertTrue(e.awaitTermination(2L, TimeUnit.SECONDS));
  }
  
}

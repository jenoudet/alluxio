/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.master.services.web;

import alluxio.master.AlluxioMasterProcess;

/**
 * A web server service that never ceases executing.
 */
public class AlwaysOnWebServerMicroservice extends WebServerMicroservice {
  /**
   * @param masterProcess required to construct the web server
   */
  AlwaysOnWebServerMicroservice(AlluxioMasterProcess masterProcess) {
    super(masterProcess);
  }

  @Override
  public synchronized void start() {
    startWebServer();
  }

  @Override
  public synchronized void promote() {}

  @Override
  public synchronized void demote() {}

  @Override
  public synchronized void stop() {
    stopWebServer();
  }
}

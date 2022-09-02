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

import alluxio.conf.Configuration;
import alluxio.master.AlluxioMasterProcess;
import alluxio.network.RejectingServer;
import alluxio.util.network.NetworkAddressUtils;

import java.net.InetSocketAddress;
import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

/**
 * Operates the web server of the {@link alluxio.master.AlluxioMasterProcess}.
 */
class DefaultWebServerMicroservice extends WebServerMicroservice {
  @Nullable @GuardedBy("this")
  private RejectingServer mRejectingWebServer = null;

  DefaultWebServerMicroservice(AlluxioMasterProcess masterProcess) {
    super(masterProcess);
  }

  @Override
  public synchronized void start() {
    InetSocketAddress webAddress =
        NetworkAddressUtils.getBindAddress(mServiceType, Configuration.global());
    mRejectingWebServer = new RejectingServer(webAddress);
    mRejectingWebServer.start();
  }

  @Override
  public synchronized void promote() {
    stopRejectingServer();
    startWebServer();
  }

  @Override
  public synchronized void demote() {
    stopWebServer();
    start(); // start rejecting sever again
  }

  @Override
  public synchronized void stop() {
    stopRejectingServer();
    stopWebServer();
  }

  private void stopRejectingServer() {
    if (mRejectingWebServer != null) {
      mRejectingWebServer.stopAndJoin();
      mRejectingWebServer = null;
    }
  }
}

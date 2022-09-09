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

package alluxio.master.microservices.web;

import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.master.AlluxioMasterProcess;
import alluxio.master.microservices.MasterProcessMicroservice;
import alluxio.util.ConfigurationUtils;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.util.network.NetworkAddressUtils.ServiceType;
import alluxio.web.MasterWebServer;
import alluxio.web.WebServer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

/**
 * Abstract implementation of the {@link alluxio.master.AlluxioMasterProcess} web server service.
 * Provides common implementation for creating a web server.
 */
public abstract class WebServerMicroservice implements MasterProcessMicroservice {
  @Nullable @GuardedBy("this")
  protected WebServer mWebServer = null;
  private final AlluxioMasterProcess mMasterProcess;
  protected final ServiceType mServiceType = ServiceType.MASTER_WEB;

  WebServerMicroservice(AlluxioMasterProcess masterProcess) {
    mMasterProcess = masterProcess;
    // configure web address
    int port = NetworkAddressUtils.getPort(mServiceType, Configuration.global());
    if (!ConfigurationUtils.isHaMode(Configuration.global()) && port == 0) {
      throw new RuntimeException(
          String.format("%s port must be nonzero in single-master mode", mServiceType));
    }
    if (port == 0) {
      try (ServerSocket s = new ServerSocket(0)) {
        Configuration.set(mServiceType.getPortKey(), s.getLocalPort());
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  /**
   * @return whether the web server has been created and started
   */
  public synchronized boolean isServing() {
    return mWebServer != null && mWebServer.getServer().isRunning();
  }

  /**
   * @return the web address
   */
  public synchronized InetSocketAddress getAddress() {
    if (mWebServer != null) {
      return new InetSocketAddress(mWebServer.getBindHost(), mWebServer.getLocalPort());
    }
    return NetworkAddressUtils.getConnectAddress(ServiceType.MASTER_WEB, Configuration.global());
  }

  protected WebServer createWebServer() {
    InetSocketAddress address =
        NetworkAddressUtils.getBindAddress(mServiceType, Configuration.global());
    return new MasterWebServer(mServiceType.getServiceName(), address, mMasterProcess);
  }

  @GuardedBy("this")
  protected void startWebServer() {
    mWebServer = createWebServer();
    mWebServer.start();
  }

  @GuardedBy("this")
  protected void stopWebServer() {
    if (mWebServer != null) {
      try {
        mWebServer.stop();
      } catch (Exception e) {
        // ignore
      } finally {
        mWebServer = null;
      }
    }
  }

  /**
   * Creates the web server service that will manager the web server for the Alluxio Master Process.
   * @param masterProcess is required to instantiate a web server
   * @return the {@link MasterProcessMicroservice} that manages the web server
   */
  public static MasterProcessMicroservice create(AlluxioMasterProcess masterProcess) {
    if (Configuration.getBoolean(PropertyKey.STANDBY_MASTER_WEB_ENABLED)) {
      return new AlwaysOnWebServerMicroservice(masterProcess);
    }
    return new DefaultWebServerMicroservice(masterProcess);
  }
}

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

package alluxio.master.microservices.grpc;

import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.exception.runtime.AlluxioRuntimeException;
import alluxio.executor.ExecutorServiceBuilder;
import alluxio.grpc.ErrorType;
import alluxio.grpc.GrpcServer;
import alluxio.grpc.GrpcServerAddress;
import alluxio.grpc.GrpcServerBuilder;
import alluxio.grpc.GrpcService;
import alluxio.grpc.ServiceType;
import alluxio.master.AlluxioExecutorService;
import alluxio.master.SafeModeManager;
import alluxio.master.microservices.MasterProcessMicroservice;
import alluxio.network.RejectingServer;
import alluxio.util.ConfigurationUtils;
import alluxio.util.network.NetworkAddressUtils;

import io.grpc.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

/**
 * Microservice that manages the master's grpc server.
 */
public class GrpcServerMicroservice implements MasterProcessMicroservice {
  private static final Logger LOG = LoggerFactory.getLogger(GrpcServerMicroservice.class);

  private final Map<ServiceType, GrpcService> mGrpcServices;
  private final NetworkAddressUtils.ServiceType mServiceType =
      NetworkAddressUtils.ServiceType.MASTER_RPC;
  private final SafeModeManager mSafeModeManager;
  @Nullable @GuardedBy("this")
  private GrpcServer mGrpcServer = null;
  @Nullable @GuardedBy("this")
  private AlluxioExecutorService mRpcExecutor = null;
  @Nullable @GuardedBy("this")
  private RejectingServer mRejectingGrpcServer = null;

  private GrpcServerMicroservice(Map<ServiceType, GrpcService> grpcServices,
      SafeModeManager safeModeManager) {
    mGrpcServices = grpcServices;
    mSafeModeManager = safeModeManager;
    // configure rpc address
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

  @Override
  public synchronized void start() {
    InetSocketAddress rpcBindAddress =
        NetworkAddressUtils.getBindAddress(mServiceType, Configuration.global());
    mRejectingGrpcServer = new RejectingServer(rpcBindAddress);
    mRejectingGrpcServer.start();
  }

  @Override
  public synchronized void promote() {
    stopRejectingServer();
    mRpcExecutor = createRpcExecutor();
    mGrpcServer = createRpcServer(mRpcExecutor);
    try {
      mGrpcServer.start();
    } catch (IOException e) {
      throw new AlluxioRuntimeException(Status.INTERNAL, "Failed to start gRPC server", e,
          ErrorType.Internal, false);
    }
    mSafeModeManager.notifyRpcServerStarted();
  }

  @Override
  public synchronized void demote() {
    stopGrpcServer();
    stopRpcExecutor();
    start(); // start rejecting server again
  }

  @Override
  public synchronized void stop() {
    stopRejectingServer();
    stopGrpcServer();
    stopRpcExecutor();
  }

  private void stopGrpcServer() {
    if (mGrpcServer != null) {
      mGrpcServer.shutdown();
      mGrpcServer.awaitTermination();
    }
  }

  private void stopRpcExecutor() {
    if (mRpcExecutor != null) {
      mRpcExecutor.shutdown();
      try {
        mRpcExecutor.awaitTermination(
            Configuration.getMs(PropertyKey.NETWORK_CONNECTION_SERVER_SHUTDOWN_TIMEOUT),
            TimeUnit.MILLISECONDS);
      } catch (InterruptedException e) {
        LOG.warn("rpc executor was interrupted while terminating", e);
      }
    }
  }

  private void stopRejectingServer() {
    if (mRejectingGrpcServer != null) {
      mRejectingGrpcServer.stopAndJoin();
      mRejectingGrpcServer = null;
    }
  }
  
  private AlluxioExecutorService createRpcExecutor() {
    return ExecutorServiceBuilder.buildExecutorService(
        ExecutorServiceBuilder.RpcExecutorHost.MASTER);
  }

  private GrpcServer createRpcServer(Executor rpcExecutor) {
    InetSocketAddress bindAddress =
        NetworkAddressUtils.getBindAddress(mServiceType, Configuration.global());
    InetSocketAddress connectAddress =
        NetworkAddressUtils.getConnectAddress(mServiceType, Configuration.global());

    GrpcServerBuilder builder = GrpcServerBuilder
        .forAddress(GrpcServerAddress.create(connectAddress.getHostName(), bindAddress),
            Configuration.global())
        .executor(rpcExecutor)
        .flowControlWindow(
            (int) Configuration.getBytes(PropertyKey.MASTER_NETWORK_FLOWCONTROL_WINDOW))
        .keepAliveTime(
            Configuration.getMs(PropertyKey.MASTER_NETWORK_KEEPALIVE_TIME_MS),
            TimeUnit.MILLISECONDS)
        .keepAliveTimeout(
            Configuration.getMs(PropertyKey.MASTER_NETWORK_KEEPALIVE_TIMEOUT_MS),
            TimeUnit.MILLISECONDS)
        .permitKeepAlive(
            Configuration.getMs(PropertyKey.MASTER_NETWORK_PERMIT_KEEPALIVE_TIME_MS),
            TimeUnit.MILLISECONDS)
        .maxInboundMessageSize((int) Configuration.getBytes(
            PropertyKey.MASTER_NETWORK_MAX_INBOUND_MESSAGE_SIZE));
    // register services
    for (Map.Entry<ServiceType, GrpcService> serviceEntry : mGrpcServices.entrySet()) {
      builder.addService(serviceEntry.getKey(), serviceEntry.getValue());
      LOG.info("registered service {}", serviceEntry.getKey().name());
    }
    return builder.build();
  }

  /**
   * @param grpcServices the services that the grpc server will register
   * @param safeModeManager the safe mode manager
   * @return a microservice that manages the lifecycle of the grpc server
   */
  public static MasterProcessMicroservice create(
      Map<ServiceType, GrpcService> grpcServices,
      SafeModeManager safeModeManager
  ) {
    return new GrpcServerMicroservice(grpcServices, safeModeManager);
  }
}

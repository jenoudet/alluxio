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

package alluxio.master;

import static alluxio.util.network.NetworkAddressUtils.ServiceType;

import alluxio.Constants;
import alluxio.ProcessUtils;
import alluxio.RuntimeConstants;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.executor.ExecutorServiceBuilder;
import alluxio.grpc.GrpcServer;
import alluxio.grpc.GrpcServerAddress;
import alluxio.grpc.GrpcServerBuilder;
import alluxio.grpc.GrpcService;
import alluxio.grpc.JournalDomain;
import alluxio.grpc.NodeState;
import alluxio.master.journal.DefaultJournalMaster;
import alluxio.master.journal.JournalMasterClientServiceHandler;
import alluxio.master.journal.JournalSystem;
import alluxio.master.journal.JournalType;
import alluxio.master.journal.JournalUtils;
import alluxio.master.journal.raft.RaftJournalSystem;
import alluxio.master.journal.raft.RaftPrimarySelector;
import alluxio.master.journal.ufs.UfsJournalSingleMasterPrimarySelector;
import alluxio.master.journal.ufs.UfsJournalSystem;
import alluxio.master.meta.DefaultMetaMaster;
import alluxio.master.meta.MetaMaster;
import alluxio.master.microservices.MasterProcessMicroservice;
import alluxio.master.microservices.journal.JournaledMicroservice;
import alluxio.master.microservices.journal.JournalingMicroservice;
import alluxio.master.microservices.metrics.MetricsSinkMicroservice;
import alluxio.master.microservices.web.WebServerMicroservice;
import alluxio.metrics.MetricKey;
import alluxio.metrics.MetricsSystem;
import alluxio.underfs.MasterUfsManager;
import alluxio.util.ConfigurationUtils;
import alluxio.util.JvmPauseMonitor;
import alluxio.util.ThreadUtils;
import alluxio.util.network.NetworkAddressUtils;

import com.google.common.base.Throwables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.concurrent.NotThreadSafe;
import javax.annotation.concurrent.ThreadSafe;

/**
 * An Alluxio Master which runs a web and rpc server to handle FileSystem operations.
 */
@NotThreadSafe
public class AlluxioMasterProcess extends MasterProcess {
  private static final Logger LOG = LoggerFactory.getLogger(AlluxioMasterProcess.class);

  /** The master registry. */
  protected final MasterRegistry mRegistry;

  /** The JVMMonitor Progress. */
  private JvmPauseMonitor mJvmPauseMonitor;

  /** The connection address for the rpc server. */
  final InetSocketAddress mRpcConnectAddress =
      NetworkAddressUtils.getConnectAddress(ServiceType.MASTER_RPC, Configuration.global());

  /** The manager of safe mode state. */
  protected final SafeModeManager mSafeModeManager = new DefaultSafeModeManager();

  private AlluxioExecutorService mRPCExecutor = null;
  /** See {@link #isStopped()}. */
  protected final AtomicBoolean mIsStopped = new AtomicBoolean(false);

  /** See {@link #isRunning()}. */
  private volatile boolean mRunning = false;

  private final Set<MasterProcessMicroservice> mMasterProcessMicroservices = new HashSet<>();

  /**
   * Creates a new {@link AlluxioMasterProcess}.
   */
  AlluxioMasterProcess(JournalSystem journalSystem, PrimarySelector leaderSelector,
      MasterRegistry registry) {
    super(journalSystem, leaderSelector, ServiceType.MASTER_WEB, ServiceType.MASTER_RPC);
    mRegistry = registry;
    try {
      stopServing();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    LOG.info("New process created.");
  }

  void registerMasterProcessMicroservice(MasterProcessMicroservice masterProcessMicroservice) {
    mMasterProcessMicroservices.add(masterProcessMicroservice);
  }

  @Override
  public <T extends Master> T getMaster(Class<T> clazz) {
    return mRegistry.get(clazz);
  }

  @Override
  public boolean isWebServing() {
    return mMasterProcessMicroservices.stream()
        .anyMatch(microservice -> microservice instanceof WebServerMicroservice
            && ((WebServerMicroservice) microservice).isServing());
  }

  @Override
  public InetSocketAddress getWebAddress() {
    Optional<InetSocketAddress> webAddress = mMasterProcessMicroservices.stream()
        .filter(microservice -> microservice instanceof WebServerMicroservice)
        .map(m -> ((WebServerMicroservice) m).getAddress()).findFirst();
    return webAddress.orElseGet(() -> NetworkAddressUtils.getConnectAddress(ServiceType.MASTER_WEB,
        Configuration.global()));
  }

  @Override
  public InetSocketAddress getRpcAddress() {
    return mRpcConnectAddress;
  }

  @Override
  public void start() throws Exception {
    LOG.info("Process starting.");
    mRunning = true;
    mMasterProcessMicroservices.forEach(MasterProcessMicroservice::start);
    startRejectingRpcServer();
    startJvmMonitorProcess();

    LOG.info("Starting leader selector.");
    mLeaderSelector.start(getRpcAddress());

    while (!Thread.interrupted()) {
      if (!mRunning) {
        LOG.info("master process is not running. Breaking out");
        break;
      }

      LOG.info("Started in stand-by mode.");
      mLeaderSelector.waitForState(NodeState.PRIMARY);
      if (!mRunning) {
        break;
      }
      mMasterProcessMicroservices.forEach(MasterProcessMicroservice::promote);
      if (!gainPrimacy()) {
        continue;
      }
      mLeaderSelector.waitForState(NodeState.STANDBY);
      if (Configuration.getBoolean(PropertyKey.MASTER_JOURNAL_EXIT_ON_DEMOTION)) {
        stop();
      } else {
        if (!mRunning) {
          break;
        }
        mMasterProcessMicroservices.forEach(MasterProcessMicroservice::demote);
        losePrimacy();
      }
    }
  }

  /**
   * Upgrades the master to primary mode.
   *
   * If the master loses primacy during the journal upgrade, this method will clean up the partial
   * upgrade, leaving the master in standby mode.
   *
   * @return whether the master successfully upgraded to primary
   */
  private boolean gainPrimacy() {
    LOG.info("Becoming a leader.");
    mServingThread = new Thread(() -> {
      try {
        startMasterServing(" (gained leadership)", " (lost leadership)");
      } catch (Throwable t) {
        Throwable root = Throwables.getRootCause(t);
        if (root instanceof InterruptedException || Thread.interrupted()) {
          return;
        }
        ProcessUtils.fatalError(LOG, t, "Exception thrown in main serving thread");
      }
    }, "MasterServingThread");
    LOG.info("Starting a server thread.");
    mServingThread.start();
    if (!waitForReady(10 * Constants.MINUTE_MS)) {
      ThreadUtils.logAllThreads();
      throw new RuntimeException("Alluxio master failed to come up");
    }
    LOG.info("Primary started");
    return true;
  }

  private void losePrimacy() throws Exception {
    LOG.info("Losing the leadership.");
    if (mServingThread != null) {
      stopServingGrpc();
    }
    if (mServingThread != null) {
      mServingThread.join(mServingThreadTimeoutMs);
      if (mServingThread.isAlive()) {
        ProcessUtils.fatalError(LOG,
            "Failed to stop serving thread after %dms. Serving thread stack trace:%n%s",
            mServingThreadTimeoutMs, ThreadUtils.formatStackTrace(mServingThread));
      }
      mServingThread = null;
      LOG.info("Primary stopped");
    }
    startRejectingRpcServer();
    LOG.info("Standby started");
  }

  protected void stopCommonServices() {
    stopRejectingRpcServer();
    stopServing();
  }

  /**
   * Starts jvm monitor process, to monitor jvm.
   */
  protected void startJvmMonitorProcess() {
    if (Configuration.getBoolean(PropertyKey.MASTER_JVM_MONITOR_ENABLED)) {
      mJvmPauseMonitor = new JvmPauseMonitor(
          Configuration.getMs(PropertyKey.JVM_MONITOR_SLEEP_INTERVAL_MS),
          Configuration.getMs(PropertyKey.JVM_MONITOR_WARN_THRESHOLD_MS),
          Configuration.getMs(PropertyKey.JVM_MONITOR_INFO_THRESHOLD_MS));
      mJvmPauseMonitor.start();
      MetricsSystem.registerGaugeIfAbsent(
              MetricsSystem.getMetricName(MetricKey.TOTAL_EXTRA_TIME.getName()),
              mJvmPauseMonitor::getTotalExtraTime);
      MetricsSystem.registerGaugeIfAbsent(
              MetricsSystem.getMetricName(MetricKey.INFO_TIME_EXCEEDED.getName()),
              mJvmPauseMonitor::getInfoTimeExceeded);
      MetricsSystem.registerGaugeIfAbsent(
              MetricsSystem.getMetricName(MetricKey.WARN_TIME_EXCEEDED.getName()),
              mJvmPauseMonitor::getWarnTimeExceeded);
    }
  }

  /**
   * Starts serving, letting {@link MetricsSystem} start sink and starting the web ui server and RPC
   * Server.
   *
   * @param startMessage empty string or the message that the master gains the leadership
   * @param stopMessage empty string or the message that the master loses the leadership
   */
  @Override
  protected void startServing(String startMessage, String stopMessage) {
    // start all common services for non-ha master or leader master
    startJvmMonitorProcess();
    startMasterServing(startMessage, stopMessage);
  }

  protected void startMasterServing(String startMessage, String stopMessage) {
    startServingRPCServer();
    LOG.info(
        "Alluxio {} master version {} started{}. bindAddress={}, connectAddress={}, webAddress={}",
        mLeaderSelector.getState().name(), RuntimeConstants.VERSION, startMessage, mRpcBindAddress,
        mRpcConnectAddress, mWebBindAddress);
    // Blocks until RPC server is shut down. (via #stopServing)
    mGrpcServer.awaitTermination();
    LOG.info("Alluxio master ended {}", stopMessage);
  }

  @Override
  public void stop() throws Exception {
    synchronized (mIsStopped) {
      if (mIsStopped.get()) {
        return;
      }
      LOG.info("Stopping...");
      mRunning = false;
      mMasterProcessMicroservices.forEach(MasterProcessMicroservice::stop);
      stopCommonServices();
      if (mLeaderSelector != null) {
        mLeaderSelector.stop();
      }
      mIsStopped.set(true);
      LOG.info("Stopped.");
    }
  }

  /**
   * @return {@code true} when {@link #start()} has been called and {@link #stop()} has not yet
   * been called, {@code false} otherwise
   */
  boolean isRunning() {
    return mRunning;
  }

  /**
   * Starts the gRPC server. The AlluxioMaster registers the Services of registered
   * {@link Master}s and meta services.
   */
  protected void startServingRPCServer() {
    stopRejectingRpcServer();
    try {
      synchronized (mGrpcServerLock) {
        LOG.info("Starting gRPC server on address:{}", mRpcBindAddress);
        mGrpcServer = createRPCServer();
        // Start serving.
        mGrpcServer.start();
        mSafeModeManager.notifyRpcServerStarted();
        // Acquire and log bind port from newly started server.
        InetSocketAddress listeningAddress = InetSocketAddress
            .createUnresolved(mRpcBindAddress.getHostName(), mGrpcServer.getBindPort());
        LOG.info("gRPC server listening on: {}", listeningAddress);
      }
    } catch (IOException e) {
      LOG.error("gRPC serving failed.", e);
      throw new RuntimeException("gRPC serving failed");
    }
  }

  private GrpcServer createRPCServer() {
    // Create an executor for Master RPC server.
    mRPCExecutor = ExecutorServiceBuilder.buildExecutorService(
        ExecutorServiceBuilder.RpcExecutorHost.MASTER);
    MetricsSystem.registerGaugeIfAbsent(MetricKey.MASTER_RPC_QUEUE_LENGTH.getName(),
        mRPCExecutor::getRpcQueueLength);
    MetricsSystem.registerGaugeIfAbsent(MetricKey.MASTER_RPC_THREAD_ACTIVE_COUNT.getName(),
        mRPCExecutor::getActiveCount);
    MetricsSystem.registerGaugeIfAbsent(MetricKey.MASTER_RPC_THREAD_CURRENT_COUNT.getName(),
        mRPCExecutor::getPoolSize);
    // Create underlying gRPC server.
    GrpcServerBuilder builder = GrpcServerBuilder
        .forAddress(GrpcServerAddress.create(mRpcConnectAddress.getHostName(), mRpcBindAddress),
            Configuration.global())
        .executor(mRPCExecutor)
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
    addGrpcServices(builder);
    // Builds a server that is not started yet.
    return builder.build();
  }

  protected void addGrpcServices(GrpcServerBuilder builder) {
    // Bind manifests of each Alluxio master to RPC server.
    for (Master master : mRegistry.getServers()) {
      registerServices(builder, master.getServices());
    }
    // Bind manifest of Alluxio JournalMaster service.
    // TODO(ggezer) Merge this with registerServices() logic.
    builder.addService(alluxio.grpc.ServiceType.JOURNAL_MASTER_CLIENT_SERVICE,
        new GrpcService(new JournalMasterClientServiceHandler(
            new DefaultJournalMaster(JournalDomain.MASTER, mJournalSystem, mLeaderSelector))));
  }

  protected void stopServingGrpc() {
    synchronized (mGrpcServerLock) {
      if (mGrpcServer != null && mGrpcServer.isServing() && !mGrpcServer.shutdown()) {
        LOG.warn("Alluxio master RPC server shutdown timed out.");
      }
    }
    if (mRPCExecutor != null) {
      mRPCExecutor.shutdownNow();
      try {
        mRPCExecutor.awaitTermination(
            Configuration.getMs(PropertyKey.NETWORK_CONNECTION_SERVER_SHUTDOWN_TIMEOUT),
            TimeUnit.MILLISECONDS);
      } catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
      }
    }
  }

  /**
   * Stops all services.
   */
  protected void stopServing() {
    stopServingGrpc();
    // stop JVM monitor process
    if (mJvmPauseMonitor != null) {
      mJvmPauseMonitor.stop();
    }
  }

  /**
   * Indicates if all master resources have been successfully released when stopping.
   * An assumption made here is that a first call to {@link #stop()} might fail while a second call
   * might succeed.
   * @return whether {@link #stop()} has concluded successfully at least once
   */
  public boolean isStopped() {
    return mIsStopped.get();
  }

  @Override
  public String toString() {
    return "Alluxio master @" + mRpcConnectAddress;
  }

  /**
   * Factory for creating {@link AlluxioMasterProcess}.
   */
  @ThreadSafe
  public static final class Factory {
    /**
     * Creates a new {@link AlluxioMasterProcess}.
     *
     * @return a new instance of {@link MasterProcess} using the given sockets for the master
     */
    public static AlluxioMasterProcess create() {
      PrimarySelector selector;
      JournalSystem journalSystem;
      // create primary selector and journal system based on static configuration
      URI journalLocation = JournalUtils.getJournalLocation();
      long quietTimeMs =
          Configuration.getMs(PropertyKey.MASTER_JOURNAL_TAILER_SHUTDOWN_QUIET_WAIT_TIME_MS);
      if (Configuration.getEnum(PropertyKey.MASTER_JOURNAL_TYPE, JournalType.class)
          == JournalType.EMBEDDED) {
        selector = new RaftPrimarySelector();
        InetSocketAddress localAddress =
            NetworkAddressUtils.getConnectAddress(ServiceType.MASTER_RAFT, Configuration.global());
        List<InetSocketAddress> clusterAddresses =
            ConfigurationUtils.getEmbeddedJournalAddresses(Configuration.global(),
                ServiceType.MASTER_RAFT);
        journalSystem = new RaftJournalSystem(journalLocation, localAddress, clusterAddresses,
            (RaftPrimarySelector) selector);
      } else if (Configuration.getBoolean(PropertyKey.ZOOKEEPER_ENABLED)) {
        selector = PrimarySelector.Factory.createZkPrimarySelector();
        journalSystem = new UfsJournalSystem(journalLocation, quietTimeMs);
      } else {
        selector = new UfsJournalSingleMasterPrimarySelector();
        journalSystem = new UfsJournalSystem(journalLocation, quietTimeMs);
      }
      MasterRegistry masterRegistry = new MasterRegistry();

      AlluxioMasterProcess process = new AlluxioMasterProcess(journalSystem, selector,
          masterRegistry);

      // create the masters
      BackupManager backupManager = new BackupManager(masterRegistry);
      MasterUfsManager ufsManager = new MasterUfsManager();
      String baseDir = Configuration.getString(PropertyKey.MASTER_METASTORE_DIR);
      CoreMasterContext context = CoreMasterContext.newBuilder()
          .setJournalSystem(journalSystem)
          .setSafeModeManager(new DefaultSafeModeManager())
          .setBackupManager(backupManager)
          .setBlockStoreFactory(MasterUtils.getBlockStoreFactory(baseDir))
          .setInodeStoreFactory(MasterUtils.getInodeStoreFactory(baseDir))
          .setStartTimeMs(process.getStartTimeMs())
          .setPort(NetworkAddressUtils.getPort(ServiceType.MASTER_RPC, Configuration.global()))
          .setUfsManager(ufsManager)
          .build();
      MasterUtils.createMasters(masterRegistry, context);
      DefaultMetaMaster metaMaster = (DefaultMetaMaster) masterRegistry.get(MetaMaster.class);

      MasterProcessMicroservice journaledMicroservice =
          JournaledMicroservice.create(journalSystem, masterRegistry, backupManager, ufsManager,
              context);
      process.registerMasterProcessMicroservice(journaledMicroservice);
      MasterProcessMicroservice journalingMicroservice =
          JournalingMicroservice.create(journalSystem, metaMaster);
      process.registerMasterProcessMicroservice(journalingMicroservice);
      MasterProcessMicroservice webMicroservice = WebServerMicroservice.create(process);
      process.registerMasterProcessMicroservice(webMicroservice);
      MasterProcessMicroservice metricsSinkMicroservice = MetricsSinkMicroservice.create();
      process.registerMasterProcessMicroservice(metricsSinkMicroservice);

      return process;
    }

    private Factory() {} // prevent instantiation
  }
}

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

import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
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
import alluxio.master.microservices.grpc.GrpcServerMicroservice;
import alluxio.master.microservices.journal.JournaledMicroservice;
import alluxio.master.microservices.journal.JournalingMicroservice;
import alluxio.master.microservices.metrics.MetricsSinkMicroservice;
import alluxio.master.microservices.web.WebServerMicroservice;
import alluxio.metrics.MetricKey;
import alluxio.metrics.MetricsSystem;
import alluxio.underfs.MasterUfsManager;
import alluxio.util.ConfigurationUtils;
import alluxio.util.JvmPauseMonitor;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.util.network.NetworkAddressUtils.ServiceType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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

  /** See {@link #isStopped()}. */
  protected final AtomicBoolean mIsStopped = new AtomicBoolean(false);

  /** See {@link #isRunning()}. */
  private volatile boolean mRunning = false;

  private final List<MasterProcessMicroservice> mMasterProcessMicroservices = new ArrayList<>();

  /**
   * Creates a new {@link AlluxioMasterProcess}.
   */
  AlluxioMasterProcess(JournalSystem journalSystem, PrimarySelector leaderSelector,
      MasterRegistry registry) {
    super(journalSystem, leaderSelector, ServiceType.MASTER_WEB, ServiceType.MASTER_RPC);
    mRegistry = registry;
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
  void startServing(String startMessage, String stopMessage) {}

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
      mLeaderSelector.waitForState(NodeState.STANDBY);
      if (Configuration.getBoolean(PropertyKey.MASTER_JOURNAL_EXIT_ON_DEMOTION)) {
        stop();
      } else {
        if (!mRunning) {
          break;
        }
        mMasterProcessMicroservices.forEach(MasterProcessMicroservice::demote);
      }
    }
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

  @Override
  public void stop() throws Exception {
    synchronized (mIsStopped) {
      if (mIsStopped.get()) {
        return;
      }
      LOG.info("Stopping...");
      mRunning = false;
      mMasterProcessMicroservices.forEach(MasterProcessMicroservice::stop);
      stopJvmMonitorProcess();
      mLeaderSelector.stop();
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
   * Stops all services.
   */
  protected void stopJvmMonitorProcess() {
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
      SafeModeManager safeModeManager = new DefaultSafeModeManager();
      String baseDir = Configuration.getString(PropertyKey.MASTER_METASTORE_DIR);
      CoreMasterContext context = CoreMasterContext.newBuilder()
          .setJournalSystem(journalSystem)
          .setSafeModeManager(safeModeManager)
          .setBackupManager(backupManager)
          .setBlockStoreFactory(MasterUtils.getBlockStoreFactory(baseDir))
          .setInodeStoreFactory(MasterUtils.getInodeStoreFactory(baseDir))
          .setStartTimeMs(process.getStartTimeMs())
          .setPort(NetworkAddressUtils.getPort(ServiceType.MASTER_RPC, Configuration.global()))
          .setUfsManager(ufsManager)
          .build();
      MasterUtils.createMasters(masterRegistry, context);
      DefaultMetaMaster metaMaster = (DefaultMetaMaster) masterRegistry.get(MetaMaster.class);

      Map<alluxio.grpc.ServiceType, GrpcService> grpcServices = new HashMap<>();
      for (Master master : masterRegistry.getServers()) {
        grpcServices.putAll(master.getServices());
      }
      grpcServices.put(alluxio.grpc.ServiceType.JOURNAL_MASTER_CLIENT_SERVICE,
          new GrpcService(new JournalMasterClientServiceHandler(
              new DefaultJournalMaster(JournalDomain.MASTER, journalSystem, selector))));

      MasterProcessMicroservice journaledMicroservice =
          JournaledMicroservice.create(journalSystem, masterRegistry, backupManager,
              safeModeManager, ufsManager, context);
      process.registerMasterProcessMicroservice(journaledMicroservice);
      MasterProcessMicroservice journalingMicroservice =
          JournalingMicroservice.create(journalSystem, metaMaster);
      process.registerMasterProcessMicroservice(journalingMicroservice);
      MasterProcessMicroservice grpcServerMicroservice = GrpcServerMicroservice.create(grpcServices,
          safeModeManager);
      process.registerMasterProcessMicroservice(grpcServerMicroservice);
      MasterProcessMicroservice webMicroservice = WebServerMicroservice.create(process);
      process.registerMasterProcessMicroservice(webMicroservice);
      MasterProcessMicroservice metricsSinkMicroservice = MetricsSinkMicroservice.create();
      process.registerMasterProcessMicroservice(metricsSinkMicroservice);

      return process;
    }

    private Factory() {} // prevent instantiation
  }
}

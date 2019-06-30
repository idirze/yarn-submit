package com.dirze.yarn.submit;

import com.google.common.base.Strings;
import com.google.common.util.concurrent.AtomicDouble;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.ShutdownHookManager;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.NMClient;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.apache.hadoop.yarn.util.ConverterUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public class ApplicationMaster {

  // The thread bounds for launching containers.
  private static final int MIN_EXECUTOR_THREADS = 0;
  private static final int MAX_EXECUTOR_THREADS = 25;

  // Exit codes.
  private static final int EXIT_OK = 0;
  private static final int EXIT_MASTER_FAILURE = 10;
  private static final int EXIT_DRIVER_FAILURE = 11;
  private static final int EXIT_SERVICE_FAILURE = 12;

  private final Configuration conf = new YarnConfiguration();

  // Heartbeat intervals between the AM and RM
  private final long idleHeartbeat = Math.min(
      5000,
      Math.max(
        0,
        conf.getInt(YarnConfiguration.RM_AM_EXPIRY_INTERVAL_MS,
                    YarnConfiguration.DEFAULT_RM_AM_EXPIRY_INTERVAL_MS) / 2
      )
  );
  private final long pendingHeartbeat = Math.min(1000, idleHeartbeat);

  private Model.ApplicationSpec spec;
  private ByteBuffer tokens;

  private Path appDir;

  private ApplicationId appId;
  private String userName;
  private ContainerId containerId;
  private Resource amResources;

  private final TreeMap<String, Msg.KeyValue.Builder> keyValueStore = new TreeMap<>();

  private final Map<String, ServiceTracker> services =
      new HashMap<String, ServiceTracker>();
  private final Map<ContainerId, Model.Container> containers =
      new ConcurrentHashMap<ContainerId, Model.Container>();

  // Set to negative to indicate hasn't been set by user
  private final AtomicDouble progress = new AtomicDouble(-1);
  private final AtomicDouble totalMemory = new AtomicDouble(0);
  private final AtomicInteger totalVcores = new AtomicInteger(0);

  private final TreeMap<Priority, ServiceTracker> priorities =
      new TreeMap<Priority, ServiceTracker>();
  private int nextPriority = 1;

  private String hostname;

  private FileSystem fs;
  private AMRMClient<ContainerRequest> rmClient;
  private NMClient nmClient;
  private ThreadPoolExecutor containerLaunchExecutor;
  private Thread allocatorThread;
  private Process driverProcess;
  private Thread driverThread;

  // Flags/attributes for shutdown procedure
  private final Object shutdownLock = new Object();
  private boolean appRegistered = false;
  private boolean appFinished = false;
  // The defaults should never get used unless there's a bug
  private int exitCode = EXIT_MASTER_FAILURE;
  private FinalApplicationStatus finalStatus = FinalApplicationStatus.FAILED;
  private String finalMessage = ("Application master shutdown unexpectedly, "
                                 + "see logs for more information");

  /** Main entrypoint for the ApplicationMaster. **/
  public static void main(String[] args) {
    log.info("Starting yarnSubmit version {}", Utils.getYarnSubmitVersion());

    // Specify the netty native workdir. This is necessary for systems where
    // `/tmp` is not executable.
    Utils.configureNettyNativeWorkDir();

    ApplicationMaster appMaster = new ApplicationMaster();

    appMaster.init(args);

    System.exit(appMaster.run());
  }

  public void init(String[] args) {
    if (args.length != 1) {
      log.error("Usage: <command> applicationDirectory");
      System.exit(1);
    }
    appDir = new Path(args[0]);
    String appIdEnv = System.getenv("YARN_SUBMIT_APPLICATION_ID");
    if (appIdEnv == null) {
      log.error("Couldn't find 'YARN_SUBMIT_APPLICATION_ID' envar");
      System.exit(1);
    }
    appId = Utils.appIdFromString(appIdEnv);

    containerId = ConverterUtils.toContainerId(
        System.getenv(Environment.CONTAINER_ID.name())
    );

    hostname = System.getenv(Environment.NM_HOST.name());
  }

  public int run() {
    try {
      userName = UserGroupInformation.getCurrentUser().getUserName();
      log.info("Running as user {}", userName);

      loadApplicationSpec();
      loadDelegationTokens();

      registerShutdownHook();

      startClients();

      log.info("Registering application with resource manager");
      synchronized (shutdownLock) {
        rmClient.registerApplicationMaster(
            hostname, 0,""
        );
        appRegistered = true;
      }

      // Determine the actual memory/vcores allocated to *this* container. We
      // need to do this here, after the application is already registered
      lookupAppMasterResources();

      // Start services
      for (ServiceTracker tracker: services.values()) {
        tracker.initialize();
      }

      // Start allocator loop
      startAllocator();

      // Start application driver (if applicable)
      startApplicationDriver();

      // Block on allocator thread until shutdown
      allocatorThread.join();
    } catch (Exception exc) {
      shutdown(FinalApplicationStatus.FAILED,
               "Unexpected error in application master",
               exc,
               EXIT_MASTER_FAILURE);
    }
    return exitCode;
  }

  private void loadApplicationSpec() throws Exception {
    spec = MsgUtils.readApplicationSpec(
        Msg.ApplicationSpec.parseFrom(new FileInputStream(".yarnSubmit.proto")));
    spec.validate();

    // Setup service trackers
    for (Map.Entry<String, Model.Service> entry : spec.getServices().entrySet()) {
      services.put(entry.getKey(),
          new ServiceTracker(entry.getKey(), entry.getValue()));
    }

    log.info("Application specification successfully loaded");
  }

  private void loadDelegationTokens() throws IOException {
    // Remove the AM->RM token
    Credentials credentials = UserGroupInformation.getCurrentUser().getCredentials();
    Iterator<Token<?>> iter = credentials.getAllTokens().iterator();
    while (iter.hasNext()) {
      Token<?> token = iter.next();
      if (token.getKind().equals(AMRMTokenIdentifier.KIND_NAME)) {
        iter.remove();
      }
    }
    DataOutputBuffer dob = new DataOutputBuffer();
    credentials.writeTokenStorageToStream(dob);
    tokens = ByteBuffer.wrap(dob.getData(), 0, dob.getLength());
  }

  private void startClients() throws IOException {
    fs = FileSystem.get(conf);

    rmClient = AMRMClient.createAMRMClient();
    rmClient.init(conf);
    rmClient.start();

    nmClient = NMClient.createNMClient();
    nmClient.init(conf);
    nmClient.start();

    containerLaunchExecutor = Utils.newThreadPoolExecutor(
        "container-launch-executor",
        MIN_EXECUTOR_THREADS,
        MAX_EXECUTOR_THREADS,
        true);
  }


  private void startAllocator() {
    log.info("Starting allocator thread");
    log.info("Heartbeat intervals [idle: {} ms, pending: {} ms]",
              idleHeartbeat, pendingHeartbeat);
    allocatorThread =
      new Thread() {
        public void run() {
          while (true) {
            try {
              long start = System.currentTimeMillis();
              allocate();
              // Check after allocation to cut sleep time from shutdown
              if (appFinished) {
                break;
              }
              long interval = (priorities.size() > 0) ? pendingHeartbeat : idleHeartbeat;
              long left = interval - (System.currentTimeMillis() - start);
              if (left > 0) {
                Thread.sleep(left);
              }
            } catch (InterruptedException exc) {
              break;
            } catch (Exception exc) {
              shutdown(FinalApplicationStatus.FAILED,
                       "Failure in container allocator",
                       exc,
                       EXIT_MASTER_FAILURE);
              break;
            }
          }
        }
      };
    allocatorThread.setDaemon(true);
    allocatorThread.start();
  }

  private void stopAllocator() {
    if (allocatorThread != null && !Thread.currentThread().equals(allocatorThread)) {
      log.debug("Stopping allocator thread");
      allocatorThread.interrupt();
    }
  }

  private void startApplicationDriver() throws IOException {
    Model.Master master = spec.getMaster();
    if (master.getScript().isEmpty()) {
      // Nothing to do
      return;
    }
    log.debug("Writing driver script...");
    Utils.stringToFile(master.getScript(), new FileOutputStream(".yarnSubmit.sh"));
    String logdir = System.getProperty("yarnSubmit.log.directory");
    ProcessBuilder pb = new ProcessBuilder()
        .command("bash", ".yarnSubmit.sh")
        .redirectErrorStream(true)
        .redirectOutput(new File(logdir, "application.driver.log"));
    updateServiceEnvironment(pb.environment(), amResources, null);
    pb.environment().remove("CLASSPATH");

    // Start the driver process
    log.info("Starting application driver");
    driverProcess = pb.start();

    // Set up a thread to manage the driver process
    driverThread =
      new Thread() {
        public void run() {
          // Wait for it to finish
          int exitValue;
          try {
            exitValue = driverProcess.waitFor();
          } catch (InterruptedException exc) {
            // Interrupted during shutdown, just cleanup process
            driverProcess.destroy();
            return;
          }
          if (exitValue == 0) {
            shutdown(FinalApplicationStatus.SUCCEEDED,
                     "Application driver completed successfully.",
                     EXIT_OK);
          } else if (exitValue == 143) {
            // SIGTERM results in exit code 143
            shutdown(FinalApplicationStatus.FAILED,
                     "Application driver failed with exit code 143. "
                     + "This is often due to the application master "
                     + "memory limit being exceeded. See the "
                     + "diagnostics for more information.",
                     EXIT_DRIVER_FAILURE);
          } else {
            shutdown(FinalApplicationStatus.FAILED,
                     "Application driver failed with exit code "
                     + exitValue + ", see logs for more information.",
                     EXIT_DRIVER_FAILURE);
          }
        }
      };
    driverThread.setDaemon(true);
    driverThread.start();
  }

  private void stopApplicationDriver() {
    if (driverThread != null && !Thread.currentThread().equals(driverThread)) {
      log.info("Stopping application driver");
      driverThread.interrupt();
    }
  }

  private void runOnExit() {
    int maxAttempts = Math.min(
        spec.getMaxAttempts(),
        conf.getInt(YarnConfiguration.RM_AM_MAX_ATTEMPTS,
                    YarnConfiguration.DEFAULT_RM_AM_MAX_ATTEMPTS)
    );
    int currentAttempt = containerId.getApplicationAttemptId().getAttemptId();

    synchronized (shutdownLock) {
      if (!appFinished) {
        // Shutdown hook called due to external mechanism (likely killed by a
        // signal). Set the message and log the reason.
        appFinished = true;
        finalStatus = FinalApplicationStatus.FAILED;
        finalMessage = ("Application master shutdown by external signal. "
                        + "This usually means that the application was killed "
                        + "by a user/administrator, or that the application "
                        + "master memory limit was exceeded. See the "
                        + "diagnostics for more information.");
        exitCode = EXIT_MASTER_FAILURE;
        log.warn(finalMessage);
      }
      if (finalStatus == FinalApplicationStatus.SUCCEEDED || currentAttempt >= maxAttempts) {
        // Unregister the application
        if (appRegistered) {
          try {
            log.info("Unregistering application with status {}", finalStatus);
            rmClient.unregisterApplicationMaster(finalStatus, finalMessage, null);
          } catch (Exception ex) {
            log.error("Failed to unregister application", ex);
          }
        }
        // Attempt to delete the app directory
        if (fs == null) {
          log.warn("Shutdown before filesystem connected, failed to delete "
                  + "application directory {}", appDir);
        } else {
          try {
            if (fs.delete(appDir, true)) {
              log.info("Deleted application directory {}", appDir);
            }
          } catch (IOException exc) {
            log.warn("Failed to delete application directory {}", appDir, exc);
          }
        }
      } else {
        log.info("Application attempt {} out of {} failed, will retry",
                currentAttempt, maxAttempts);
      }
    }
  }

  private void registerShutdownHook() {
    // We register with a higher priority than FileSystem to ensure that the
    // hdfs client is still active
    ShutdownHookManager.get().addShutdownHook(
        new Runnable() {
          @Override
          public void run() {
            runOnExit();
          }
        },
        FileSystem.SHUTDOWN_HOOK_PRIORITY + 30);
  }

  private void shutdown(FinalApplicationStatus status, String msg,
      Exception exc, int code) {
    synchronized (shutdownLock) {
      startShutdown(status, msg, exc, code);
      finishShutdown();
    }
  }

  private void shutdown(FinalApplicationStatus status, String msg, int code) {
    shutdown(status, msg, null, code);
  }

  private void startShutdown(FinalApplicationStatus status, String msg,
      Exception exc, int code) {
    synchronized (shutdownLock) {
      // If shutdown has't already been called
      if (!appFinished) {
        if (exc != null) {
          // If the application terminated with an exception, log it as an
          // error, and add the exception to the final message.
          log.error("Shutting down: {}", msg, exc);
          msg = String.format("%s:\n%s", msg, exc.getMessage());
        } else {
          log.info("Shutting down: {}", msg);
        }
        appFinished = true;
        finalStatus = status;
        finalMessage = msg;
        exitCode = code;
      }
    }
  }

  private void finishShutdown() {
    synchronized (shutdownLock) {
      stopApplicationDriver();
      stopAllocator();
    }
  }

  private Priority newPriority(ServiceTracker tracker) {
    synchronized (priorities) {
      Priority priority = Priority.newInstance(nextPriority);
      nextPriority += 1;
      priorities.put(priority, tracker);
      return priority;
    }
  }

  private ServiceTracker trackerFromPriority(Priority priority) {
    synchronized (priorities) {
      return priorities.get(priority);
    }
  }

  private void removePriority(Priority priority) {
    synchronized (priorities) {
      priorities.remove(priority);
    }
  }

  private void updatePriorities() {
    // Store the next priority for fast access between allocation cycles.
    synchronized (priorities) {
      if (priorities.size() > 0) {
        nextPriority = priorities.lastKey().getPriority() + 1;
      } else {
        nextPriority = 1;
      }
    }
  }

  private void allocate() throws IOException, YarnException {
    // If the user hasn't set the progress, set it to started but not far along.
    float prog = progress.floatValue();
    AllocateResponse resp = rmClient.allocate(prog < 0 ? 0.1f : prog);

    List<Container> allocated = resp.getAllocatedContainers();
    List<ContainerStatus> completed = resp.getCompletedContainersStatuses();

    log.info("Number of containers -> allocated: {}, completed: {}, ResponseId: {}", allocated.size(), completed.size(), resp.getResponseId());
    
    if (allocated.size() > 0) {
      handleAllocated(allocated);
    }

    if (completed.size() > 0) {
      handleCompleted(completed);
    }

    if (allocated.size() > 0 || completed.size() > 0) {
      updatePriorities();
    }
  }

  private void handleAllocated(List<Container> newContainers) {
    log.debug("Received {} new containers", newContainers.size());

    for (Container c : newContainers) {
      ServiceTracker tracker = trackerFromPriority(c.getPriority());
      if (tracker != null) {
        tracker.handleNewContainer(c);
      } else {
        log.debug("Releasing {} with priority {} due to canceled request",
                  c.getId(), c.getPriority());
        rmClient.releaseAssignedContainer(c.getId());
      }
    }
  }

  private void handleCompleted(List<ContainerStatus> containerStatuses) {
    log.debug("Received {} completed containers", containerStatuses.size());

    for (ContainerStatus status : containerStatuses) {
      Model.Container container = containers.get(status.getContainerId());
      if (container == null) {
        // released container that was never started
        log.debug("{} was released without ever starting, nothing to do",
                  status.getContainerId());
        continue;
      }

      Model.Container.State state;
      String exitMessage;

      switch (status.getExitStatus()) {
        case ContainerExitStatus.KILLED_BY_APPMASTER:
          return;  // state change already handled by killContainer
        case ContainerExitStatus.SUCCESS:
          state = Model.Container.State.SUCCEEDED;
          exitMessage = "Completed successfully.";
          break;
        case ContainerExitStatus.KILLED_EXCEEDED_PMEM:
          state = Model.Container.State.FAILED;
          exitMessage = Utils.formatExceededMemMessage(
              status.getDiagnostics(),
              Utils.EXCEEDED_PMEM_PATTERN);
          break;
        case ContainerExitStatus.KILLED_EXCEEDED_VMEM:
          state = Model.Container.State.FAILED;
          exitMessage = Utils.formatExceededMemMessage(
              status.getDiagnostics(),
              Utils.EXCEEDED_VMEM_PATTERN);
          break;
        default:
          state = Model.Container.State.FAILED;
          if (status.getExitStatus() > 0) {
            // Positive error codes indicate service failure
            exitMessage = "Container failed during execution, see logs for more information.";
          } else {
            exitMessage = status.getDiagnostics();
          }
          break;
      }

      services.get(container.getServiceName())
              .finishContainer(container.getInstance(), state, exitMessage);
    }
  }

  private void maybeShutdown() {
    // Fail if any service is failed
    // Succeed if no driver, all services are finished, and none failed
    boolean finished = (driverProcess == null);
    for (ServiceTracker tracker : services.values()) {
      finished &= tracker.isFinished();
      if (tracker.isFailed()) {
        shutdown(FinalApplicationStatus.FAILED,
                 "Failure in service " + tracker.getName()
                 + ", see logs for more information.",
                 EXIT_SERVICE_FAILURE);
        return;
      }
    }
    if (finished) {
      shutdown(FinalApplicationStatus.SUCCEEDED,
               "Application completed successfully.",
               EXIT_OK);
    }
  }

  private void lookupAppMasterResources() throws IOException, YarnException {
    log.debug("Determining resources available for application master");
    YarnClient yarnClient = YarnClient.createYarnClient();
    yarnClient.init(conf);
    yarnClient.start();
    amResources = yarnClient
      .getApplicationReport(appId)
      .getApplicationResourceUsageReport()
      .getUsedResources();
    yarnClient.stop();

    totalMemory.addAndGet(amResources.getMemory());
    totalVcores.addAndGet(amResources.getVirtualCores());
  }

  private void updateServiceEnvironment(Map<String, String> env, Resource resource,
      String containerId) {
    env.put("YARN_SUBMIT_APPLICATION_ID", appId.toString());
    env.put("YARN_SUBMIT_RESOURCE_VCORES", String.valueOf(resource.getVirtualCores()));
    env.put("YARN_SUBMIT_RESOURCE_MEMORY", String.valueOf(resource.getMemory()));
    if (containerId != null) {
      env.put("YARN_SUBMIT_CONTAINER_ID", containerId);
    }
  }


  final class ServiceTracker {
    private String name;
    private Model.Service service;
    private boolean initialRunning = false;
    private final Set<Integer> waiting = new LinkedHashSet<Integer>();
    // An ordered map of priority -> container. Earlier entries are older
    // requests. The priority is the same as container.req.getPriority().
    private final TreeMap<Priority, Model.Container> requested =
        new TreeMap<Priority, Model.Container>();
    private final Set<Integer> running = new LinkedHashSet<Integer>();
    private final List<Model.Container> containers = new ArrayList<Model.Container>();
    private int numTarget = 0;
    private int numSucceeded = 0;
    private int numFailed = 0;
    private int numKilled = 0;
    private int numRestarted = 0;

    public ServiceTracker(String name, Model.Service service) {
      this.name = name;
      this.service = service;
      this.numTarget = service.getInstances();
    }

    public String getName() {
      return name;
    }

    public synchronized int getNumActive() {
      return waiting.size() + requested.size() + running.size();
    }

    public boolean isFinished() {
      return getNumActive() == 0;
    }

    public synchronized boolean isFailed() {
      return numFailed > numRestarted && !service.getAllowFailures();
    }

    public synchronized boolean addOwnedKey(int instance, String key) {
      Model.Container container = getContainer(instance);
      assert container != null;  // pre-checked before calling
      if (!container.completed()) {
        container.addOwnedKey(key);
        return true;
      }
      return false;
    }

    public synchronized void removeOwnedKey(int instance, String key) {
      Model.Container container = getContainer(instance);
      assert container != null;  // should never get here.
      container.removeOwnedKey(key);
    }

    public void initialize() throws IOException {
      log.info("Initializing service '{}'.", name);
      // Request initial containers
      for (int i = 0; i < service.getInstances(); i++) {
        addContainer();
      }
    }

    private synchronized Model.Container getContainer(int instance) {
      if (instance >= 0 && instance < containers.size()) {
        return containers.get(instance);
      }
      return null;
    }


    public List<Model.Container> scale(int count, int delta) {
      List<Model.Container> out =  new ArrayList<Model.Container>();

      // Any function that may remove containers needs to lock the kv store
      // outside the tracker to prevent deadlocks.
      synchronized (keyValueStore) {
        synchronized (this) {
          int active = getNumActive();
          if (delta == 0) {
            delta = count - active;
          } else {
            // If delta would result in negative instances, we just scale to 0
            delta = Math.max(delta, -active);
            count = delta + active;
          }
          log.info("Scaling service '{}' to {} instances, a delta of {}.",
                   name, count, delta);
          if (delta > 0) {
            // Scale up
            for (int i = 0; i < delta; i++) {
              out.add(addContainer());
              numTarget += 1;
            }
          } else if (delta < 0) {
            // Scale down
            for (int i = delta; i < 0; i++) {
              int instance;
              if (waiting.size() > 0) {
                instance = Utils.popfirst(waiting);
              } else if (requested.size() > 0) {
                instance = requested.get(requested.firstKey()).getInstance();
              } else {
                instance = Utils.popfirst(running);
              }
              finishContainer(instance, Model.Container.State.KILLED,
                              "Killed by user request.");
              out.add(containers.get(instance));
            }
          }
        }
      }
      return out;
    }

    private synchronized void requestContainer(Model.Container container) {
      Priority priority = newPriority(this);
      String[] nodes = (service.getNodes().isEmpty() ? null
                        : service.getNodes().toArray(new String[0]));
      String[] racks = (service.getRacks().isEmpty() ? null
                        : service.getRacks().toArray(new String[0]));
      boolean relaxLocality = ((nodes == null && racks == null) ? true
                               : service.getRelaxLocality());
      ContainerRequest req = new ContainerRequest(
          service.getResources(),
          nodes,
          racks,
          priority,
          relaxLocality,
          Strings.emptyToNull(service.getNodeLabel()));
      container.setContainerRequest(req);
      rmClient.addContainerRequest(req);
      requested.put(priority, container);
      log.info("REQUESTED: {}", container.getId());
    }

    public Model.Container addContainer() {
      return addContainer(Collections.<String, String>emptyMap());
    }

    public synchronized Model.Container addContainer(Map<String, String> env) {
      Model.Container container = new Model.Container(name, containers.size(),
                                        Model.Container.State.REQUESTED,
                                        env);
        requestContainer(container);

      containers.add(container);
      return container;
    }

    public void handleNewContainer(final Container container) {
      // Synchronize only in this block so that only one service is blocked at
      // a time (instead of potentially multiple).
      Model.Container newContainer;
      synchronized (this) {
        Priority priority = container.getPriority();
        // Some YARN configurations return resources on allocate that have
        // vcores always set to 1 (even though the allocated container gets the
        // correct number of vcores). Here we create a new resource with the same
        // memory returned, but the requested number of vcores, and use that
        // instead. See https://github.com/dask/dask-yarn/issues/48 for more
        // discussion.
        Resource requestedResource = service.getResources();
        Resource resource = Resource.newInstance(
            container.getResource().getMemory(),
            requestedResource.getVirtualCores()
        );
        if (requestedResource.compareTo(resource) > 0) {
          // Safeguard around containers not matching, shouldn't ever be hit
          log.warn("{} with {}, priority {} doesn't meet requested resource "
                   + "requirements {}, releasing",
                   container.getId(), resource, priority, service.getResources());
          rmClient.releaseAssignedContainer(container.getId());
          return;
        }

        removePriority(priority);
        newContainer = requested.remove(priority);
        if (newContainer == null) {
          // Container received after request was canceled
          log.debug("Releasing {} with priority {} due to canceled request for service {}",
                    container.getId(), priority, name);
          rmClient.releaseAssignedContainer(container.getId());
          return;
        }
        // Remove request so it dosn't get resubmitted
        rmClient.removeContainerRequest(newContainer.popContainerRequest());

        // Add fields for running container
        newContainer.setState(Model.Container.State.RUNNING);
        newContainer.setStartTime(System.currentTimeMillis());
        newContainer.setYarnContainerId(container.getId());
        newContainer.setYarnNodeId(container.getNodeId());
        newContainer.setYarnNodeHttpAddress(container.getNodeHttpAddress());
        newContainer.setResources(resource);

        ApplicationMaster.this.containers.put(container.getId(), newContainer);

        final int instance = newContainer.getInstance();
        running.add(instance);

        // Update container environment variables
        Map<String, String> env = new HashMap<String, String>(service.getEnv());
        env.putAll(newContainer.getEnv());
        updateServiceEnvironment(env, resource, newContainer.getId());
        if (!UserGroupInformation.isSecurityEnabled()) {
          // Add HADOOP_USER_NAME to environment for *simple* authentication only
          env.put("HADOOP_USER_NAME", userName);
        }

        final ContainerLaunchContext ctx =
            ContainerLaunchContext.newInstance(
                service.getLocalResources(),
                env,
                Arrays.asList(service.getScript()),
                null,
                tokens,
                null);

        totalMemory.addAndGet(resource.getMemory());
        totalVcores.addAndGet(resource.getVirtualCores());

        log.info("Starting {}...", container.getId());
        containerLaunchExecutor.execute(
            new Runnable() {
              @Override
              public void run() {
                try {
                  nmClient.startContainer(container, ctx);
                } catch (Throwable exc) {
                  log.warn("Failed to start {}_{}", ServiceTracker.this.name, instance, exc);
                  ServiceTracker.this.finishContainer(instance,
                      Model.Container.State.FAILED,
                      "Failed to start, exception raised: " + exc.getMessage());
                }
              }
            });

        log.info("RUNNING: {} on {}", newContainer.getId(), container.getId());
      }

      if (!initialRunning && requested.size() == 0) {
        initialRunning = true;
      }
    }

    public void finishContainer(int instance, Model.Container.State state, String exitMessage) {
      // Any function that may remove containers, needs to lock the kv store
      // outside the tracker to prevent deadlocks.
      synchronized (keyValueStore) {
        synchronized (this) {
          Model.Container container = containers.get(instance);

          switch (container.getState()) {
            case WAITING:
              waiting.remove(instance);
              break;
            case REQUESTED:
              ContainerRequest req = container.popContainerRequest();
              Priority priority = req.getPriority();
              removePriority(priority);
              requested.remove(priority);
              rmClient.removeContainerRequest(req);
              break;
            case RUNNING:
              rmClient.releaseAssignedContainer(container.getYarnContainerId());
              running.remove(instance);
              container.setFinishTime(System.currentTimeMillis());
              Resource resource = container.getResources();
              totalMemory.getAndAdd(-resource.getMemory());
              totalVcores.getAndAdd(-resource.getVirtualCores());
              break;
            default:
              return;  // Already finished, should never get here
          }

          boolean mayRestart = false;
          boolean warn = false;
          switch (state) {
            case SUCCEEDED:
              numSucceeded += 1;
              break;
            case KILLED:
              numKilled += 1;
              break;
            case FAILED:
              numFailed += 1;
              mayRestart = true;
              warn = true;
              break;
            default:
              throw new IllegalArgumentException(
                  "finishContainer got illegal state " + state);
          }

          if (warn) {
            log.warn("{}: {} - {}", state, container.getId(), exitMessage);
          } else {
            log.info("{}: {} - {}", state, container.getId(), exitMessage);
          }

          container.setState(state);
          container.setExitMessage(exitMessage);
          container.clearOwnedKeys();

          if (mayRestart && (service.getMaxRestarts() == -1
              || numRestarted < service.getMaxRestarts())) {
            numRestarted += 1;
            log.info("RESTARTING: adding new container to replace {}.",
                     container.getId());
            addContainer(container.getEnv());
          }

          if (isFinished() || isFailed()) {
            maybeShutdown();
          }
        }
      }
    }
  }
}
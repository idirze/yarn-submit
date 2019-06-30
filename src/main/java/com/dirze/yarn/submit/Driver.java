package com.dirze.yarn.submit;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.ObjectArrays;
import com.google.common.collect.Sets;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.security.TokenCache;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.ClientRMProxy;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.security.client.RMDelegationTokenIdentifier;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.log4j.Level;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.PrivilegedExceptionAction;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
public class Driver {


    // Owner rwx (700)
    private static final FsPermission YARN_SUBMIT_DIR_PERM =
            FsPermission.createImmutable((short) 448);
    // Owner rw (600)
    private static final FsPermission YARN_SUBMIT_FILE_PERM =
            FsPermission.createImmutable((short) 384);

    private Configuration conf;

    private UserGroupInformation ugi;
    private FileSystem defaultFileSystem;
    private YarnClient defaultYarnClient;

    private String classpath;

    // -- commandline flags
    private String jarPath = null;
    private String keytabPath = null;
    private String principal = null;

    /**
     * Main Entry Point.
     **/
    public static void main(String[] args) {
        log.debug("Starting yarnSubmit version {}", Utils.getYarnSubmitVersion());

        Configuration conf = new YarnConfiguration();

        // Maybe specify the netty native workdir. This is necessary for systems
        // where `/tmp` is not executable.
        Utils.configureNettyNativeWorkDir();

        try {
            Driver driver = new Driver();
            log.info("Init");
            driver.init(args);
            log.info("Run");
            driver.run();
            // System.exit(0);
            log.info("Instantiate driver");
            DriverImpl client = driver.new DriverImpl();


            Model.ApplicationSpec spec = new Model.ApplicationSpec();


            Map<String, Model.Service> services = new HashMap();
            Model.Service service1 = new Model.Service();

            // -- resources
            service1.setResources(Resource.newInstance(256, 1));


            // -- files
            FileSystem fs = FileSystem.get(conf);
            //Copy bundle to HDFS
            Path localPath1 = new Path("/root/script1.sh");
            Path localPath2 = new Path("/root/script2.sh");

            Path yarnPath1 = fs.makeQualified(Path.mergePaths(new Path("/apps/"), localPath1));
            Path yarnPath2 = fs.makeQualified(Path.mergePaths(new Path("/apps/"), localPath2));

            System.out.println("Copy from local: "+ localPath1 +", to: "+ yarnPath1);
            fs.copyFromLocalFile(false, true, localPath1, yarnPath1);

            System.out.println("Copy from local: "+ localPath2 +", to: "+ yarnPath2);
            fs.copyFromLocalFile(false, true, localPath2, yarnPath2);


            Map<String, LocalResource> localResources = new HashMap<>();

            LocalResource lr1 = Utils.localResource(fs, yarnPath1, LocalResourceType.FILE);
            localResources.put("script1.sh", lr1);

            service1.setLocalResources(localResources);
            service1.setEnv(Maps.<String, String>newHashMap());
            service1.setInstances(1);


            // -- Commands
            service1.setScript("sh script1.sh");

            service1.setMaxRestarts(15);
            service1.setNodeLabel("");
            service1.setRacks(Lists.<String>newArrayList());
            service1.setNodes(Lists.<String>newArrayList());

            services.put("worker1", service1);

            spec.setServices(services);

            spec.setName("kafka-streams");
            spec.setQueue("default");

            spec.setUser("yarn");
            spec.setMaxAttempts(5);
            spec.setTags(Sets.newHashSet("Kafka-streams"));

            // Master: process to run on the app master
            Model.Master master = new Model.Master();
            master.setResources(Resource.newInstance(256, 2));
            master.setEnv(Maps.<String, String>newHashMap());

            Map<String, LocalResource> localResources2 = new HashMap<>();

            LocalResource lr2 = Utils.localResource(fs, yarnPath2, LocalResourceType.FILE);
            localResources2.put("script2.sh", lr2);

            master.setLocalResources(localResources2);
            master.setScript("sh script2.sh");
            master.setLogLevel(Level.DEBUG);

            spec.setMaster(master);


            spec.setNodeLabel("");


            spec.validate();


            log.info("Submit the application");

            client.submit(spec);
            log.info("Application  submitted");

        } catch (Throwable exc) {
            log.error("Error running Driver", exc);
            System.exit(1);
        }


    }

    private void usageError() {
        log.error("Usage: COMMAND --jar PATH [--keytab PATH, --principal NAME] [--daemon]");
        System.exit(1);
    }

    private void init(String[] args) throws IOException {

        // Parse arguments
        int i = 0;
        while (i < args.length) {
            String value = (i + 1 < args.length) ? args[i + 1] : null;
            switch (args[i]) {
                case "--jar":
                    jarPath = value;
                    i += 2;
                    break;
                case "--keytab":
                    keytabPath = value;
                    i += 2;
                    break;
                case "--principal":
                    principal = value;
                    i += 2;
                    break;
                default:
                    //usageError();
                    i += 1;
                    break;
            }
        }
        if (jarPath == null || ((keytabPath == null) != (principal == null))) {
            usageError();
        }

        // Login using the appropriate method. We don't need to start a thread to
        // do periodic logins, as we're only making use of normal Hadoop RPC apis,
        // and these automatically handle relogin on failure. See
        // https://stackoverflow.com/q/34616676/1667287
        if (keytabPath != null) {
            log.debug("Logging in using keytab: {}, principal: {}", keytabPath, principal);
            UserGroupInformation.loginUserFromKeytab(principal, keytabPath);
            ugi = UserGroupInformation.getLoginUser();
        } else {
            log.debug("Logging in using ticket cache");
            ugi = UserGroupInformation.getLoginUser();
        }

        conf = new YarnConfiguration();

        // Build the classpath for running the appmaster
        StringBuilder cpBuilder = new StringBuilder(Environment.CLASSPATH.$$());
        cpBuilder.append(ApplicationConstants.CLASS_PATH_SEPARATOR).append("./*");
        for (String c : conf.getStrings(
                YarnConfiguration.YARN_APPLICATION_CLASSPATH,
                YarnConfiguration.DEFAULT_YARN_CROSS_PLATFORM_APPLICATION_CLASSPATH)) {
            cpBuilder.append(ApplicationConstants.CLASS_PATH_SEPARATOR);
            cpBuilder.append(c.trim());
        }
        classpath = cpBuilder.toString();
    }

    private void run() throws Exception {
        // Connect to hdfs as *this* user
        defaultFileSystem = getFs();
        // Start the yarn client as *this* user
        defaultYarnClient = getYarnClient();
    }

    public FileSystem getFs() throws IOException {
        return FileSystem.get(conf);
    }

    public YarnClient getYarnClient() {
        YarnClient client = YarnClient.createYarnClient();
        client.init(conf);
        client.start();
        return client;
    }

    public Path getAppDir(FileSystem fs, ApplicationId appId) {
        return new Path(fs.getHomeDirectory(), ".yarnSubmit/" + appId.toString());
    }

    public void killApplication(final ApplicationId appId, String user)
            throws IOException, YarnException, InterruptedException {
        if (user.isEmpty()) {
            killApplicationInner(defaultYarnClient, defaultFileSystem, appId);
        } else {
            UserGroupInformation.createProxyUser(user, ugi).doAs(
                    new PrivilegedExceptionAction<Void>() {
                        public Void run() throws IOException, YarnException {
                            killApplicationInner(getYarnClient(), getFs(), appId);
                            return null;
                        }
                    });
        }
    }

    private void killApplicationInner(YarnClient yarnClient, FileSystem fs,
                                      ApplicationId appId) throws IOException, YarnException {

        log.debug("Killing application {}", appId);
        yarnClient.killApplication(appId);
        deleteAppDir(fs, getAppDir(fs, appId));
    }

    /**
     * Start a new application.
     **/
    public ApplicationId submitApplication(final Model.ApplicationSpec spec)
            throws IOException, YarnException, InterruptedException {
        if (spec.getUser().isEmpty()) {
            return submitApplicationInner(defaultYarnClient, defaultFileSystem, spec);
        } else {
            return UserGroupInformation.createProxyUser(spec.getUser(), ugi).doAs(
                    new PrivilegedExceptionAction<ApplicationId>() {
                        public ApplicationId run() throws IOException, YarnException {
                            return submitApplicationInner(getYarnClient(), getFs(), spec);
                        }
                    });
        }
    }

    private ApplicationId submitApplicationInner(YarnClient yarnClient,
                                                 FileSystem fs, Model.ApplicationSpec spec) throws IOException, YarnException {
        // First validate the spec request
        spec.validate();

        // Get an application id. This is needed before doing anything else so we
        // can upload additional files to the application directory
        YarnClientApplication app = yarnClient.createApplication();
        ApplicationSubmissionContext appContext = app.getApplicationSubmissionContext();
        ApplicationId appId = appContext.getApplicationId();

        // Start building the appmaster request
        Model.Master master = spec.getMaster();

        // Directory to store temporary application resources
        Path appDir = getAppDir(fs, appId);

        // Setup the appmaster environment variables
        Map<String, String> env = new HashMap<String, String>();
        env.putAll(master.getEnv());
        env.put("CLASSPATH", classpath);
        env.put("YARN_SUBMIT_APPLICATION_ID", appId.toString());
        String lang = System.getenv("LANG");
        if (lang != null) {
            env.put("LANG", lang);
        }

        // Setup the appmaster commands
        String logdir = ApplicationConstants.LOG_DIR_EXPANSION_VAR;
        String log4jConfig = (master.hasLogConfig()
                ? "-Dlog4j.configuration=file:./.yarnSubmit.log4j.properties "
                : "");
        Level logLevel = master.getLogLevel();
        List<String> commands = Arrays.asList(
                (Environment.JAVA_HOME.$$() + "/bin/java "
                        + "-Xmx128M "
                        + log4jConfig
                        + "-DyarnSubmit.log.level=" + logLevel
                        + " -DyarnSubmit.log.directory=" + ApplicationConstants.LOG_DIR_EXPANSION_VAR
                        + " com.dirze.yarn.submit.ApplicationMaster "
                        + appDir
                        + " >" + logdir + "/application.master.log 2>&1"));

        UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
        ByteBuffer fsTokens = null;
        if (UserGroupInformation.isSecurityEnabled()) {
            fsTokens = collectTokens(yarnClient, fs, spec);
        } else {
            env.put("HADOOP_USER_NAME", ugi.getUserName());
        }

        try {
            // Setup the LocalResources for the appmaster and containers
            Map<String, LocalResource> localResources = setupAppDir(fs, spec, appDir);

            ContainerLaunchContext amContext = ContainerLaunchContext.newInstance(
                    localResources, env, commands, null, fsTokens, null);

            appContext.setApplicationType("yarnSubmit");
            appContext.setAMContainerSpec(amContext);
            appContext.setApplicationName(spec.getName());
            appContext.setResource(master.getResources());
            appContext.setPriority(Priority.newInstance(0));
            appContext.setQueue(spec.getQueue());
            appContext.setMaxAppAttempts(spec.getMaxAttempts());
            appContext.setNodeLabelExpression(Strings.emptyToNull(spec.getNodeLabel()));
            appContext.setApplicationTags(spec.getTags());

            log.info("Submitting application...");
            yarnClient.submitApplication(appContext);
        } catch (Exception exc) {
            // Ensure application directory is deleted on submission failure
            deleteAppDir(fs, appDir);
            throw exc;
        }

        return appId;
    }

    private ByteBuffer collectTokens(YarnClient yarnClient, FileSystem fs,
                                     Model.ApplicationSpec spec) throws IOException, YarnException {
        // Collect security tokens as needed
        log.debug("Collecting filesystem delegation tokens");
        Credentials credentials = UserGroupInformation.getCurrentUser().getCredentials();
        TokenCache.obtainTokensForNamenodes(
                credentials,
                ObjectArrays.concat(
                        new Path(fs.getUri()),
                        spec.getFileSystems().toArray(new Path[0])),
                conf);

        boolean hasRMToken = false;
        for (Token<?> token : credentials.getAllTokens()) {
            if (token.getKind().equals(RMDelegationTokenIdentifier.KIND_NAME)) {
                log.debug("RM delegation token already acquired");
                hasRMToken = true;
                break;
            }
        }
        if (!hasRMToken) {
            log.debug("Adding RM delegation token");
            Text rmDelegationTokenService = ClientRMProxy.getRMDelegationTokenService(conf);
            String tokenRenewer = conf.get(YarnConfiguration.RM_PRINCIPAL);
            org.apache.hadoop.yarn.api.records.Token rmDelegationToken =
                    yarnClient.getRMDelegationToken(new Text(tokenRenewer));
            Token<TokenIdentifier> rmToken = ConverterUtils.convertFromYarn(
                    rmDelegationToken, rmDelegationTokenService
            );
            credentials.addToken(rmDelegationTokenService, rmToken);
        }

        DataOutputBuffer dob = new DataOutputBuffer();
        credentials.writeTokenStorageToStream(dob);
        return ByteBuffer.wrap(dob.getData(), 0, dob.getLength());
    }

    private void deleteAppDir(FileSystem fs, Path appDir) {
        try {
            if (fs.exists(appDir)) {
                if (fs.delete(appDir, true)) {
                    log.debug("Deleted application directory {}", appDir);
                    return;
                }
                if (fs.exists(appDir)) {
                    log.warn("Failed to delete application directory {}", appDir);
                }
            }
        } catch (IOException exc) {
            log.warn("Failed to delete application directory {}", appDir, exc);
        }
    }

    private Map<String, LocalResource> setupAppDir(FileSystem fs,
                                                   Model.ApplicationSpec spec, Path appDir) throws IOException {

        // Make the ~/.yarnSubmit/app_id dir
        log.info("Uploading application resources to {}", appDir);
        FileSystem.mkdirs(fs, appDir, YARN_SUBMIT_DIR_PERM);

        Map<Path, Path> uploadCache = new HashMap<Path, Path>();

        // Create LocalResources for the crt/pem files, and add them to the
        // security object.
        Model.Master master = spec.getMaster();


        // Setup the LocalResources for the services
        for (Map.Entry<String, Model.Service> entry : spec.getServices().entrySet()) {
            finalizeService(entry.getKey(), entry.getValue(), fs,
                    uploadCache, appDir);
        }
        spec.validate();

        // Setup the LocalResources for the application master
        Map<String, LocalResource> lr = master.getLocalResources();
        for (LocalResource resource : lr.values()) {
            finalizeLocalResource(uploadCache, appDir, resource, true);
        }
        lr.put(".yarnSubmit.jar", newLocalResource(uploadCache, appDir, jarPath));
        if (master.hasLogConfig()) {
            LocalResource logConfig = master.getLogConfig();
            finalizeLocalResource(uploadCache, appDir, logConfig, false);
            lr.put(".yarnSubmit.log4j.properties", logConfig);
        }

        // Write the application specification to file
        Path specPath = new Path(appDir, ".yarnSubmit.proto");
        log.debug("Writing application specification to {}", specPath);
        OutputStream out = fs.create(specPath);
        try {
            MsgUtils.writeApplicationSpec(spec).writeTo(out);
        } finally {
            out.close();
        }
        LocalResource specFile = Utils.localResource(fs, specPath,
                LocalResourceType.FILE);
        lr.put(".yarnSubmit.proto", specFile);
        return lr;
    }

    private void finalizeService(String serviceName, Model.Service service,
                                 FileSystem fs, Map<Path, Path> uploadCache, Path appDir) throws IOException {

        // Write the service script to file
        final Path scriptPath = new Path(appDir, serviceName + ".sh");
        log.debug("Writing script for service '{}' to {}", serviceName, scriptPath);
        Utils.stringToFile(service.getScript(), fs.create(scriptPath));
        LocalResource scriptFile = Utils.localResource(fs, scriptPath,
                LocalResourceType.FILE);

        // Build command to execute script and set as new script
        String logdir = ApplicationConstants.LOG_DIR_EXPANSION_VAR;
        service.setScript("bash .yarnSubmit.sh >" + logdir + "/" + serviceName + ".log 2>&1");

        // Upload files/archives as necessary
        Map<String, LocalResource> lr = service.getLocalResources();
        for (LocalResource resource : lr.values()) {
            finalizeLocalResource(uploadCache, appDir, resource, true);
        }

        // Add script/crt/pem files
        lr.put(".yarnSubmit.sh", scriptFile);
        // Add LANG if present
        String lang = System.getenv("LANG");
        if (lang != null) {
            service.getEnv().put("LANG", lang);
        }
    }

    private LocalResource newLocalResource(Map<Path, Path> uploadCache, Path appDir,
                                           String localPath) throws IOException {
        LocalResource out = LocalResource.newInstance(
                URL.newInstance("file", null, -1, localPath),
                LocalResourceType.FILE,
                LocalResourceVisibility.APPLICATION,
                0, 0);
        finalizeLocalResource(uploadCache, appDir, out, false);
        return out;
    }

    private void finalizeLocalResource(Map<Path, Path> uploadCache,
                                       Path appDir, LocalResource file, boolean hash) throws IOException {

        Path srcPath = Utils.pathFromUrl(file.getResource());
        Path dstPath;

        FileSystem dstFs = appDir.getFileSystem(conf);
        FileSystem srcFs = srcPath.getFileSystem(conf);

        dstPath = uploadCache.get(srcPath);
        if (dstPath == null) {
            if (Utils.equalFs(srcFs, dstFs)) {
                // File exists in filesystem but not in upload cache
                dstPath = srcPath;
            } else {
                // File needs to be uploaded to the destination filesystem
                MessageDigest md;
                try {
                    md = MessageDigest.getInstance("MD5");
                } catch (NoSuchAlgorithmException ex) {
                    throw new IllegalArgumentException("MD5 not supported on this platform");
                }
                if (hash) {
                    md.update(srcPath.toString().getBytes());
                    String prefix = Utils.hexEncode(md.digest());
                    dstPath = new Path(new Path(appDir, prefix), srcPath.getName());
                } else {
                    dstPath = new Path(appDir, srcPath.getName());
                }
                log.debug("Uploading {} to {}", srcPath, dstPath);
                FileUtil.copy(srcFs, srcPath, dstFs, dstPath, false, conf);
                dstFs.setPermission(dstPath, YARN_SUBMIT_FILE_PERM);
            }
            uploadCache.put(srcPath, dstPath);
        }

        file.setResource(ConverterUtils.getYarnUrlFromPath(dstPath));

        FileStatus status = dstFs.getFileStatus(dstPath);

        // Only set size & timestamp if not set already
        if (file.getSize() == 0) {
            file.setSize(status.getLen());
        }

        if (file.getTimestamp() == 0) {
            file.setTimestamp(status.getModificationTime());
        }
    }

    class DriverImpl {


        private ApplicationReport getReport(String appIdString) {

            ApplicationId appId = Utils.appIdFromString(appIdString);

            if (appId == null) {
                log.error("Invalid ApplicationId '" + appIdString + "'");
                return null;
            }

            ApplicationReport report;
            try {
                report = defaultYarnClient.getApplicationReport(appId);
            } catch (Exception exc) {
                log.error("Unknown ApplicationId '" + appIdString + "'");
                return null;
            }

            if (!report.getApplicationType().equals("yarnSubmit")) {
                log.error("ApplicationId '" + appIdString
                                + "' is not a yarnSubmit application");
                return null;
            }

            return report;
        }

        public void submit(Model.ApplicationSpec spec) {

            try {
                System.out.println("Submit application"+spec.toString());
                log.info("Submit application: {}", spec.toString());
                ApplicationId  appId = submitApplication(spec);
                log.info("Application submitted: {}, appId: {}", spec.toString(), appId);
            } catch (IOException e) {
                e.printStackTrace();
            } catch (YarnException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

    }
}
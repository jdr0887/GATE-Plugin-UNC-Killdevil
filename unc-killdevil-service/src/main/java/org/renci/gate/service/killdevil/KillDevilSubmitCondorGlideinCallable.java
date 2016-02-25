package org.renci.gate.service.killdevil;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.LineNumberReader;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.Date;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.time.DateFormatUtils;
import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.Velocity;
import org.renci.jlrm.JLRMException;
import org.renci.jlrm.Queue;
import org.renci.jlrm.Site;
import org.renci.jlrm.commons.ssh.SSHConnectionUtil;
import org.renci.jlrm.lsf.ssh.LSFSSHJob;
import org.renci.jlrm.lsf.ssh.LSFSubmitScriptExporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KillDevilSubmitCondorGlideinCallable implements Callable<LSFSSHJob> {

    private final Logger logger = LoggerFactory.getLogger(KillDevilSubmitCondorGlideinCallable.class);

    private Site site;

    private Queue queue;

    private LSFSSHJob job;

    private File submitDir;

    private String collectorHost;

    private String hostAllowRead;

    private String hostAllowWrite;

    private String jobName;

    private Integer requiredMemory;

    private String username;

    private String numberOfProcessors = "$(DETECTED_CORES)";

    public KillDevilSubmitCondorGlideinCallable() {
        super();
    }

    /**
     * Creates a new glide-in
     * 
     * @return number of glide-ins submitted
     */
    public LSFSSHJob call() throws JLRMException {
        logger.debug("ENTERING call()");

        try {
            Properties velocityProperties = new Properties();
            velocityProperties.put("runtime.log.logsystem.class", "org.apache.velocity.runtime.log.NullLogChute");
            Velocity.init(velocityProperties);
        } catch (Exception e) {
            e.printStackTrace();
        }

        LSFSSHJob job = new LSFSSHJob();
        job.setTransferExecutable(Boolean.TRUE);
        job.setTransferInputs(Boolean.TRUE);
        job.setQueueName(this.queue.getName());
        job.setName(this.jobName);
        job.setHostCount(1);
        job.setNumberOfProcessors(getQueue().getNumberOfProcessors());
        job.setOutput(new File("glidein.out"));
        job.setError(new File("glidein.err"));
        job.setWallTime(queue.getRunTime());
        job.setMemory(null);

        VelocityContext velocityContext = new VelocityContext();
        velocityContext.put("siteName", getSite().getSubmitHost());
        velocityContext.put("collectorHost", this.collectorHost);
        velocityContext.put("jlrmUser", this.username);
        velocityContext.put("jlrmSiteName", getSite().getName());
        velocityContext.put("hostAllowRead", this.hostAllowRead);
        velocityContext.put("hostAllowWrite", this.hostAllowWrite);
        velocityContext.put("numberOfProcessors", this.numberOfProcessors);

        // note that we want a lower max run time here, so that the glidein can shut down
        // gracefully before getting kicked off by the batch scheduler
        long maxRunTimeAdjusted = queue.getRunTime() - 20;
        if (maxRunTimeAdjusted < 0) {
            maxRunTimeAdjusted = queue.getRunTime() / 2;
        }
        velocityContext.put("siteMaxRunTimeMins", maxRunTimeAdjusted);
        velocityContext.put("siteMaxRunTimeSecs", maxRunTimeAdjusted * 60);
        velocityContext.put("requiredMemory", this.requiredMemory * 1024);
        velocityContext.put("glideinStartTime", new Date().getTime());
        velocityContext.put("maxRunTime", maxRunTimeAdjusted);

        try {

            String remoteWorkDirSuffix = String.format(".jlrm/jobs/%s/%s",
                    DateFormatUtils.ISO_DATE_FORMAT.format(new Date()), UUID.randomUUID().toString());
            String command = String.format("(mkdir -p $HOME/%s && echo $HOME)", remoteWorkDirSuffix);

            String remoteHome = SSHConnectionUtil.execute(command, site.getUsername(), getSite().getSubmitHost());

            try {
                TimeUnit.SECONDS.sleep(2);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            logger.info("remoteHome: {}", remoteHome);
            String remoteWorkDir = String.format("%s/%s", remoteHome, remoteWorkDirSuffix);
            logger.info("remoteWorkDir: {}", remoteWorkDir);
            velocityContext.put("remoteWorkDir", remoteWorkDir);

            File tmpDir = new File(System.getProperty("java.io.tmpdir"));
            File myDir = new File(tmpDir, System.getProperty("user.name"));
            File localWorkDir = new File(myDir, UUID.randomUUID().toString());
            localWorkDir.mkdirs();
            logger.info("localWorkDir: {}", localWorkDir);

            String glideinScriptMacro = IOUtils.toString(this.getClass().getResourceAsStream("glidein.sh.vm"));
            File glideinScript = new File(localWorkDir.getAbsolutePath(), "glidein.sh");
            writeTemplate(velocityContext, glideinScript, glideinScriptMacro);
            job.setExecutable(glideinScript);

            String condorConfigLocalScriptMacro = IOUtils
                    .toString(this.getClass().getResourceAsStream("condor_config.local.vm"));
            File condorConfigLocal = new File(localWorkDir.getAbsolutePath(), "condor_config.local");
            writeTemplate(velocityContext, condorConfigLocal, condorConfigLocalScriptMacro);
            job.getInputFiles().add(condorConfigLocal);

            String condorConfigScriptMacro = IOUtils.toString(this.getClass().getResourceAsStream("condor_config"));
            File condorConfig = new File(localWorkDir.getAbsolutePath(), "condor_config");
            writeTemplate(velocityContext, condorConfig, condorConfigScriptMacro);
            job.getInputFiles().add(condorConfig);

            LSFSubmitScriptExporter<LSFSSHJob> exporter = new LSFSubmitScriptExporter<LSFSSHJob>();
            this.job = exporter.export(localWorkDir, remoteWorkDir, job);

            SSHConnectionUtil.transferSubmitScript(site.getUsername(), site.getSubmitHost(), remoteWorkDir,
                    this.job.getTransferExecutable(), this.job.getExecutable(), this.job.getTransferInputs(),
                    this.job.getInputFiles(), job.getSubmitFile());

            String targetFile = String.format("%s/%s", remoteWorkDir, job.getSubmitFile().getName());

            command = String.format("bsub < %s", targetFile);

            String submitOutput = SSHConnectionUtil.execute(command, site.getUsername(), getSite().getSubmitHost());

            LineNumberReader lnr = new LineNumberReader(new StringReader(submitOutput));
            String line;
            while ((line = lnr.readLine()) != null) {
                if (line.indexOf("submitted") != -1) {
                    logger.info("line = " + line);
                    Pattern pattern = Pattern.compile("^Job.+<(\\d*)> is submitted.+\\.$");
                    Matcher matcher = pattern.matcher(line);
                    if (!matcher.matches()) {
                        throw new JLRMException("failed to parse the jobid number");
                    } else {
                        job.setId(matcher.group(1));
                    }
                    break;
                }
            }
        } catch (FileNotFoundException e) {
            logger.error("FileNotFoundException", e);
            throw new JLRMException("JSchException: " + e.getMessage());
        } catch (IOException e) {
            logger.error("IOException", e);
            throw new JLRMException("IOException: " + e.getMessage());
        }
        return job;
    }

    /**
     * Generates a new job file which is used to setup and start the glidein
     */
    private void writeTemplate(VelocityContext velocityContext, File file, String template) throws IOException {
        StringWriter sw = new StringWriter();
        Velocity.evaluate(velocityContext, sw, "glidein", template);
        file.setReadable(true);
        file.setExecutable(true);
        file.setWritable(true, true);
        FileUtils.writeStringToFile(file, sw.toString());
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public Site getSite() {
        return site;
    }

    public void setSite(Site site) {
        this.site = site;
    }

    public LSFSSHJob getJob() {
        return job;
    }

    public void setJob(LSFSSHJob job) {
        this.job = job;
    }

    public File getSubmitDir() {
        return submitDir;
    }

    public void setSubmitDir(File submitDir) {
        this.submitDir = submitDir;
    }

    public String getCollectorHost() {
        return collectorHost;
    }

    public void setCollectorHost(String collectorHost) {
        this.collectorHost = collectorHost;
    }

    public Integer getRequiredMemory() {
        return requiredMemory;
    }

    public void setRequiredMemory(Integer requiredMemory) {
        this.requiredMemory = requiredMemory;
    }

    public Queue getQueue() {
        return queue;
    }

    public void setQueue(Queue queue) {
        this.queue = queue;
    }

    public String getJobName() {
        return jobName;
    }

    public void setJobName(String jobName) {
        this.jobName = jobName;
    }

    public String getHostAllowRead() {
        return hostAllowRead;
    }

    public void setHostAllowRead(String hostAllowRead) {
        this.hostAllowRead = hostAllowRead;
    }

    public String getHostAllowWrite() {
        return hostAllowWrite;
    }

    public void setHostAllowWrite(String hostAllowWrite) {
        this.hostAllowWrite = hostAllowWrite;
    }

    public String getNumberOfProcessors() {
        return numberOfProcessors;
    }

    public void setNumberOfProcessors(String numberOfProcessors) {
        this.numberOfProcessors = numberOfProcessors;
    }

}

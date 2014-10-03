package org.renci.gate.service.killdevil;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;

import org.apache.commons.lang.StringUtils;
import org.renci.gate.AbstractGATEService;
import org.renci.gate.GATEException;
import org.renci.gate.GlideinMetric;
import org.renci.jlrm.JLRMException;
import org.renci.jlrm.Queue;
import org.renci.jlrm.commons.ssh.SSHConnectionUtil;
import org.renci.jlrm.lsf.LSFJobStatusInfo;
import org.renci.jlrm.lsf.LSFJobStatusType;
import org.renci.jlrm.lsf.ssh.LSFSSHJob;
import org.renci.jlrm.lsf.ssh.LSFSSHKillCallable;
import org.renci.jlrm.lsf.ssh.LSFSSHLookupStatusCallable;
import org.renci.jlrm.lsf.ssh.LSFSSHSubmitCondorGlideinCallable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author jdr0887
 */
public class KillDevilGATEService extends AbstractGATEService {

    private final Logger logger = LoggerFactory.getLogger(KillDevilGATEService.class);

    private final List<LSFSSHJob> jobCache = new ArrayList<LSFSSHJob>();

    private String username;

    public KillDevilGATEService() {
        super();
    }

    @Override
    public Boolean isValid() throws GATEException {
        logger.debug("ENTERING isValid()");
        try {
            String results = SSHConnectionUtil.execute("ls /nas02/apps | wc -l", getSite().getUsername(), getSite()
                    .getSubmitHost());
            if (StringUtils.isNotEmpty(results) && Integer.valueOf(results.trim()) > 0) {
                return true;
            }
        } catch (NumberFormatException | JLRMException e) {
            throw new GATEException(e);
        }
        return false;
    }

    @Override
    public List<GlideinMetric> lookupMetrics() throws GATEException {
        logger.debug("ENTERING lookupMetrics()");
        Map<String, GlideinMetric> metricsMap = new HashMap<String, GlideinMetric>();

        List<Queue> queueList = getSite().getQueueList();
        for (Queue queue : queueList) {
            metricsMap.put(queue.getName(), new GlideinMetric(getSite().getName(), queue.getName(), 0, 0));
        }

        try {
            LSFSSHLookupStatusCallable callable = new LSFSSHLookupStatusCallable(getSite());
            Set<LSFJobStatusInfo> jobStatusSet = Executors.newSingleThreadExecutor().submit(callable).get();
            logger.debug("jobStatusSet.size(): {}", jobStatusSet.size());

            if (jobStatusSet != null && jobStatusSet.size() > 0) {

                String jobName = String.format("glidein-%s", getSite().getName().toLowerCase());

                for (LSFJobStatusInfo info : jobStatusSet) {

                    if (!info.getJobName().equals(jobName)) {
                        continue;
                    }

                    switch (info.getType()) {
                        case PENDING:
                            metricsMap.get(info.getQueue()).incrementPending();
                            break;
                        case RUNNING:
                            metricsMap.get(info.getQueue()).incrementRunning();
                            break;
                    }
                }

            }

        } catch (Exception e) {
            throw new GATEException(e);
        }

        List<GlideinMetric> metricList = new ArrayList<GlideinMetric>();
        metricList.addAll(metricsMap.values());

        return metricList;
    }

    @Override
    public void createGlidein(Queue queue) throws GATEException {
        logger.debug("ENTERING createGlidein(Queue)");

        File submitDir = new File("/tmp", System.getProperty("user.name"));
        submitDir.mkdirs();
        LSFSSHJob job = null;
        try {
            logger.info("siteInfo: {}", getSite());
            logger.info("queueInfo: {}", queue);
            String hostAllow = "*.unc.edu";
            LSFSSHSubmitCondorGlideinCallable callable = new LSFSSHSubmitCondorGlideinCallable();
            callable.setCollectorHost(getCollectorHost());
            callable.setHostAllowRead(hostAllow);
            callable.setHostAllowWrite(hostAllow);
            callable.setJobName(String.format("glidein-%s", getSite().getName().toLowerCase()));
            callable.setQueue(queue);
            callable.setRequiredMemory(40);
            callable.setSite(getSite());
            callable.setSubmitDir(submitDir);
            callable.setUsername(username);

            job = Executors.newSingleThreadExecutor().submit(callable).get();
            if (job != null && StringUtils.isNotEmpty(job.getId())) {
                logger.info("job.getId(): {}", job.getId());
                jobCache.add(job);
            }
        } catch (Exception e) {
            throw new GATEException(e);
        }
    }

    @Override
    public void deleteGlidein(Queue queue) throws GATEException {
        logger.debug("ENTERING deleteGlidein(QueueInfo)");
        try {
            LSFSSHLookupStatusCallable lookupStatusCallable = new LSFSSHLookupStatusCallable(getSite());
            Set<LSFJobStatusInfo> jobStatusSet = Executors.newSingleThreadExecutor().submit(lookupStatusCallable).get();
            Iterator<LSFJobStatusInfo> iter = jobStatusSet.iterator();
            String jobName = String.format("glidein-%s", getSite().getName().toLowerCase());
            while (iter.hasNext()) {
                LSFJobStatusInfo info = iter.next();
                if (!info.getJobName().equals(jobName)) {
                    continue;
                }
                logger.debug("deleting: {}", info.toString());
                LSFSSHKillCallable killCallable = new LSFSSHKillCallable(getSite(), info.getJobId());
                Executors.newSingleThreadExecutor().submit(killCallable).get();
                break;
            }
        } catch (Exception e) {
            throw new GATEException(e);
        }
    }

    @Override
    public void deletePendingGlideins() throws GATEException {
        logger.debug("ENTERING deletePendingGlideins()");
        try {
            LSFSSHLookupStatusCallable lookupStatusCallable = new LSFSSHLookupStatusCallable(getSite());
            Set<LSFJobStatusInfo> jobStatusSet = Executors.newSingleThreadExecutor().submit(lookupStatusCallable).get();
            String jobName = String.format("glidein-%s", getSite().getName().toLowerCase());
            for (LSFJobStatusInfo info : jobStatusSet) {
                if (!info.getJobName().equals(jobName)) {
                    continue;
                }
                if (info.getType().equals(LSFJobStatusType.PENDING)) {
                    logger.debug("deleting: {}", info.toString());
                    LSFSSHKillCallable killCallable = new LSFSSHKillCallable(getSite(), info.getJobId());
                    Executors.newSingleThreadExecutor().submit(killCallable).get();
                    // throttle the deleteGlidein calls such that SSH doesn't complain
                    Thread.sleep(2000);
                }
            }
        } catch (Exception e) {
            throw new GATEException(e);
        }
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

}

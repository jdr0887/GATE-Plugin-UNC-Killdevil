package org.renci.gate.plugin.killdevil;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;

import org.apache.commons.lang.StringUtils;
import org.renci.gate.AbstractGATEService;
import org.renci.gate.GATEException;
import org.renci.gate.GlideinMetric;
import org.renci.jlrm.Queue;
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
    public Map<String, GlideinMetric> lookupMetrics() throws GATEException {
        logger.info("ENTERING lookupMetrics()");
        Map<String, GlideinMetric> metricsMap = new HashMap<String, GlideinMetric>();
        try {

            LSFSSHLookupStatusCallable callable = new LSFSSHLookupStatusCallable(jobCache, getSite());
            Set<LSFJobStatusInfo> jobStatusSet = Executors.newSingleThreadExecutor().submit(callable).get();

            // get unique list of queues
            Set<String> queueSet = new HashSet<String>();
            if (jobStatusSet != null) {
                for (LSFJobStatusInfo info : jobStatusSet) {
                    queueSet.add(info.getQueue());
                }
            }

            Iterator<LSFSSHJob> jobCacheIter = jobCache.iterator();
            while (jobCacheIter.hasNext()) {
                LSFSSHJob job = jobCacheIter.next();
                for (String queue : queueSet) {
                    int running = 0;
                    int pending = 0;
                    for (LSFJobStatusInfo info : jobStatusSet) {
                        GlideinMetric metrics = new GlideinMetric();
                        if (info.getQueue().equals(queue) && job.getId().equals(info.getJobId())) {
                            switch (info.getType()) {
                                case PENDING:
                                    ++pending;
                                    break;
                                case RUNNING:
                                    ++running;
                                    break;
                                case EXIT:
                                case UNKNOWN:
                                case ZOMBIE:
                                case DONE:
                                    jobCacheIter.remove();
                                    break;
                                case SUSPENDED_BY_SYSTEM:
                                case SUSPENDED_BY_USER:
                                case SUSPENDED_FROM_PENDING:
                                default:
                                    break;
                            }
                        }
                        metrics.setQueue(queue);
                        metrics.setPending(pending);
                        metrics.setRunning(running);
                        metricsMap.put(queue, metrics);
                    }
                }
            }

        } catch (Exception e ) {
            throw new GATEException(e);
        }
        return metricsMap;
    }

    @Override
    public void createGlidein(Queue queue) throws GATEException {
        logger.info("ENTERING createGlidein(Queue)");

        if (StringUtils.isNotEmpty(getActiveQueues()) && !getActiveQueues().contains(queue.getName())) {
            logger.warn("queue name is not in active queue list...see etc/org.renci.gate.plugin.killdevil.cfg");
            return;
        }

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
            callable.setJobName("glidein");
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
        } catch (Exception e ) {
            throw new GATEException(e);
        }
    }

    @Override
    public void deleteGlidein(Queue queue) throws GATEException {
        logger.info("ENTERING deleteGlidein(QueueInfo)");
        if (jobCache.size() > 0) {
            try {
                logger.info("siteInfo: {}", getSite());
                logger.info("queueInfo: {}", queue);
                LSFSSHJob job = jobCache.get(0);
                LSFSSHKillCallable callable = new LSFSSHKillCallable(getSite(), job.getId());
                Executors.newSingleThreadExecutor().submit(callable).get();
                jobCache.remove(0);
            } catch (Exception e ) {
                throw new GATEException(e);
            }
        }
    }

    @Override
    public void deletePendingGlideins() throws GATEException {
        logger.info("ENTERING deletePendingGlideins()");
        try {
            LSFSSHLookupStatusCallable lookupStatusCallable = new LSFSSHLookupStatusCallable(jobCache, getSite());
            Set<LSFJobStatusInfo> jobStatusSet = Executors.newSingleThreadExecutor().submit(lookupStatusCallable).get();
            for (LSFJobStatusInfo info : jobStatusSet) {
                if (info.getType().equals(LSFJobStatusType.PENDING)) {
                    LSFSSHKillCallable killCallable = new LSFSSHKillCallable(getSite(), info.getJobId());
                    Executors.newSingleThreadExecutor().submit(killCallable).get();
                }
                // throttle the deleteGlidein calls such that SSH doesn't complain
                Thread.sleep(2000);
            }
        } catch (Exception e ) {
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

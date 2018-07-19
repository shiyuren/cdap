/*
 * Copyright © 2018 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.runtime.spi.provisioner.emr;

import co.cask.cdap.runtime.spi.provisioner.Node;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.amazonaws.services.ec2.model.DeleteKeyPairRequest;
import com.amazonaws.services.ec2.model.ImportKeyPairRequest;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClientBuilder;
import com.amazonaws.services.elasticmapreduce.model.Cluster;
import com.amazonaws.services.elasticmapreduce.model.ClusterState;
import com.amazonaws.services.elasticmapreduce.model.ClusterStatus;
import com.amazonaws.services.elasticmapreduce.model.ClusterSummary;
import com.amazonaws.services.elasticmapreduce.model.DescribeClusterRequest;
import com.amazonaws.services.elasticmapreduce.model.JobFlowInstancesConfig;
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowRequest;
import com.amazonaws.services.elasticmapreduce.model.TerminateJobFlowsRequest;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Wrapper around the EMR client that adheres to our configuration settings.
 */
public class EMRClient implements AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(EMRClient.class);

  private static final Set UNTERMINATED_STATES = ImmutableSet.of(ClusterState.BOOTSTRAPPING, ClusterState.STARTING,
                                                                 ClusterState.RUNNING, ClusterState.WAITING);

  private final EMRConf emrConf;
  private final AmazonElasticMapReduce client;

  static EMRClient fromConf(EMRConf conf) {
    AmazonElasticMapReduceClientBuilder standard = AmazonElasticMapReduceClientBuilder.standard();
    standard.setCredentials(conf.getCredentialsProvider());
    standard.withRegion(conf.getRegion());
    AmazonElasticMapReduce client = standard.build();
    return new EMRClient(conf, client);
  }

  private EMRClient(EMRConf emrConf, AmazonElasticMapReduce client) {
    this.emrConf = emrConf;
    this.client = client;
  }

  /**
   * Create a cluster. This will return after the initial request to create the cluster is completed.
   * At this point, the cluster is likely not yet running, but in a provisioning state.
   *
   * @param name the name of the cluster to create
   * @return the id of the created EMR cluster
   */
  public String createCluster(String name) {
    AmazonEC2 ec2 = AmazonEC2ClientBuilder.standard()
            .withCredentials(emrConf.getCredentialsProvider())
            .withRegion(emrConf.getRegion())
            .build();

    // name the keypair the same thing as the cluster name
    ec2.importKeyPair(new ImportKeyPairRequest(name, emrConf.getPublicKey().getKey()));


    RunJobFlowRequest request = new RunJobFlowRequest()
      .withName(name)
      // all 4.9.x is java 7... which we don't support, so EMR 5.0.0 is our minimum
      .withReleaseLabel("emr-5.15.0")
      .withServiceRole(emrConf.getServiceRole())
      .withJobFlowRole(emrConf.getJobFlowRole())
      .withInstances(new JobFlowInstancesConfig()
        .withEc2KeyName(name)
        .withAdditionalMasterSecurityGroups(emrConf.getAdditionalMasterSecurityGroup())
        .withInstanceCount(emrConf.getInstanceCount())
        .withEc2SubnetId(emrConf.getEc2SubnetId())
        .withKeepJobFlowAliveWhenNoSteps(true)
        .withMasterInstanceType(emrConf.getMasterInstanceType())
        .withSlaveInstanceType(emrConf.getWorkerInstanceType()));

    if (emrConf.getLogURI() != null) {
      request.withLogUri(emrConf.getLogURI());
    }
    LOG.info("Creating cluster {}.", name);
    return client.runJobFlow(request).getJobFlowId();
  }

  /**
   * Delete the specified cluster if it exists. This will return after the initial request to delete the cluster
   * is completed. At this point, the cluster is likely not yet deleted, but in a deleting state.
   *
   * @param id the id of the cluster to delete
   */
  public void deleteCluster(String id) {
    LOG.info("Deleting cluster {}.", id);
    client.terminateJobFlows(new TerminateJobFlowsRequest().withJobFlowIds(id));

    AmazonEC2 ec2 = AmazonEC2ClientBuilder.standard()
            .withCredentials(emrConf.getCredentialsProvider())
            .withRegion(emrConf.getRegion())
            .build();

    // named the keypair the same thing as the cluster id
    ec2.deleteKeyPair(new DeleteKeyPairRequest().withKeyName(id));
  }

  /**
   * Get information about the specified cluster. The cluster will not be present if it could not be found.
   *
   * @param id the cluster id
   * @return the cluster information if it exists
   */
  public Optional<co.cask.cdap.runtime.spi.provisioner.Cluster> getCluster(String id) {
    Cluster cluster = describeCluster(id);

    List<Node> nodes = new ArrayList<>();
    // required by RemoteExecutionTwillRunnerService
    Map<String, String> properties = new HashMap<>();
    properties.put("type", "master");
    properties.put("ip.external", cluster.getMasterPublicDnsName());
    nodes.add(new Node("id", System.currentTimeMillis(), properties));

    return Optional.of(new co.cask.cdap.runtime.spi.provisioner.Cluster(
      cluster.getId(), convertStatus(cluster.getStatus()), nodes, Collections.emptyMap()));
  }

  /**
   * Get the status of the specified cluster.
   *
   * @param id the cluster id
   * @return the cluster status
   */
  public co.cask.cdap.runtime.spi.provisioner.ClusterStatus getClusterStatus(String id) {
    return convertStatus(describeCluster(id).getStatus());
  }

  private Cluster describeCluster(String id) {
    return client.describeCluster(new DescribeClusterRequest().withClusterId(id)).getCluster();
  }

  Optional<ClusterSummary> getUnterminatedClusterByName(String name) {
    List<ClusterSummary> clusters = client.listClusters().getClusters();

    List<ClusterSummary> clustersWithSameName = clusters.stream()
            .filter(clusterSummary -> name.equals(clusterSummary.getName()))
            .filter(clusterSummary -> UNTERMINATED_STATES.contains(
                    ClusterState.fromValue(clusterSummary.getStatus().getState())))
            .collect(Collectors.toList());

    if (clustersWithSameName.size() == 0) {
      return Optional.empty();
    } else if (clustersWithSameName.size() == 1) {
      return Optional.of(Iterables.getOnlyElement(clustersWithSameName));
    }
    throw new IllegalStateException("Multiple clusters with the name '" + name + "': " + clustersWithSameName);
  }

  private co.cask.cdap.runtime.spi.provisioner.ClusterStatus convertStatus(ClusterStatus status) {
    switch (ClusterState.fromValue(status.getState())) {
      case BOOTSTRAPPING:
      case STARTING:
        return co.cask.cdap.runtime.spi.provisioner.ClusterStatus.CREATING;
      case WAITING:
      case RUNNING:
        return co.cask.cdap.runtime.spi.provisioner.ClusterStatus.RUNNING;
      case TERMINATING:
        return co.cask.cdap.runtime.spi.provisioner.ClusterStatus.DELETING;
      case TERMINATED:
      case TERMINATED_WITH_ERRORS:
        // we don't returned FAILED, because then that means we will attempt to delete it
        return co.cask.cdap.runtime.spi.provisioner.ClusterStatus.NOT_EXISTS;
      default:
        // unrecognized and unknown
        return co.cask.cdap.runtime.spi.provisioner.ClusterStatus.ORPHANED;
    }
  }

  @Override
  public void close() {
    client.shutdown();
  }
}

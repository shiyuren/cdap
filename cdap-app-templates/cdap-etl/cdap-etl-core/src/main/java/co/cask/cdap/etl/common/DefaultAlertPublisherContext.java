/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.cdap.etl.common;

import co.cask.cdap.api.messaging.MessageFetcher;
import co.cask.cdap.api.messaging.MessagePublisher;
import co.cask.cdap.api.messaging.MessagingAdmin;
import co.cask.cdap.api.messaging.MessagingContext;
import co.cask.cdap.api.messaging.TopicAlreadyExistsException;
import co.cask.cdap.api.messaging.TopicNotFoundException;
import co.cask.cdap.etl.api.AlertPublisherContext;
import co.cask.cdap.etl.api.lineage.field.PipelineOperation;
import co.cask.cdap.etl.spec.StageSpec;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Default implementation of {@link AlertPublisherContext}.
 */
public class DefaultAlertPublisherContext extends AbstractStageContext implements AlertPublisherContext {
  private final MessagingContext messagingContext;
  private final MessagingAdmin messagingAdmin;

  public DefaultAlertPublisherContext(PipelineRuntime pipelineRuntime, StageSpec stageSpec,
                                      MessagingContext messagingContext, MessagingAdmin messagingAdmin) {
    super(pipelineRuntime, stageSpec);
    this.messagingContext = messagingContext;
    this.messagingAdmin = messagingAdmin;
  }

  @Override
  public MessagePublisher getMessagePublisher() {
    return messagingContext.getMessagePublisher();
  }

  @Override
  public MessagePublisher getDirectMessagePublisher() {
    return messagingContext.getDirectMessagePublisher();
  }

  @Override
  public MessageFetcher getMessageFetcher() {
    return messagingContext.getMessageFetcher();
  }

  @Override
  public void createTopic(String topic) throws TopicAlreadyExistsException, IOException {
    messagingAdmin.createTopic(topic);
  }

  @Override
  public void createTopic(String topic,
                          Map<String, String> properties) throws TopicAlreadyExistsException, IOException {
    messagingAdmin.createTopic(topic, properties);
  }

  @Override
  public Map<String, String> getTopicProperties(String topic) throws TopicNotFoundException, IOException {
    return messagingAdmin.getTopicProperties(topic);
  }

  @Override
  public void updateTopic(String topic, Map<String, String> properties) throws TopicNotFoundException, IOException {
    messagingAdmin.updateTopic(topic, properties);
  }

  @Override
  public void deleteTopic(String topic) throws TopicNotFoundException, IOException {
    messagingAdmin.deleteTopic(topic);
  }

  @Override
  public void record(List<PipelineOperation> pipelineOperations) {
    throw new UnsupportedOperationException("Lineage recording is not supported.");
  }
}

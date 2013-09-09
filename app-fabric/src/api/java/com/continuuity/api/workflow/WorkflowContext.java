/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.api.workflow;

/**
 * Represents runtime context of a {@link WorkflowAction}.
 */
public interface WorkflowContext {

  WorkflowSpecification getWorkflowSpecification();

  WorkflowActionSpecification getSpecification();
}

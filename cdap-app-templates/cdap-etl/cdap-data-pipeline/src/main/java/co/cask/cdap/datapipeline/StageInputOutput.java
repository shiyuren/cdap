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

package co.cask.cdap.datapipeline;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * Stores the set of fields which are input and output for a stage.
 */
public class StageInputOutput {
  private final Set<String> inputs;
  private final Set<String> outputs;

  public StageInputOutput(Set<String> inputs, Set<String> outputs) {
    this.inputs = Collections.unmodifiableSet(new HashSet<>(inputs));
    this.outputs = Collections.unmodifiableSet(new HashSet<>(outputs));
  }

  public Set<String> getInputs() {
    return inputs;
  }

  public Set<String> getOutputs() {
    return outputs;
  }
}

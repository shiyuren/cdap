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

package co.cask.cdap.data2.metadata.lineage.field;

import co.cask.cdap.api.lineage.field.EndPoint;
import co.cask.cdap.api.lineage.field.InputField;
import co.cask.cdap.api.lineage.field.Operation;
import co.cask.cdap.api.lineage.field.ReadOperation;
import co.cask.cdap.api.lineage.field.TransformOperation;
import co.cask.cdap.api.lineage.field.WriteOperation;
import co.cask.cdap.internal.guava.reflect.TypeToken;
import co.cask.cdap.proto.codec.OperationTypeAdapter;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Test for {@link FieldLineageInfo}
 */
public class FieldLineageInfoTest {
  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(Operation.class, new OperationTypeAdapter())
    .create();

  @Test
  public void testInvalidOperations() {
    ReadOperation read = new ReadOperation("read", "some read", EndPoint.of("endpoint1"), "offset", "body");
    TransformOperation parse = new TransformOperation("parse", "parse body",
                                                      Collections.singletonList(InputField.of("read", "body")),
                                                      "name", "address");
    WriteOperation write = new WriteOperation("write", "write data", EndPoint.of("ns", "endpoint2"),
            Arrays.asList(InputField.of("read", "offset"),
                    InputField.of("parse", "name"),
                    InputField.of("parse", "body")));

    List<Operation> operations = new ArrayList<>();
    operations.add(parse);
    operations.add(write);

    try {
      // Create info without read operation
      FieldLineageInfo info = new FieldLineageInfo(operations);
      Assert.fail("Field lineage info creation should fail since no read operation is specified.");
    } catch (IllegalArgumentException e) {
      String msg = "Field level lineage requires at least one operation of type 'READ'.";
      Assert.assertEquals(msg, e.getMessage());
    }

    operations.clear();

    operations.add(read);
    operations.add(parse);

    try {
      // Create info without write operation
      FieldLineageInfo info = new FieldLineageInfo(operations);
      Assert.fail("Field lineage info creation should fail since no write operation is specified.");
    } catch (IllegalArgumentException e) {
      String msg = "Field level lineage requires at least one operation of type 'WRITE'.";
      Assert.assertEquals(msg, e.getMessage());
    }

    WriteOperation duplicateWrite = new WriteOperation("write", "write data", EndPoint.of("ns", "endpoint3"),
                                                       Arrays.asList(InputField.of("read", "offset"),
                                                                     InputField.of("parse", "name"),
                                                                     InputField.of("parse", "body")));

    operations.add(write);
    operations.add(duplicateWrite);

    try {
      // Create info with non-unique operation names
      FieldLineageInfo info = new FieldLineageInfo(operations);
      Assert.fail("Field lineage info creation should fail since operation name 'write' is repeated.");
    } catch (IllegalArgumentException e) {
      String msg = "Operation name 'write' is repeated";
      Assert.assertTrue(e.getMessage().contains(msg));
    }

    operations.clear();

    TransformOperation invalidOrigin = new TransformOperation("anotherparse", "parse body",
                                                              Arrays.asList(InputField.of("invalid", "body"),
                                                                            InputField.of("anotherinvalid", "body")),
                                                              "name", "address");

    operations.add(read);
    operations.add(parse);
    operations.add(write);
    operations.add(invalidOrigin);

    try {
      // Create info without invalid origins
      FieldLineageInfo info = new FieldLineageInfo(operations);
      Assert.fail("Field lineage info creation should fail since operation with name 'invalid' " +
              "and 'anotherinvalid' do not exist.");
    } catch (IllegalArgumentException e) {
      String msg = "No operation is associated with the origins '[invalid, anotherinvalid]'.";
      Assert.assertEquals(msg, e.getMessage());
    }
  }

  @Test
  public void testValidOperations() {
    ReadOperation read = new ReadOperation("read", "some read", EndPoint.of("endpoint1"), "offset", "body");
    TransformOperation parse = new TransformOperation("parse", "parse body",
                                                      Collections.singletonList(InputField.of("read", "body")),
                                                      "name", "address");
    WriteOperation write = new WriteOperation("write", "write data", EndPoint.of("ns", "endpoint2"),
                                              Arrays.asList(InputField.of("read", "offset"),
                                                            InputField.of("parse", "name"),
                                                            InputField.of("parse", "body")));

    List<Operation> operations = new ArrayList<>();
    operations.add(read);
    operations.add(write);
    operations.add(parse);
    FieldLineageInfo info1 = new FieldLineageInfo(operations);

    // Serializing and deserializing should result in the same checksum.
    String operationsJson = GSON.toJson(info1.getOperations());
    Type setType = new TypeToken<Set<Operation>>() { }.getType();
    Set<Operation> operationsFromJson = GSON.fromJson(operationsJson, setType);
    FieldLineageInfo info2 = new FieldLineageInfo(operationsFromJson);
    Assert.assertEquals(info1, info2);

    // Create lineage info with different ordering of same operations. Checksum should still be same.
    operations.clear();
    operations.add(write);
    operations.add(parse);
    operations.add(read);

    FieldLineageInfo info3 = new FieldLineageInfo(operations);
    Assert.assertEquals(info1, info3);

    // Change the namespace name of the write operation from ns to myns. The checksum should change now.
    operations.clear();

    WriteOperation anotherWrite = new WriteOperation("write", "write data", EndPoint.of("myns", "endpoint2"),
                                                     Arrays.asList(InputField.of("read", "offset"),
                                                                   InputField.of("parse", "name"),
                                                                   InputField.of("parse", "body")));
    operations.add(anotherWrite);
    operations.add(parse);
    operations.add(read);
    FieldLineageInfo info4 = new FieldLineageInfo(operations);
    Assert.assertNotEquals(info1, info4);
  }

  @Test
  public void testSimpleFieldLineageSummary() {
    // read: file -> (offset, body)
    // parse: (body) -> (first_name, last_name)
    // concat: (first_name, last_name) -> (name)
    // write: (offset, name) -> another_file

    ReadOperation read = new ReadOperation("read", "some read", EndPoint.of("endpoint1"), "offset", "body");

    TransformOperation parse = new TransformOperation("parse", "parsing body",
                                                      Collections.singletonList(InputField.of("read", "body")),
                                                      "first_name", "last_name");

    TransformOperation concat = new TransformOperation("concat", "concatinating the fields",
                                                       Arrays.asList(InputField.of("parse", "first_name"),
                                                                     InputField.of("parse", "last_name")), "name");

    WriteOperation write = new WriteOperation("write_op", "writing data to file",
                                              EndPoint.of("myns", "another_file"),
                                              Arrays.asList(InputField.of("read", "offset"),
                                                            InputField.of("concat", "name")));

    List<Operation> operations = new ArrayList<>();
    operations.add(parse);
    operations.add(concat);
    operations.add(read);
    operations.add(write);

    FieldLineageInfo info = new FieldLineageInfo(operations);

    // EndPoint(myns, another_file) should have two fields: offset and name
    Map<EndPoint, Set<String>> destinationFields = info.getDestinationFields();
    EndPoint destination = EndPoint.of("myns", "another_file");
    Assert.assertEquals(1, destinationFields.size());
    Assert.assertEquals(new HashSet<>(Arrays.asList("offset", "name")), destinationFields.get(destination));

    Map<EndPointField, Set<EndPointField>> incomingSummary = info.getIncomingSummary();
    Map<EndPointField, Set<EndPointField>> outgoingSummary = info.getOutgoingSummary();

    // test incoming summaries

    // offset in the destination is generated from offset field read from source
    EndPointField endPointField = new EndPointField(destination, "offset");
    Set<EndPointField> sourceEndPointFields = incomingSummary.get(endPointField);
    Assert.assertEquals(1, sourceEndPointFields.size());
    EndPointField expectedEndPointField = new EndPointField(EndPoint.of("endpoint1"), "offset");
    Assert.assertEquals(expectedEndPointField, sourceEndPointFields.iterator().next());

    Set<Operation> incomingOperationsForField = info.getIncomingOperationsForField(endPointField);
    Set<Operation> expectedOperations = new HashSet<>();
    expectedOperations.add(write);
    expectedOperations.add(read);
    Assert.assertEquals(expectedOperations, incomingOperationsForField);

    // name in the destination is generated from body field read from source
    endPointField = new EndPointField(destination, "name");
    sourceEndPointFields = incomingSummary.get(endPointField);
    Assert.assertEquals(1, sourceEndPointFields.size());
    expectedEndPointField = new EndPointField(EndPoint.of("endpoint1"), "body");
    Assert.assertEquals(expectedEndPointField, sourceEndPointFields.iterator().next());

    incomingOperationsForField = info.getIncomingOperationsForField(endPointField);
    expectedOperations = new HashSet<>();
    expectedOperations.add(write);
    expectedOperations.add(concat);
    expectedOperations.add(parse);
    expectedOperations.add(read);
    Assert.assertEquals(expectedOperations, incomingOperationsForField);

    // offset in the source should only affect the field offset in the destination
    EndPoint source = EndPoint.of("endpoint1");
    endPointField = new EndPointField(source, "offset");
    Set<EndPointField> destinationEndPointFields = outgoingSummary.get(endPointField);
    Assert.assertEquals(1, destinationEndPointFields.size());
    expectedEndPointField = new EndPointField(EndPoint.of("myns", "another_file"), "offset");
    Assert.assertEquals(expectedEndPointField, destinationEndPointFields.iterator().next());
  }

  @Test
  public void testSourceToMultipleDestinations() {
    // read: file -> (offset, body)
    // parse: body -> (id, name, address, zip)
    // write1: (parse.id, parse.name) -> info
    // write2: (parse.address, parse.zip) -> location

    EndPoint source = EndPoint.of("ns", "file");
    EndPoint info = EndPoint.of("ns", "info");
    EndPoint location = EndPoint.of("ns", "location");

    ReadOperation read = new ReadOperation("read", "Reading from file", source, "offset", "body");
    TransformOperation parse = new TransformOperation("parse", "parsing body",
                                                      Collections.singletonList(InputField.of("read", "body")),
                                                      "id", "name", "address", "zip");
    WriteOperation infoWrite = new WriteOperation("infoWrite", "writing info", info,
                                                  Arrays.asList(InputField.of("parse", "id"),
                                                                InputField.of("parse", "name")));
    WriteOperation locationWrite = new WriteOperation("locationWrite", "writing location", location,
                                                      Arrays.asList(InputField.of("parse", "address"),
                                                                    InputField.of("parse", "zip")));

    List<Operation> operations = new ArrayList<>();
    operations.add(read);
    operations.add(parse);
    operations.add(infoWrite);
    operations.add(locationWrite);

    FieldLineageInfo fllInfo = new FieldLineageInfo(operations);

    Map<EndPoint, Set<String>> destinationFields = fllInfo.getDestinationFields();
    Assert.assertEquals(2, destinationFields.size());
    Assert.assertEquals(new HashSet<>(Arrays.asList("id", "name")), destinationFields.get(info));
    Assert.assertEquals(new HashSet<>(Arrays.asList("address", "zip")), destinationFields.get(location));

    Map<EndPointField, Set<EndPointField>> incomingSummary = fllInfo.getIncomingSummary();
    Assert.assertEquals(4, incomingSummary.size());
    EndPointField expected = new EndPointField(source, "body");
    Assert.assertEquals(1, incomingSummary.get(new EndPointField(info, "id")).size());
    Assert.assertEquals(expected, incomingSummary.get(new EndPointField(info, "id")).iterator().next());
    Assert.assertEquals(1, incomingSummary.get(new EndPointField(info, "name")).size());
    Assert.assertEquals(expected, incomingSummary.get(new EndPointField(info, "name")).iterator().next());
    Assert.assertEquals(1, incomingSummary.get(new EndPointField(location, "address")).size());
    Assert.assertEquals(expected, incomingSummary.get(new EndPointField(location, "address")).iterator().next());
    Assert.assertEquals(1, incomingSummary.get(new EndPointField(location, "zip")).size());
    Assert.assertEquals(expected, incomingSummary.get(new EndPointField(location, "zip")).iterator().next());

    Map<EndPointField, Set<EndPointField>> outgoingSummary = fllInfo.getOutgoingSummary();
    // Note that outgoing summary just contains 1 entry, because offset field from source
    // is not contributing to any destination field
    Assert.assertEquals(1, outgoingSummary.size());

    Set<EndPointField> expectedSet = new HashSet<>();
    expectedSet.add(new EndPointField(info, "id"));
    expectedSet.add(new EndPointField(info, "name"));
    expectedSet.add(new EndPointField(location, "address"));
    expectedSet.add(new EndPointField(location, "zip"));
    Assert.assertEquals(4, outgoingSummary.get(new EndPointField(source, "body")).size());
    Assert.assertEquals(expectedSet, outgoingSummary.get(new EndPointField(source, "body")));
  }

  @Test
  public void testMultiSourceSingleDestinationWithoutMerge() {
    // pRead: personFile -> (offset, body)
    // parse: body -> (id, name, address)
    // cRead: codeFile -> id
    // codeGen: (parse.id, cRead.id) -> id
    // sWrite: (codeGen.id, parse.name, parse.address) -> secureStore
    // iWrite: (parse.id, parse.name, parse.address) -> insecureStore

    EndPoint pEndPoint = EndPoint.of("ns", "personFile");
    EndPoint cEndPoint = EndPoint.of("ns", "codeFile");
    EndPoint sEndPoint = EndPoint.of("ns", "secureStore");
    EndPoint iEndPoint = EndPoint.of("ns", "insecureStore");

    ReadOperation pRead = new ReadOperation("pRead", "Reading from person file", pEndPoint, "offset", "body");

    ReadOperation cRead = new ReadOperation("cRead", "Reading from code file", cEndPoint, "id");

    TransformOperation parse = new TransformOperation("parse", "parsing body",
                                                      Collections.singletonList(InputField.of("pRead", "body")),
                                                      "id", "name", "address");

    TransformOperation codeGen = new TransformOperation("codeGen", "Generate secure code",
                                                        Arrays.asList(InputField.of("parse", "id"),
                                                                      InputField.of("cRead", "id")), "id");

    WriteOperation sWrite = new WriteOperation("sWrite", "writing secure store", sEndPoint,
                                               Arrays.asList(InputField.of("codeGen", "id"),
                                                             InputField.of("parse", "name"),
                                                             InputField.of("parse", "address")));

    WriteOperation iWrite = new WriteOperation("iWrite", "writing insecure store", iEndPoint,
                                               Arrays.asList(InputField.of("parse", "id"),
                                                             InputField.of("parse", "name"),
                                                             InputField.of("parse", "address")));

    List<Operation> operations = new ArrayList<>();
    operations.add(pRead);
    operations.add(cRead);
    operations.add(parse);
    operations.add(codeGen);
    operations.add(sWrite);
    operations.add(iWrite);

    FieldLineageInfo fllInfo = new FieldLineageInfo(operations);
    Map<EndPoint, Set<String>> destinationFields = fllInfo.getDestinationFields();
    Assert.assertEquals(new HashSet<>(Arrays.asList("id", "name", "address")), destinationFields.get(sEndPoint));
    Assert.assertEquals(new HashSet<>(Arrays.asList("id", "name", "address")), destinationFields.get(iEndPoint));
    Assert.assertNull(destinationFields.get(pEndPoint));

    Map<EndPointField, Set<EndPointField>> incomingSummary = fllInfo.getIncomingSummary();
    Assert.assertEquals(6, incomingSummary.size());
    EndPointField expected = new EndPointField(pEndPoint, "body");
    Assert.assertEquals(1, incomingSummary.get(new EndPointField(iEndPoint, "id")).size());
    Assert.assertEquals(expected, incomingSummary.get(new EndPointField(iEndPoint, "id")).iterator().next());
    Assert.assertEquals(1, incomingSummary.get(new EndPointField(iEndPoint, "name")).size());
    Assert.assertEquals(expected, incomingSummary.get(new EndPointField(iEndPoint, "name")).iterator().next());
    Assert.assertEquals(1, incomingSummary.get(new EndPointField(iEndPoint, "address")).size());
    Assert.assertEquals(expected, incomingSummary.get(new EndPointField(iEndPoint, "address")).iterator().next());

    // name and address from secure endpoint also depends on the body field of pEndPoint
    Assert.assertEquals(1, incomingSummary.get(new EndPointField(sEndPoint, "name")).size());
    Assert.assertEquals(expected, incomingSummary.get(new EndPointField(sEndPoint, "name")).iterator().next());
    Assert.assertEquals(1, incomingSummary.get(new EndPointField(sEndPoint, "address")).size());
    Assert.assertEquals(expected, incomingSummary.get(new EndPointField(sEndPoint, "address")).iterator().next());

    // id of secure endpoint depends on both body field of pEndPoint and id field of cEndPoint
    Set<EndPointField> expectedSet = new HashSet<>();
    expectedSet.add(new EndPointField(pEndPoint, "body"));
    expectedSet.add(new EndPointField(cEndPoint, "id"));
    Assert.assertEquals(expectedSet, incomingSummary.get(new EndPointField(sEndPoint, "id")));

    Map<EndPointField, Set<EndPointField>> outgoingSummary = fllInfo.getOutgoingSummary();
    // outgoing summary will not contain offset but only body from pEndPoint and id from cEndPoint
    Assert.assertEquals(2, outgoingSummary.size());

    expectedSet = new HashSet<>();
    expectedSet.add(new EndPointField(iEndPoint, "id"));
    expectedSet.add(new EndPointField(iEndPoint, "name"));
    expectedSet.add(new EndPointField(iEndPoint, "address"));
    expectedSet.add(new EndPointField(sEndPoint, "id"));
    expectedSet.add(new EndPointField(sEndPoint, "name"));
    expectedSet.add(new EndPointField(sEndPoint, "address"));
    // body affects all fields from both secure and insecure endpoints
    Assert.assertEquals(expectedSet, outgoingSummary.get(new EndPointField(pEndPoint, "body")));

    expectedSet.clear();
    expectedSet.add(new EndPointField(sEndPoint, "id"));
    // id field of cEndPoint only affects id field of secure endpoint
    Assert.assertEquals(expectedSet, outgoingSummary.get(new EndPointField(cEndPoint, "id")));

    // Test incoming operations from all destination fields
    Set<Operation> inComingOperations = fllInfo.getIncomingOperationsForField(new EndPointField(iEndPoint, "id"));
    Set<Operation> expectedOperations = new HashSet<>();
    expectedOperations.add(iWrite);
    expectedOperations.add(parse);
    expectedOperations.add(pRead);
    Assert.assertEquals(expectedOperations, inComingOperations);

    inComingOperations = fllInfo.getIncomingOperationsForField(new EndPointField(iEndPoint, "name"));
    expectedOperations = new HashSet<>();
    expectedOperations.add(iWrite);
    expectedOperations.add(parse);
    expectedOperations.add(pRead);
    Assert.assertEquals(new FieldLineageInfo(expectedOperations), new FieldLineageInfo(inComingOperations));

    inComingOperations = fllInfo.getIncomingOperationsForField(new EndPointField(iEndPoint, "address"));
    expectedOperations = new HashSet<>();
    expectedOperations.add(iWrite);
    expectedOperations.add(parse);
    expectedOperations.add(pRead);
    Assert.assertEquals(expectedOperations, inComingOperations);

    inComingOperations = fllInfo.getIncomingOperationsForField(new EndPointField(sEndPoint, "id"));
    expectedOperations = new HashSet<>();
    expectedOperations.add(sWrite);
    expectedOperations.add(codeGen);
    expectedOperations.add(cRead);
    expectedOperations.add(parse);
    expectedOperations.add(pRead);
    Assert.assertEquals(expectedOperations, inComingOperations);

    inComingOperations = fllInfo.getIncomingOperationsForField(new EndPointField(sEndPoint, "name"));
    expectedOperations = new HashSet<>();
    expectedOperations.add(sWrite);
    expectedOperations.add(parse);
    expectedOperations.add(pRead);
    Assert.assertEquals(expectedOperations, inComingOperations);

    inComingOperations = fllInfo.getIncomingOperationsForField(new EndPointField(sEndPoint, "address"));
    expectedOperations = new HashSet<>();
    expectedOperations.add(sWrite);
    expectedOperations.add(parse);
    expectedOperations.add(pRead);
    Assert.assertEquals(expectedOperations, inComingOperations);
  }

  @Test
  public void testMultiPathFieldLineage() {
    // read1: file1 -> (offset, body)
    // read2: file2 -> (offset, body)
    // merge: (read1.offset, read1.body, read2.offset, read2.body) -> (offset, body)
    // parse: (merge.body) -> (name,address)
    // write: (parse.name, parse.address, merge.offset) -> file

    EndPoint read1EndPoint = EndPoint.of("ns1", "file1");
    EndPoint read2EndPoint = EndPoint.of("ns2", "file2");
    EndPoint fileEndPoint = EndPoint.of("ns3", "file");

    ReadOperation read1 = new ReadOperation("read1", "Reading from file1", read1EndPoint, "offset", "body");

    ReadOperation read2 = new ReadOperation("read2", "Reading from file2", read2EndPoint, "offset", "body");

    TransformOperation merge = new TransformOperation("merge", "merging fields",
                                                      Arrays.asList(InputField.of("read1", "offset"),
                                                                    InputField.of("read2", "offset"),
                                                                    InputField.of("read1", "body"),
                                                                    InputField.of("read2", "body")), "offset", "body");

    TransformOperation parse = new TransformOperation("parse", "parsing body",
                                                      Collections.singletonList(InputField.of("merge", "body")),
                                                      "name", "address");

    WriteOperation write = new WriteOperation("write", "writing to another file", fileEndPoint,
                                              Arrays.asList(InputField.of("merge", "offset"),
                                                            InputField.of("parse", "name"),
                                                            InputField.of("parse", "address")));

    List<Operation> operations = new ArrayList<>();
    operations.add(parse);
    operations.add(merge);
    operations.add(read1);
    operations.add(read2);
    operations.add(write);
    FieldLineageInfo fllInfo = new FieldLineageInfo(operations);

    Map<EndPoint, Set<String>> destinationFields = fllInfo.getDestinationFields();
    Assert.assertEquals(1, destinationFields.size());
    Assert.assertEquals(new HashSet<>(Arrays.asList("name", "address", "offset")), destinationFields.get(fileEndPoint));

    Map<EndPointField, Set<EndPointField>> incomingSummary = fllInfo.getIncomingSummary();
    Assert.assertEquals(3, incomingSummary.size());

    Set<EndPointField> expectedSet = new HashSet<>();
    expectedSet.add(new EndPointField(read1EndPoint, "body"));
    expectedSet.add(new EndPointField(read1EndPoint, "offset"));
    expectedSet.add(new EndPointField(read2EndPoint, "body"));
    expectedSet.add(new EndPointField(read2EndPoint, "offset"));
    Assert.assertEquals(expectedSet, incomingSummary.get(new EndPointField(fileEndPoint, "name")));
    Assert.assertEquals(expectedSet, incomingSummary.get(new EndPointField(fileEndPoint, "address")));
    Assert.assertEquals(expectedSet, incomingSummary.get(new EndPointField(fileEndPoint, "offset")));

    Map<EndPointField, Set<EndPointField>> outgoingSummary = fllInfo.getOutgoingSummary();
    Assert.assertEquals(4, outgoingSummary.size());

    expectedSet = new HashSet<>();
    expectedSet.add(new EndPointField(fileEndPoint, "offset"));
    expectedSet.add(new EndPointField(fileEndPoint, "name"));
    expectedSet.add(new EndPointField(fileEndPoint, "address"));
    Assert.assertEquals(expectedSet, outgoingSummary.get(new EndPointField(read1EndPoint, "offset")));
    Assert.assertEquals(expectedSet, outgoingSummary.get(new EndPointField(read1EndPoint, "body")));
    Assert.assertEquals(expectedSet, outgoingSummary.get(new EndPointField(read2EndPoint, "offset")));
    Assert.assertEquals(expectedSet, outgoingSummary.get(new EndPointField(read2EndPoint, "body")));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testCycle() {
    EndPoint readEndPoint = EndPoint.of("ns", "file1");
    EndPoint writeEndPoint = EndPoint.of("ns", "file2");

    ReadOperation read = new ReadOperation("read", "read", readEndPoint, "offset", "body");
    TransformOperation parse = new TransformOperation("parse", "parse",
                                                      Arrays.asList(InputField.of("read", "body"),
                                                                    InputField.of("normalize", "name")),
                                                      "name", "address");
    TransformOperation normalize = new TransformOperation("normalize", "normalize",
                                                          Collections.singletonList(InputField.of("parse", "name")),
                                                          "name");
    WriteOperation write = new WriteOperation("write", "writing to another file", writeEndPoint,
                                              Arrays.asList(InputField.of("normalize", "name"),
                                                            InputField.of("parse", "address")));

    List<Operation> operations = new ArrayList<>();
    operations.add(parse);
    operations.add(read);
    operations.add(normalize);
    operations.add(write);
    FieldLineageInfo.getTopologicallySortedOperations(new HashSet<>(operations));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testCycleWithNonExistentOperationNames() {
    EndPoint readEndPoint = EndPoint.of("ns", "file1");
    EndPoint writeEndPoint = EndPoint.of("ns", "file2");

    ReadOperation read = new ReadOperation("read", "read", readEndPoint, "offset", "body");
    TransformOperation parse = new TransformOperation("parse", "parse",
                                                      Arrays.asList(InputField.of("read", "body"),
                                                                    InputField.of("normalize", "name"),
                                                                    InputField.of("nop1", "field1")),
                                                      "name", "address");
    TransformOperation normalize = new TransformOperation("normalize", "normalize",
                                                          Arrays.asList(InputField.of("parse", "name"),
                                                                        InputField.of("nop2", "field2")),
                                                          "name");
    WriteOperation write = new WriteOperation("write", "writing to another file", writeEndPoint,
                                              Arrays.asList(InputField.of("normalize", "name"),
                                                            InputField.of("parse", "address"),
                                                            InputField.of("nop3", "field3")));

    List<Operation> operations = new ArrayList<>();
    operations.add(parse);
    operations.add(read);
    operations.add(normalize);
    operations.add(write);
    FieldLineageInfo.getTopologicallySortedOperations(new HashSet<>(operations));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testSelfReferentialOperations() {
    TransformOperation parse = new TransformOperation("parse", "parse",
                                                      Arrays.asList(InputField.of("read", "body"),
                                                                    InputField.of("parse", "name")),
                                                      "name", "address");
    FieldLineageInfo.getTopologicallySortedOperations(Collections.singleton(parse));
  }

  @Test
  public void testLinearTopologicalSort() {
    // read---->parse---->normalize--->write
    ReadOperation read = new ReadOperation("read", "read descr", EndPoint.of("ns", "input"), "offset", "body");
    TransformOperation parse = new TransformOperation("parse", "parse descr",
                                                      Collections.singletonList(InputField.of("read", "body")), "name",
                                                      "address");
    TransformOperation normalize = new TransformOperation("normalize", "normalize descr",
                                                          Collections.singletonList(InputField.of("parse", "address")),
                                                          "address");
    List<InputField> writeInputs = new ArrayList<>();
    writeInputs.add(InputField.of("parse", "name"));
    writeInputs.add(InputField.of("normalize", "address"));
    WriteOperation write = new WriteOperation("write", "write descr", EndPoint.of("ns", "output"), writeInputs);

    Set<Operation> operations = new LinkedHashSet<>();
    operations.add(read);
    operations.add(parse);
    operations.add(normalize);
    operations.add(write);

    List<Operation> topologicallySortedOperations = FieldLineageInfo.getTopologicallySortedOperations(operations);
    assertBefore(topologicallySortedOperations, read, parse);
    assertBefore(topologicallySortedOperations, parse, normalize);
    assertBefore(topologicallySortedOperations, normalize, write);
    assertBefore(topologicallySortedOperations, read, write);

    // try with few different insertion orders, the topological sort should give the same results
    operations = new LinkedHashSet<>();
    operations.add(parse);
    operations.add(normalize);
    operations.add(write);
    operations.add(read);
    topologicallySortedOperations = FieldLineageInfo.getTopologicallySortedOperations(operations);
    assertBefore(topologicallySortedOperations, read, parse);
    assertBefore(topologicallySortedOperations, parse, normalize);
    assertBefore(topologicallySortedOperations, normalize, write);
    assertBefore(topologicallySortedOperations, read, write);

    operations = new LinkedHashSet<>();
    operations.add(write);
    operations.add(normalize);
    operations.add(parse);
    operations.add(read);
    topologicallySortedOperations = FieldLineageInfo.getTopologicallySortedOperations(operations);
    assertBefore(topologicallySortedOperations, read, parse);
    assertBefore(topologicallySortedOperations, parse, normalize);
    assertBefore(topologicallySortedOperations, normalize, write);
    assertBefore(topologicallySortedOperations, read, write);
  }

  @Test
  public void testBranchTopologicalSort() {
    // read----------------------write
    //   \                      /
    //    ----parse---normalize

    ReadOperation read = new ReadOperation("read", "read descr", EndPoint.of("ns", "input"), "offset", "body");
    TransformOperation parse = new TransformOperation("parse", "parse descr",
                                                      Collections.singletonList(InputField.of("read", "body")), "name",
                                                      "address");
    TransformOperation normalize = new TransformOperation("normalize", "normalize descr",
                                                          Collections.singletonList(InputField.of("parse", "address")),
                                                          "address");
    List<InputField> writeInputs = new ArrayList<>();
    writeInputs.add(InputField.of("read", "offset"));
    writeInputs.add(InputField.of("parse", "name"));
    writeInputs.add(InputField.of("normalize", "address"));
    WriteOperation write = new WriteOperation("write", "write descr", EndPoint.of("ns", "output"), writeInputs);

    Set<Operation> operations = new LinkedHashSet<>();
    operations.add(read);
    operations.add(parse);
    operations.add(normalize);
    operations.add(write);

    List<Operation> topologicallySortedOperations = FieldLineageInfo.getTopologicallySortedOperations(operations);
    assertBefore(topologicallySortedOperations, read, parse);
    assertBefore(topologicallySortedOperations, parse, normalize);
    assertBefore(topologicallySortedOperations, normalize, write);
    assertBefore(topologicallySortedOperations, read, write);

    // try with different insertion orders
    operations = new LinkedHashSet<>();
    operations.add(parse);
    operations.add(normalize);
    operations.add(write);
    operations.add(read);

    topologicallySortedOperations = FieldLineageInfo.getTopologicallySortedOperations(operations);
    assertBefore(topologicallySortedOperations, read, parse);
    assertBefore(topologicallySortedOperations, parse, normalize);
    assertBefore(topologicallySortedOperations, normalize, write);
    assertBefore(topologicallySortedOperations, read, write);

    operations = new LinkedHashSet<>();
    operations.add(write);
    operations.add(normalize);
    operations.add(parse);
    operations.add(read);

    topologicallySortedOperations = FieldLineageInfo.getTopologicallySortedOperations(operations);
    assertBefore(topologicallySortedOperations, read, parse);
    assertBefore(topologicallySortedOperations, parse, normalize);
    assertBefore(topologicallySortedOperations, normalize, write);
    assertBefore(topologicallySortedOperations, read, write);

    // When the field lineage is queried for offset field, we will only return the
    // read and write operations, since parse and normalize operations are not affecting
    // the offset field in anyway. In this case even though write operation has input with origin
    // as normalize, topological sort should not affect by this case, where normalize operation
    // itself is missing.
    operations = new LinkedHashSet<>();
    operations.add(write);
    operations.add(read);

    topologicallySortedOperations = FieldLineageInfo.getTopologicallySortedOperations(operations);
    assertBefore(topologicallySortedOperations, read, write);
  }

  @Test
  public void testDisjointBranches() {
    // read1 -----> write1
    // read2 -----> write2
    ReadOperation read1 = new ReadOperation("read1", "read descr", EndPoint.of("ns", "input1"), "offset", "body");
    WriteOperation write1 = new WriteOperation("write1", "write descr", EndPoint.of("ns", "output"),
                                               InputField.of("read1", "offset"));

    ReadOperation read2 = new ReadOperation("read2", "read descr", EndPoint.of("ns", "input2"), "offset", "body");
    WriteOperation write2 = new WriteOperation("write2", "write descr", EndPoint.of("ns", "output"),
                                               InputField.of("read2", "offset"));

    Set<Operation> operations = new LinkedHashSet<>();
    operations.add(write1);
    operations.add(write2);
    operations.add(read2);
    operations.add(read1);

    List<Operation> topologicallySortedOperations = FieldLineageInfo.getTopologicallySortedOperations(operations);
    assertBefore(topologicallySortedOperations, read1, write1);
    assertBefore(topologicallySortedOperations, read2, write2);
  }

  private void assertBefore(List<Operation> list, Operation a, Operation b) {
    int aIndex = list.indexOf(a);
    int bIndex = list.indexOf(b);
    Assert.assertTrue(aIndex < bIndex);
  }
}

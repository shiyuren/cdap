/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.metrics.collect;

import com.continuuity.metrics.transport.MetricRecord;
import com.continuuity.metrics.transport.TagMetric;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;
import com.google.common.collect.Maps;
import org.junit.Assert;
import org.junit.Test;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Testing the basic properties of the {@link AggregatedMetricsCollectionService}.
 */
public class AggregatedMetricsCollectionServiceTest {

  @Test
  public void testPublish() throws InterruptedException {
    final BlockingQueue<MetricRecord> published = new LinkedBlockingQueue<MetricRecord>();

    AggregatedMetricsCollectionService service = new AggregatedMetricsCollectionService() {
      @Override
      protected void publish(Iterator<MetricRecord> metrics) {
        Iterators.addAll(published, metrics);
      }

      @Override
      protected Scheduler scheduler() {
        return Scheduler.newFixedRateSchedule(5, 1, TimeUnit.SECONDS);
      }
    };

    service.startAndWait();
    try {
      // Publish couple metrics, they should be aggregated.
      service.getCollector("context", "metric").gauge(1);
      service.getCollector("context", "metric").gauge(2);
      service.getCollector("context", "metric").gauge(3);
      service.getCollector("context", "metric").gauge(4);

      MetricRecord record = published.poll(10, TimeUnit.SECONDS);
      Assert.assertNotNull(record);
      Assert.assertEquals(10, record.getValue());

      // No publishing for 0 value metrics
      Assert.assertNull(published.poll(3, TimeUnit.SECONDS));

      // Publish a metric and wait for it so that we know there is around 1 second to publish more metrics to test.
      service.getCollector("context", "metric").gauge(1);
      Assert.assertNotNull(published.poll(3, TimeUnit.SECONDS));

      // Publish metrics with tags
      service.getCollector("context", "metric").gauge(3, "tag1", "tag2");
      service.getCollector("context", "metric").gauge(4, "tag2", "tag3");

      record = published.poll(3, TimeUnit.SECONDS);
      Assert.assertNotNull(record);
      Assert.assertEquals(7, record.getValue());

      // Verify tags are aggregated individually.
      Map<String, Integer> tagMetrics = Maps.newHashMap();
      for (TagMetric tagMetric : record.getTags()) {
        tagMetrics.put(tagMetric.getTag(), tagMetric.getValue());
      }
      Assert.assertEquals(ImmutableMap.of("tag1", 3, "tag2", 7, "tag3", 4), tagMetrics);

    } finally {
      service.stopAndWait();
    }
  }
}

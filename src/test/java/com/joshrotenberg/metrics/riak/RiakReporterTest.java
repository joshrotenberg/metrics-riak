package com.joshrotenberg.metrics;

import org.junit.BeforeClass;
import org.junit.Before;
import org.junit.Test;

import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.codahale.metrics.*;
import static com.codahale.metrics.MetricRegistry.name;

public class RiakReporterTest {

	@Test
	public void NoTest() throws Exception {
	    MetricRegistry registry = new MetricRegistry();
	    RiakReporter reporter = RiakReporter.forRegistry(registry).build();
	    Counter c = registry.counter(name(RiakReporterTest.class, "counter"));
	    c.inc(201);
	    reporter.report();
	    assertThat(1 == 1);
	}
}

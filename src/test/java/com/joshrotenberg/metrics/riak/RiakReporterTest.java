package com.joshrotenberg.metrics.riak;

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
	    RiakReporter reporter = RiakReporter
		.forRegistry(registry)
		.bucketNamer(new Namer() {
			@Override
			public String getName(String key) {
			    return "bar";
			}
		    })
		.build();
	    Counter c = registry.counter(name(RiakReporterTest.class, "counter"));
	    c.inc(201);

	    registry.register(name(RiakReporterTest.class, "gauge"),
			     new Gauge<Integer>() {
				 @Override
				 public Integer getValue() {
				     return 1000;
				 }
			     });
	    Meter m = registry.meter(name(RiakReporterTest.class, "meter"));
	    m.mark(11);
				    
	    reporter.report();
	    assertThat(1 == 1);
	}
}

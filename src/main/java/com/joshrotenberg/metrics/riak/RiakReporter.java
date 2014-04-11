package com.joshrotenberg.metrics.riak;

import com.codahale.metrics.ScheduledReporter;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.Clock;
import com.codahale.metrics.Metric;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;

import com.codahale.metrics.json.MetricsModule;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.JsonProcessingException;

import com.basho.riak.client.IRiakClient;
import com.basho.riak.client.IRiakObject;
import com.basho.riak.client.RiakFactory;
import com.basho.riak.client.RiakException;
import com.basho.riak.client.RiakRetryFailedException;
import com.basho.riak.client.bucket.Bucket;

import java.text.DateFormat;
import java.util.*;
import java.util.concurrent.TimeUnit;


/**
 * A reporter which outputs measurements to Riak.
 */
public class RiakReporter extends ScheduledReporter {
    /**
     * Returns a new {@link Builder} for {@link RiakReporter}.
     *
     * @param registry the registry to report
     * @return a {@link Builder} instance for a {@link RiakReporter}
     */
    public static Builder forRegistry(MetricRegistry registry) {
        return new Builder(registry);
    }

    /**
     * A builder for {@link RiakReporter} instances. 
     */
    public static class Builder {
        private final MetricRegistry registry;
        private String host;
	private int port;
	private String bucket;
	private Namer bucketNamer;
        private Locale locale;
        private Clock clock;
        private TimeZone timeZone;
        private TimeUnit rateUnit;
        private TimeUnit durationUnit;
        private MetricFilter filter;

        private Builder(MetricRegistry registry) {
            this.registry = registry;
	    this.host = "127.0.0.1";
	    this.port = 8087;
	    this.bucket = "metrics";
	    this.bucketNamer = null;
            this.locale = Locale.getDefault();
            this.clock = Clock.defaultClock();
            this.timeZone = TimeZone.getDefault();
            this.rateUnit = TimeUnit.SECONDS;
            this.durationUnit = TimeUnit.MILLISECONDS;
            this.filter = MetricFilter.ALL;
        }

	/**
	 * Write to the given Riak host.
	 *
	 * @param host a
	 * @return {@code this}
	 */
	public Builder host(String host) {
	    this.host = host;
	    return this;
	}

	/**
	 * Use the given port on the Riak host.
	 *
	 * @param port a
	 * @return {@code this}
	 */
	public Builder port(int port) {
	    this.port = port;
	    return this;
	}

	/**
	 * Use the given Riak bucket for metrics.
	 *
	 * @param bucket a
	 * @return {@code this}
	 */
	public Builder bucket(String bucket) {
	    this.bucket = bucket;
	    return this;
	}

	public Builder bucketNamer(Namer namer) {
	    this.bucketNamer = namer;
	    return this;
	}

        /**
         * Format numbers for the given {@link Locale}.
         *
         * @param locale a {@link Locale}
         * @return {@code this}
         */
        public Builder formattedFor(Locale locale) {
            this.locale = locale;
            return this;
        }

        /**
         * Use the given {@link Clock} instance for the time.
         *
         * @param clock a {@link Clock} instance
         * @return {@code this}
         */
        public Builder withClock(Clock clock) {
            this.clock = clock;
            return this;
        }

        /**
         * Use the given {@link TimeZone} for the time.
         *
         * @param timeZone a {@link TimeZone}
         * @return {@code this}
         */
        public Builder formattedFor(TimeZone timeZone) {
            this.timeZone = timeZone;
            return this;
        }

        /**
         * Convert rates to the given time unit.
         *
         * @param rateUnit a unit of time
         * @return {@code this}
         */
        public Builder convertRatesTo(TimeUnit rateUnit) {
            this.rateUnit = rateUnit;
            return this;
        }

        /**
         * Convert durations to the given time unit.
         *
         * @param durationUnit a unit of time
         * @return {@code this}
         */
        public Builder convertDurationsTo(TimeUnit durationUnit) {
            this.durationUnit = durationUnit;
            return this;
        }

        /**
         * Only report metrics which match the given filter.
         *
         * @param filter a {@link MetricFilter}
         * @return {@code this}
         */
        public Builder filter(MetricFilter filter) {
            this.filter = filter;
            return this;
        }

        /**
         * Builds a {@link RiakReporter} with the given properties.
         *
         * @return a {@link RiakReporter}
         */
        public RiakReporter build() throws RiakException {
            return new RiakReporter(registry,
				    host,
				    port,
				    bucket,
				    bucketNamer,
				    locale,
				    clock,
				    timeZone,
				    rateUnit,
				    durationUnit,
				    filter);
        }
    }

    private final String host;
    private final int port;
    private final String bucket;
    private final Namer bucketNamer;
    private final Locale locale;
    private final Clock clock;
    private final DateFormat dateFormat;
    private final IRiakClient pbClient;
    private final Bucket riakBucket;
    private final ObjectMapper mapper;

    private RiakReporter(MetricRegistry registry,
			 String host,
			 int port,
			 String bucket,
			 Namer bucketNamer,
			 Locale locale,
			 Clock clock,
			 TimeZone timeZone,
			 TimeUnit rateUnit,
			 TimeUnit durationUnit,
			 MetricFilter filter) throws RiakException {
        super(registry, "riak-reporter", filter, rateUnit, durationUnit);
	this.host = host;
	this.port = port;
	this.bucket = bucket;
	this.bucketNamer = bucketNamer;
        this.locale = locale;
        this.clock = clock;
        this.dateFormat = DateFormat.getDateTimeInstance(DateFormat.SHORT,
                                                         DateFormat.MEDIUM,
                                                         locale);
        dateFormat.setTimeZone(timeZone);

	mapper = new ObjectMapper().registerModule(
						   new MetricsModule(TimeUnit.SECONDS, TimeUnit.MILLISECONDS, false));

	pbClient = RiakFactory.pbcClient(this.host, this.port);
        riakBucket = pbClient.fetchBucket(this.bucket).execute();
	if(this.bucketNamer != null)
	    System.out.println(this.bucketNamer.getName("foo"));
    }

    @Override
    public void report(SortedMap<String, Gauge> gauges,
                       SortedMap<String, Counter> counters,
                       SortedMap<String, Histogram> histograms,
                       SortedMap<String, Meter> meters,
                       SortedMap<String, Timer> timers)  {

	try {

	    if (!gauges.isEmpty()) {
		for (Map.Entry<String, Gauge> entry : gauges.entrySet()) {
		    storeMetric(entry.getKey(), entry.getValue());
		}
	    }
	    
	    if (!counters.isEmpty()) {
		for (Map.Entry<String, Counter> entry : counters.entrySet()) {
		    storeMetric(entry.getKey(), entry.getValue());
		}
	    }
	    
	    if (!histograms.isEmpty()) {
		for (Map.Entry<String, Histogram> entry : histograms.entrySet()) {
		    storeMetric(entry.getKey(), entry.getValue());
		}
	    }

	    if (!meters.isEmpty()) {
		for (Map.Entry<String, Meter> entry : meters.entrySet()) {
		    storeMetric(entry.getKey(), entry.getValue());
		}
	    }
	    
	    if (!timers.isEmpty()) {
		for (Map.Entry<String, Timer> entry : timers.entrySet()) {
		    storeMetric(entry.getKey(), entry.getValue());
		}
	    }
	    
	} catch (RiakRetryFailedException e) {
	    e.printStackTrace();
	} catch (JsonProcessingException e) {
	    e.printStackTrace();
	}
    }

    private void storeMetric(String key, Metric metric) 
	throws RiakRetryFailedException,
	       JsonProcessingException {

	riakBucket.store(key, 
			 mapper.writeValueAsString(metric)).execute();
    }

}

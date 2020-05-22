## Bug: NPE

Did not rebuild metric after query.

```
2020-05-18 21:04:10,287 ERROR [qtp1723422310-2020] o.o.w.r.v.MeasurementsRestService: Fetch failed: java.util.concurrent.ExecutionException: java.lang.NullPointerException
org.opennms.netmgt.measurements.api.exceptions.FetchException: Fetch failed: java.util.concurrent.ExecutionException: java.lang.NullPointerException
  at org.opennms.netmgt.measurements.api.DefaultMeasurementsService.query(DefaultMeasurementsService.java:78) ~[org.opennms.features.measurements.api-27.0.0-SNAPSHOT.jar:?]
  at org.opennms.web.rest.v1.MeasurementsRestService.query(MeasurementsRestService.java:176) [org.opennms.features.measurements.rest-27.0.0-SNAPSHOT.jar:?]
  at org.opennms.web.rest.v1.MeasurementsRestService$$FastClassBySpringCGLIB$$a66decc9.invoke(<generated>) [org.opennms.features.measurements.rest-27.0.0-SNAPSHOT.jar:?]
  at org.springframework.cglib.proxy.MethodProxy.invoke(MethodProxy.java:204) [org.apache.servicemix.bundles.spring-core-4.2.9.RELEASE_1.jar:?]
  at org.springframework.aop.framework.CglibAopProxy$CglibMethodInvocation.invokeJoinpoint(CglibAopProxy.java:720) [org.apache.servicemix.bundles.spring-aop-4.2.9.RELEASE_1.jar:?]
  at org.springframework.aop.framework.ReflectiveMethodInvocation.proceed(ReflectiveMethodInvocation.java:157) [org.apache.servicemix.bundles.spring-aop-4.2.9.RELEASE_1.jar:?]
```

## Improvement: Optimize queries

This function: `List<Metric> getMetrics(Collection<Tag> tags)` gets called way too many times:

```
Retrieving metrics for tags: [_idx1=(snmp:2,4)]
...
Retrieving metrics for tags: [_idx2=(snmp:2:lgpEnvTemperatureIdDegF,5)]
...
Retrieving metrics for tags: [_idx3=(snmp:2:sinkProducerMetrics:Telemetry-Netflow-9,5)]
```

### Algo

Take first 2 parts of _idx* value i.e. snmp:2
* TODO: What does an FS:FID look like?

Lookup in cache

On cache miss -  search for _idx_key=snmp:2

Store for 60s  

Apply matchers to the tags

On write, append only if entry is present in cache - i.e. do no trigger a load

This results in us having to wait for 1 round-trip: retrieving all known resources (sets of labels) for the node

## Improvement: More tags

Must be able to pull in:
* Node label
* FS
* FID
* Categories - catProduction:true
* Asset fields
* Meta-data

# jocker-grpc-loadbalancing
Lightweight utility which exposes built-in gRPC features including load balancing,
high availability (retries and request hedging), health checks without any 
additional entities. No sidecars, proxies or external control planes. 
Just client, server and tiny fluent configuration :)

Example usage is as follows:

```scala
// Initialize configuration
val conf = new BalancingNameResolverConfig("server1:10", "server2:11")    // The replicas to send RPC's to
    .withLoadBalancingPolicy(LoadBalancingPolicies.round_robin)           // Enable load balancing
    .withTimeoutMs(100)                                                   // Set the overall timeout to 100ms
    .withService[SimpleServiceGrpc]                                       // Sets the name of the service 
    .withRetryPolicy(RetryPolicies.hedge)                                 // Enable hedging (speculative retries)
    .withMaxAttempts(4)                                                   // Set maximum number of requests to 4
    .withDelayMs(10)                                                      // Configure first hedged call after 10 ms
    .withRetryableStatusCodes(StatusCodes.UNAVAILABLE)                    // Retry call if the server is unavailable

// Create channel using the configuraiont
val channel: ManagedChannel = NettyChannelBuilder
    .forTarget("lbs://myservice")                                        // The host name from here will be used as authority, lbs is the default schema
    .enableRetry()                                                       // This is required to enable either retry or hedging
    .nameResolverFactory(conf.toProvider)                                // Inject real server names and configuration
    .build()

// Now create gRPC stub to access the server (blocking or asynchronous)
val client = SimpleServiceGrpc.newBlockingStub(channel)

// Resolver can be registered in resolver's registry, although it is not recommended
// because it'll cause all connection with given schema to share same configuration
NameResolverRegistry.getDefaultRegistry.register(
  resolverConfig.withSchema("lbs").toProvider)
```

For more configuration examples see the unit test.

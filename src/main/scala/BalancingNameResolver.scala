package org.jocker.grpc.loadbalancer

import LoadBalancingPolicies.LoadBalancingPolicy
import RetryPolicies.RetryPolicy
import StatusCodes.StatusCode

import com.typesafe.scalalogging.StrictLogging
import io.grpc.NameResolver.{Factory, Listener2}
import io.grpc.internal.{DnsNameResolver, DnsNameResolverProvider}
import io.grpc.{EquivalentAddressGroup, LoadBalancer, NameResolver, NameResolverProvider, Status}

import java.net.URI
import java.util
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.Random

/**
 * Balancing name resolver can be used to implement client-side load balancing for gRPC client
 * relying on build-in gRPC features like load balancing, timeouts, retries and hedging which are
 * rather hard to use directly. See details at https://github.com/grpc/proposal/blob/master/A6-client-retries.md
 *
 * In order to use it, just add as a name resolver provider with proper configuration, for example:
 *
 * {{{
 *   val channel: ManagedChannel = NettyChannelBuilder
 *         .forTarget("lbs://myservice") // The host name from here will be used as authority
 *         .enableRetry() // This is required to enable either retry or hedging
 *         .nameResolverFactory(
 *            new BalancingNameResolverConfig("server1:10", "server2:11")    // The replicas to send RPC's to
 *              .withLoadBalancingPolicy(LoadBalancingPolicies.round_robin)  // Enable load balancing
 *              .withTimeoutMs(100)                                          // Set the overal timeout to 100ms
 *              .withServiceName("grpc.testing.SimpleService")                 // Configure the name of the service
 *              .withRetryPolicy(RetryPolicies.hedge)                        // Enable hedging (speculative retries)
 *              .withMaxAttempts(4)                                          // Set maximum number of requests to 4
 *              .withDelayMs(10)                                             // Configure first hedged call after 10 ms
 *              .withRetryableStatusCodes(StatusCodes.UNAVAILABLE)           // Retry call if the server is unavailable
 *              .toProvider)
 *         .build()
 * }}}
 *
 * @param replicas                 Replicas to send requests to in the form of hostnames or hostname:port
 * @param loadBalancingPolicy      Load balancing policy to use, see [[LoadBalancingPolicies]], the default is pick_first
 * @param timeoutMs                Maximum time in milliseconds for the entire call processing. After this time passes, call will fail
 *                                 with DEADLINE_EXCEEDED
 * @param serviceName              Name of the service is required to apply retry configuration properly. Should be
 *                                 package_name.service_name as in protobuf file (eg. sberstore.FeatureStorage)
 * @param retryPolicy              How to handle retries, see [[RetryPolicies]], default os none. There are basically two strategies
 *                    - retry after failure, works for exceptions or network failures, but no fo timeouts
 *                    - hedge - issue a speculative RPC after time limit or failure. This is the recommended way if you
 *                      need to handle timeouts
 * @param maxAttempts              Maximum number of RPC calls issued
 * @param delayMs                  Delay injected before retry (for retry strategy) or delay before issuing speculative call
 *                                 (for hedging). The delay is NOT injected on UNAVAILABLE status returned (no connection to the server)
 * @param retriesBackOffMultiplier Multiplication factor to be used to increase delay between retry calls, works
 *                                 only for rerty.
 * @param maxBackOffMs             Maximum value to which delay may grow with multiplier
 * @param retryableStatusCodes     Status codes for which we should attempt to retry
 * @param healthCheckService       Name of the service to use for health checking. If not specified,
 *                                 no health checking performed.
 * @param schema                   Default schema for created provider, used with [[NameResolverRegistry]], defaults to lbs
 * @param priority                 Priority for provider selection, used with [[NameResolverRegistry]], defaults to 5
 */
case class BalancingNameResolverConfig
(
  replicas: Set[String],
  loadBalancingPolicy: LoadBalancingPolicy = LoadBalancingPolicies.pick_first,
  timeoutMs: Int = 0,
  serviceName: String = "",
  retryPolicy: RetryPolicy = RetryPolicies.none,
  maxAttempts: Int = 2,
  delayMs: Int = 0,
  retriesBackOffMultiplier: Double = 1,
  maxBackOffMs: Int = 10000,
  retryableStatusCodes: Set[StatusCode] = Set(StatusCodes.UNAVAILABLE),
  healthCheckService: Option[String] = None,
  schema: String = "lbs",
  priority: Int = 5
) extends StrictLogging {

  def this(replicas: String*) = this(replicas.toSet)

  def withLoadBalancingPolicy(loadBalancingPolicy: LoadBalancingPolicy): BalancingNameResolverConfig =
    copy(loadBalancingPolicy = loadBalancingPolicy)

  def withTimeoutMs(timeoutMs: Int): BalancingNameResolverConfig =
    copy(timeoutMs = timeoutMs)

  def withServiceName(serviceName: String): BalancingNameResolverConfig =
    copy(serviceName = serviceName)

  def withRetryPolicy(retryPolicy: RetryPolicy): BalancingNameResolverConfig =
    copy(retryPolicy = retryPolicy)

  def withMaxAttempts(maxAttempts: Int): BalancingNameResolverConfig =
    copy(maxAttempts = maxAttempts)

  def withRetryableStatusCodes(codes: StatusCode*): BalancingNameResolverConfig =
    copy(retryableStatusCodes = codes.toSet)

  def withDelayMs(delayMs: Int): BalancingNameResolverConfig =
    copy(delayMs = delayMs)

  def withMaxBackOffMs(maxBackOffMs: Int): BalancingNameResolverConfig =
    copy(maxBackOffMs = maxBackOffMs)

  def withRetriesBackOffMultiplier(retriesBackOffMultiplier: Double): BalancingNameResolverConfig =
    copy(retriesBackOffMultiplier = retriesBackOffMultiplier)

  def withHealthCheck(): BalancingNameResolverConfig = withHealthCheck(serviceName)

  def withHealthCheck(healthCheckService: String): BalancingNameResolverConfig =
    copy(healthCheckService = Some(healthCheckService))

  def withSchema(schema: String) : BalancingNameResolverConfig =
    copy(schema = schema)

  def withPriority(priority: Int) : BalancingNameResolverConfig =
    copy(priority = priority)

  def toMap: Option[util.Map[String, Object]] = {
    val result = new util.HashMap[String, Object]()

    loadBalancingPolicy match {
      case LoadBalancingPolicies.pick_first =>
      case _ => result.put("loadBalancingPolicy", loadBalancingPolicy.toString)
    }

    if (serviceName.nonEmpty) {
      val serviceConfig = new util.HashMap[String, Object]()

      serviceConfig.put("name", List(
        // This would specify a service+method if you wanted
        // different methods to have different settings
        Map("service" -> serviceName).asJava).asJava)

      if (timeoutMs > 0) {
        serviceConfig.put("timeout", s"${timeoutMs.toDouble / 1000}s")
      }

      retryPolicy match {
        case RetryPolicies.retry =>
          serviceConfig.put("retryPolicy", Map(
            "maxAttempts" -> maxAttempts.toDouble,
            "initialBackoff" -> s"${delayMs.toDouble / 1000}s",
            "maxBackoff" -> s"${maxBackOffMs.toDouble / 1000}s",
            "backoffMultiplier" -> retriesBackOffMultiplier,
            "retryableStatusCodes" -> retryableStatusCodes.map(_.toString).toList.asJava
          ).asJava)
        case RetryPolicies.hedge =>
          serviceConfig.put("hedgingPolicy", Map(
            "maxAttempts" -> maxAttempts.toDouble,
            "hedgingDelay" -> s"${delayMs.toDouble / 1000}s",
            "nonFatalStatusCodes" -> retryableStatusCodes.map(_.toString).toList.asJava
          ).asJava)
        case _ =>
      }

      healthCheckService.foreach(x => result.put("healthCheckConfig", Map("serviceName" -> x).asJava))

      result.put("methodConfig", List(serviceConfig).asJava)
    }

    if (result.isEmpty) None else Some(result)
  }

  def toProvider: BalancingNameResolverProvider = {
    logger.info("Creating name resolver with config {}", this)
    new BalancingNameResolverProvider(replicas, toMap, schema, priority)
  }
}

/**
 * Available load balancing policies.
 * - always peek the first server for first request, replicas are used only for retries
 * - peek the servers in round-robin fashion
 */
object LoadBalancingPolicies extends Enumeration {
  type LoadBalancingPolicy = Value

  val pick_first, round_robin = Value
}

/**
 * Available retry policies
 * - none: do not issue retries
 * - retry: attempt to retry if the call fails
 * - hedge: attempt to retry if the call fails or does not completed in the specified time
 */
object RetryPolicies extends Enumeration {
  type RetryPolicy = Value

  val none, retry, hedge = Value
}

/**
 * gRPC status codes valid for retrying. See [[Status]] for details
 */
object StatusCodes extends Enumeration {
  type StatusCode = Value

  val UNAVAILABLE, INTERNAL, UNKNOWN, INVALID_ARGUMENT, NOT_FOUND, ALREADY_EXISTS,
  ABORTED, OUT_OF_RANGE = Value
}

/**
 * Name resolver used to balance requests between replicas.
 *
 * @param replicas Replicas to balance load between
 * @param config   Configuration of the load balancing.
 */
class BalancingNameResolverProvider private[grpc](
                                                   replicas: Set[String],
                                                   config: Option[java.util.Map[String, Object]],
                                                   schema: String,
                                                   priority: Int = 5
                                                 ) extends NameResolverProvider with StrictLogging {

  override def getDefaultScheme = schema

  override def newNameResolver(targetUri: URI, args: NameResolver.Args): NameResolver = {
    logger.debug(s"Creating resolver for $targetUri")

    val delegates: Set[DnsNameResolver] = replicas
      .map(x => {
        val newURI = new URI(
          "dns",
          targetUri.getHost,
          "/" + x,
          targetUri.getFragment)
        logger.debug(s"Created delegate resolver for URI $newURI")
        newURI
      })
      .map(new DnsNameResolverProvider().newNameResolver(_, args))

    new NameResolver {

      val parsedConfig: Option[NameResolver.ConfigOrError] = config.map(x => {
        val config = args.getServiceConfigParser.parseServiceConfig(x)
        logger.info("Parsed configuration for client is {}", config.getConfig)
        config
      })
      val knownAddresses = new mutable.HashSet[EquivalentAddressGroup]()
      var numResponses = 0


      override def getServiceAuthority: String = targetUri.getAuthority

      override def start(listener: NameResolver.Listener2): Unit = delegates.foreach(_.start(new Listener2 {
        override def onResult(resolutionResult: NameResolver.ResolutionResult): Unit = {
          val (addresses, numResponsesReceived) = knownAddresses.synchronized {
            resolutionResult.getAddresses.forEach(knownAddresses.add)
            numResponses += 1
            (knownAddresses.toArray, numResponses)
          }

          logger.debug(s"Got new result $resolutionResult")

          if (numResponsesReceived >= delegates.size) {
            logger.debug(s"Reporting the final result ${addresses.mkString(",")}")
            // Randomize replicas for smother balance
            val builder = resolutionResult.toBuilder.setAddresses(Random.shuffle(addresses.toSeq).asJava)

            config.foreach(x => {
              val healthCheck = x.get("healthCheckConfig")
              builder.setAttributes(
                resolutionResult.getAttributes.toBuilder.set(
                  LoadBalancer.ATTR_HEALTH_CHECKING_CONFIG, healthCheck.asInstanceOf[java.util.Map[String, _]])
                  .build())
            })

            parsedConfig.foreach(builder.setServiceConfig)

            listener.onResult(builder.build())
          }
        }

        override def onError(error: Status): Unit = {
          logger.error(s"Got error for DNS resolution $error")
          listener.onError(error)
        }
      }))

      override def refresh(): Unit = {
        logger.debug("Asked for refresh")
        knownAddresses.synchronized {
          knownAddresses.clear()
          numResponses = 0
        }
        delegates.foreach(_.refresh())
      }

      override def shutdown(): Unit = delegates.foreach(_.shutdown())
    }
  }

  override def isAvailable: Boolean = true

  override def priority(): Int = priority
}

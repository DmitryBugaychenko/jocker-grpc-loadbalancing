package org.jocker.grpc.loadbalancer

import com.google.common.util.concurrent.MoreExecutors
import io.grpc.internal.GrpcUtil
import io.grpc.netty.shaded.io.netty.util.concurrent.DefaultEventExecutor
import io.grpc.{ChannelLogger, NameResolver, Status, SynchronizationContext}
import org.scalatest.concurrent.TimeLimits
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.net.{InetSocketAddress, SocketAddress, URI}
import java.util
import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

class LoadBalancingTest extends AnyFlatSpec with SimpleServiceTests {

  "Name resolver" should "combine addresses" in {
    val resolver = new BalancingNameResolverProvider(testPorts.map(x => s"localhost:$x"), None, "https")
      .newNameResolver(
        new URI("https://jokerconf.com"),
        NameResolver.Args.newBuilder()
          .setDefaultPort(22)
          .setProxyDetector(GrpcUtil.DEFAULT_PROXY_DETECTOR)
          .setSynchronizationContext(new SynchronizationContext((t: Thread, e: Throwable) => ???))
          .setServiceConfigParser((rawServiceConfig: util.Map[String, _]) => ???)
          .setScheduledExecutorService(new DefaultEventExecutor())
          .setChannelLogger(new ChannelLogger {
            override def log(level: ChannelLogger.ChannelLogLevel, message: String): Unit = ???

            override def log(level: ChannelLogger.ChannelLogLevel, messageFormat: String, args: java.lang.Object*): Unit = ???
          })
          .setOffloadExecutor(MoreExecutors.directExecutor())
          .build())
    val results = new ArrayBuffer[NameResolver.ResolutionResult]()

    try {
      resolver.start(new NameResolver.Listener2 {
        override def onResult(resolutionResult: NameResolver.ResolutionResult): Unit = {
          results.synchronized {
            results += resolutionResult
          }
        }

        override def onError(error: Status): Unit = {
          System.out.println(error)
        }
      })

      for (i <- 0 to 100) {
        val size = results.synchronized {
          results.length
        }
        if (size < 2) {
          Thread.sleep(10)
        }
      }
    } finally {
      resolver.shutdown()
    }

    val addresses: Set[SocketAddress] = results.flatMap(_.getAddresses.asScala).flatMap(_.getAddresses.asScala).toSet

    addresses.map(_.asInstanceOf[InetSocketAddress].getAddress.getHostAddress) should contain atLeastOneElementOf Set("127.0.0.1")
    addresses.map(_.asInstanceOf[InetSocketAddress].getPort) should contain theSameElementsAs testPorts

  }

  "Name resolver" should "be compatible with load balancing" in {
    startAndTest(
      serviceFactory = normalService,
      resolverConfig = configWithLoadBalancing,
      expectPorts = testPorts
    )
  }

  "Retry config" should "retry on error" in {
    startAndTest(
      serviceFactory = port => if (port % 2 == 0) failingService else normalService(port),
      resolverConfig = configWithRetries,
      expectPorts = testPorts.filterNot(_ % 2 == 0))
  }

  "Retry config" should "retry on unavailable server" in {
    startAndTest(
      serviceFactory = port => if (port % 2 == 0) normalService(port) else unavailableService,
      resolverConfig = configWithRetries,
      expectPorts = testPorts.filterNot(_ % 2 != 0))
  }

  "Hedging config" should "retry on error" in {
    startAndTest(
      serviceFactory = port => if (port % 2 == 0) failingService else normalService(port),
      resolverConfig = configWithHedging,
      expectPorts = testPorts.filterNot(_ % 2 == 0))
  }

  "Hedging config" should "retry on unavailable server" in {
    startAndTest(
      serviceFactory = port => if (port % 2 == 0) normalService(port) else unavailableService,
      resolverConfig = configWithHedging,
      expectPorts = testPorts.filterNot(_ % 2 != 0))
  }

  "Hedging config" should "workaround timeout" in {
    startAndTest(
      serviceFactory = port => if (port % 2 == 0) hangingService else normalService(port),
      resolverConfig = configWithHedging,
      expectPorts = testPorts.filterNot(_ % 2 == 0))
  }
}
package org.jocker.grpc.loadbalancer

import io.grpc.health.v1.HealthGrpc
import io.grpc.{BindableService, ManagedChannel, NameResolverRegistry, Status}
import io.grpc.netty.shaded.io.grpc.netty.{NettyChannelBuilder, NettyServerBuilder}
import io.grpc.stub.StreamObserver
import io.grpc.testing.protobuf.SimpleServiceGrpc.{SimpleServiceBlockingStub, SimpleServiceImplBase}
import io.grpc.testing.protobuf.{SimpleRequest, SimpleResponse, SimpleServiceGrpc}
import org.scalatest.concurrent.TimeLimits
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.SpanSugar.convertIntToGrainOfTime

trait SimpleServiceTests extends Matchers with TimeLimits {

  val testPorts = Set(50051, 50052)

  val serviceName = "grpc.testing.SimpleService"

  val configWithLoadBalancing = new BalancingNameResolverConfig(testPorts.map(x => s"localhost:$x").toSeq: _*)
    .withLoadBalancingPolicy(LoadBalancingPolicies.round_robin)
    .withService[SimpleServiceGrpc]


  val configWithRetries = configWithLoadBalancing
      .withRetryPolicy(RetryPolicies.retry)
      .withMaxAttempts(4)
      .withDelayMs(100)
      .withRetryableStatusCodes(StatusCodes.UNAVAILABLE, StatusCodes.INTERNAL)

  val configWithHedging = configWithLoadBalancing
    .withRetryPolicy(RetryPolicies.hedge)
    .withMaxAttempts(4)
    .withTimeoutMs(1000)
    .withDelayMs(100)
    .withRetryableStatusCodes(StatusCodes.UNAVAILABLE, StatusCodes.INTERNAL)

  val configWithHealthChecks = configWithHedging
      .withHealthCheck()

  def normalService(port: Int): SimpleServiceImplBase = {
    new SimpleServiceImplBase {
      override def unaryRpc(request: SimpleRequest, responseObserver: StreamObserver[SimpleResponse]): Unit = {
        responseObserver.onNext(SimpleResponse.newBuilder().setResponseMessage(port.toString).build())
        responseObserver.onCompleted()
      }
    }
  }

  def failingService: SimpleServiceImplBase = {
    new SimpleServiceImplBase {
      override def unaryRpc(request: SimpleRequest, responseObserver: StreamObserver[SimpleResponse]): Unit = {
        responseObserver.onError(Status.INTERNAL.withDescription("Simulating failure").asException())
      }
    }
  }

  def hangingService: SimpleServiceImplBase = {
    new SimpleServiceImplBase {
      override def unaryRpc(request: SimpleRequest, responseObserver: StreamObserver[SimpleResponse]): Unit = {
        // Do nothing for timeout
      }
    }
  }

  def unavailableService: SimpleServiceImplBase = {
    null
  }

  def startAndTest(
                    serviceFactory: Int => SimpleServiceImplBase,
                    resolverConfig: BalancingNameResolverConfig,
                    expectPorts: Set[Int],
                    healthCheckFactory: Int => BindableService = _ => null): Unit = {

    val services = testPorts.flatMap(port => {
      val services = Seq(Option(serviceFactory(port)), Option(healthCheckFactory(port))).flatten
      if (services.nonEmpty) {
        val builder = NettyServerBuilder
          .forPort(port)

        services.foreach(builder.addService)

        Some(builder.build().start())
      } else {
        None
      }
    })

    try {

      // This is the non-deprecated method for injection of provider, however it is globally scoped
      // and thus less convenient.
      //NameResolverRegistry.getDefaultRegistry.register(resolverConfig.toProvider)

      val channel: ManagedChannel = NettyChannelBuilder
        .forTarget("lbs://localhost")
        .enableRetry()
        .usePlaintext()
        .nameResolverFactory(resolverConfig.toProvider)
        .build()

      try {
        val client: SimpleServiceBlockingStub = SimpleServiceGrpc.newBlockingStub(channel)

        failAfter(2 seconds) {
          Array
            .tabulate(5) { _ => client.unaryRpc(SimpleRequest.newBuilder().build()).getResponseMessage.toInt }
            .toSet should contain theSameElementsAs expectPorts
        }
      } finally {
        channel.shutdown()
      }

    } finally {
      services.foreach(_.shutdownNow())
    }
  }

}

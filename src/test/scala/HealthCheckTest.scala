package org.jocker.grpc.loadbalancer

import io.grpc.health.v1.{HealthCheckRequest, HealthCheckResponse, HealthGrpc}
import io.grpc.stub.StreamObserver
import org.scalatest.flatspec.AnyFlatSpec

class HealthCheckTest extends AnyFlatSpec with SimpleServiceTests {

  "Health check" should "not sent requests to unhealthy server" in {
    startAndTest(
      serviceFactory = normalService,
      resolverConfig = configWithHealthChecks,
      expectPorts = testPorts.filterNot(_ % 2 == 0),
      healthCheckFactory = port => if (port % 2 == 0) {
        wrapHealthCheck(() => HealthCheckResponse.ServingStatus.NOT_SERVING)
      } else {
        wrapHealthCheck(() => HealthCheckResponse.ServingStatus.SERVING)
      })
  }

  "Health check" should "not sent requests to hanging server" in {
    startAndTest(
      serviceFactory = normalService,
      resolverConfig = configWithHealthChecks,
      expectPorts = testPorts.filterNot(_ % 2 == 0),
      healthCheckFactory = port => if (port % 2 == 0) {
        wrapHealthCheck(() => {
          Thread.sleep(10000)
          HealthCheckResponse.ServingStatus.SERVING
        })
      } else {
        wrapHealthCheck(() => HealthCheckResponse.ServingStatus.SERVING)
      })
  }

  private def wrapHealthCheck(checker: () => HealthCheckResponse.ServingStatus): HealthGrpc.HealthImplBase = {
    new HealthGrpc.HealthImplBase {
      override def check(request: HealthCheckRequest, responseObserver: StreamObserver[HealthCheckResponse]): Unit = {
        responseObserver.onNext(
          HealthCheckResponse.newBuilder().setStatus(checker()).build())
        responseObserver.onCompleted()
      }

      override def watch(request: HealthCheckRequest, responseObserver: StreamObserver[HealthCheckResponse]): Unit = {
        responseObserver.onNext(
          HealthCheckResponse.newBuilder().setStatus(checker()).build())
      }
    }
  }
}

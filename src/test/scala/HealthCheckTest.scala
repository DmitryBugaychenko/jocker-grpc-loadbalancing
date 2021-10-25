package org.jocker.grpc.loadbalancer

import io.grpc.health.v1.HealthCheckResponse.ServingStatus
import io.grpc.health.v1.{HealthCheckRequest, HealthCheckResponse, HealthGrpc}
import io.grpc.protobuf.services.HealthStatusManager
import io.grpc.stub.StreamObserver
import org.scalatest.flatspec.AnyFlatSpec

class HealthCheckTest extends AnyFlatSpec with SimpleServiceTests {

  "Health check" should "not sent requests to unhealthy server" in {
    startAndTest(
      serviceFactory = normalService,
      resolverConfig = configWithHealthChecks,
      expectPorts = testPorts.filterNot(_ % 2 == 0),
      healthCheckFactory = port => if (port % 2 == 0) {
        serviceWithConstantHealth(ServingStatus.NOT_SERVING)
      } else {
        serviceWithConstantHealth(ServingStatus.SERVING)
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
        serviceWithConstantHealth(ServingStatus.SERVING)
      })
  }

  private def serviceWithConstantHealth(status: ServingStatus) = {
    val manager = new HealthStatusManager()
    manager.setStatus(serviceName, status)
    manager.getHealthService
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

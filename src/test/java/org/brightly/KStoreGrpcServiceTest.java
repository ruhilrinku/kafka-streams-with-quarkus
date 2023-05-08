package org.brightly;

import io.quarkus.grpc.GrpcClient;
import io.quarkus.test.junit.QuarkusTest;

@QuarkusTest
public class KStoreGrpcServiceTest {

    @GrpcClient
    KStoreGrpcService kStoreGrpcService;

}

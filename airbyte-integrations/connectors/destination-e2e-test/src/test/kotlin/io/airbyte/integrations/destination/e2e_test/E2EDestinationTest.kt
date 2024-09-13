/*
 * Copyright (c) 2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.destination.e2e_test

import io.airbyte.cdk.output.OutputConsumer
import io.airbyte.protocol.models.v0.AirbyteMessage
import io.micronaut.context.annotation.Requires
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import jakarta.inject.Singleton
import java.time.Instant

@MicronautTest(startApplication = false)
class E2EDestinationTest : DestinationTest {

    @Singleton
    @Requires(env = ["test"])
    class MockOutputConsumer : OutputConsumer {
        override val emittedAt: Instant
            get() = Instant.now()

        override fun accept(t: AirbyteMessage) {
            println("accept: $t")
        }

        override fun close() {}
    }
}

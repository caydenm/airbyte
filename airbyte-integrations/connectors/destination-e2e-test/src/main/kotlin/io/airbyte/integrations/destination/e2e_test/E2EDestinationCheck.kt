/*
 * Copyright (c) 2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.destination.e2e_test

import io.airbyte.cdk.check.DestinationCheck
import jakarta.inject.Singleton

@Singleton
class E2EDestinationCheck : DestinationCheck {
    override fun check() {
        // Do nothing: this is a test destination
    }
}

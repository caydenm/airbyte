package io.airbyte.cdk.test.spec

import com.deblock.jsondiff.DiffGenerator
import com.deblock.jsondiff.diff.JsonDiff
import com.deblock.jsondiff.matcher.CompositeJsonMatcher
import com.deblock.jsondiff.matcher.JsonMatcher
import com.deblock.jsondiff.matcher.LenientJsonObjectPartialMatcher
import com.deblock.jsondiff.matcher.StrictJsonArrayPartialMatcher
import com.deblock.jsondiff.matcher.StrictPrimitivePartialMatcher
import com.deblock.jsondiff.viewer.OnlyErrorDiffViewer
import com.fasterxml.jackson.databind.ObjectMapper
import io.airbyte.cdk.test.util.DestinationProcessFactory
import io.airbyte.cdk.test.util.FakeDataDumper
import io.airbyte.cdk.test.util.IntegrationTest
import io.airbyte.cdk.test.util.NoopExpectedRecordMapper
import io.airbyte.protocol.models.Jsons
import io.airbyte.protocol.models.v0.AirbyteMessage
import java.nio.file.Files
import java.nio.file.Path
import kotlin.test.assertEquals
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test

private const val EXPECTED_SPEC_FILENAME = "expected_spec.json"

/**
 * This is largely copied from [io.airbyte.cdk.spec.SpecTest], but adapted to
 * use our [DestinationProcessFactory].
 *
 * It also automatically writes the actual spec back to `expected_spec.json`
 * for easier inspection of the diff. This diff is _really messy_ for the
 * initial migration from the old CDK to the new one, but after that, it should
 * be pretty readable.
 */
open abstract class SpecTest: IntegrationTest(
    FakeDataDumper,
    NoopExpectedRecordMapper(),
) {
    @Test
    fun testSpec() {
        val expectedSpec = Files.readString(Path.of(EXPECTED_SPEC_FILENAME))
        val process = destinationProcessFactory.createDestinationProcess("spec")
        process.run()
        val messages = process.readMessages()
        val specMessages = messages.filter { it.type == AirbyteMessage.Type.SPEC }

        assertEquals(
            specMessages.size,
            1,
            "Expected to receive exactly one connection status message, but got ${specMessages.size}: $specMessages"
        )

        val spec = specMessages.first().spec
        Files.write(
            Path.of(EXPECTED_SPEC_FILENAME),
            ObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(spec).toByteArray()
        )

        val jsonMatcher: JsonMatcher =
            CompositeJsonMatcher(
                StrictJsonArrayPartialMatcher(),
                LenientJsonObjectPartialMatcher(),
                StrictPrimitivePartialMatcher(),
            )
        val diff: JsonDiff = DiffGenerator.diff(expectedSpec, Jsons.serialize(spec), jsonMatcher)
        Assertions.assertEquals(
            "",
            OnlyErrorDiffViewer.from(diff).toString(),
            "Spec snapshot test failed. Run this test locally and then `git diff <...>/expected_spec.json` to see what changed, and commit the diff if that change was intentional."
        )
    }
}

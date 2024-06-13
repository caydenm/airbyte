/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.destination.s3;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.UploadPartRequest;
import com.amazonaws.services.s3.model.UploadPartResult;
import com.fasterxml.jackson.databind.JsonNode;
import io.airbyte.cdk.integrations.destination.s3.S3BaseChecks;
import io.airbyte.cdk.integrations.destination.s3.S3DestinationConfig;
import io.airbyte.cdk.integrations.destination.s3.S3DestinationConfigFactory;
import io.airbyte.cdk.integrations.destination.s3.S3StorageOperations;
import io.airbyte.cdk.integrations.destination.s3.StorageProvider;
import io.airbyte.cdk.integrations.destination.s3.constant.S3Constants;
import io.airbyte.commons.features.EnvVariableFeatureFlags;
import io.airbyte.commons.features.FeatureFlagsWrapper;
import io.airbyte.commons.json.Jsons;
import io.airbyte.protocol.models.v0.AirbyteConnectionStatus;
import io.airbyte.protocol.models.v0.AirbyteConnectionStatus.Status;
import io.airbyte.protocol.models.v0.ConnectorSpecification;
import java.util.Collections;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.Mockito;

public class S3DestinationTest {

  private AmazonS3 s3;
  private S3DestinationConfig config;
  private S3DestinationConfigFactory factoryConfig;

  @BeforeEach
  public void setup() {
    s3 = mock(AmazonS3.class);
    final InitiateMultipartUploadResult uploadResult = mock(InitiateMultipartUploadResult.class);
    final UploadPartResult uploadPartResult = mock(UploadPartResult.class);
    when(s3.uploadPart(any(UploadPartRequest.class))).thenReturn(uploadPartResult);
    when(s3.initiateMultipartUpload(any(InitiateMultipartUploadRequest.class))).thenReturn(uploadResult);

    config = S3DestinationConfig.create("fake-bucket", "fake-bucketPath", "fake-region")
        .withEndpoint("fake-endpoint")
        .withAccessKeyCredential("fake-accessKeyId", "fake-secretAccessKey")
        .withS3Client(s3)
        .get();

    factoryConfig = new S3DestinationConfigFactory() {

      public S3DestinationConfig getS3DestinationConfig(final JsonNode config, final StorageProvider storageProvider, Map<String, String> env) {
        return S3DestinationConfig.create("fake-bucket", "fake-bucketPath", "fake-region")
            .withEndpoint("https://s3.example.com")
            .withAccessKeyCredential("fake-accessKeyId", "fake-secretAccessKey")
            .withS3Client(s3)
            .get();
      }

    };
  }

  @Test
  /**
   * Test that check will fail if IAM user does not have listObjects permission
   */
  public void checksS3WithoutListObjectPermission() {
    final S3Destination destinationFail = new S3Destination(factoryConfig, Collections.emptyMap());
    doThrow(new AmazonS3Exception("Access Denied")).when(s3).listObjects(any(ListObjectsRequest.class));
    final AirbyteConnectionStatus status = destinationFail.check(Jsons.emptyObject());
    assertEquals(Status.FAILED, status.getStatus(), "Connection check should have failed");
    assertTrue(status.getMessage().indexOf("Access Denied") > 0, "Connection check returned wrong failure message");
  }

  @Test
  /**
   * Test that check will succeed when IAM user has all required permissions
   */
  public void checksS3WithListObjectPermission() {
    final S3Destination destinationSuccess = new S3Destination(factoryConfig, Collections.emptyMap());
    final AirbyteConnectionStatus status = destinationSuccess.check(Jsons.emptyObject());
    assertEquals(Status.SUCCEEDED, status.getStatus(), "Connection check should have succeeded");
  }

  @Test
  /**
   * Test that check will succeed when IAM user has all required permissions
   */
  public void testCheckOnCloud() throws Exception {
    final S3Destination s3Destination = new S3Destination(factoryConfig, Collections.emptyMap());
    s3Destination.setFeatureFlags(FeatureFlagsWrapper.overridingDeploymentMode(new EnvVariableFeatureFlags(), "CLOUD"));
    final ConnectorSpecification spec = s3Destination.spec();
    assertTrue(spec.getConnectionSpecification().get("properties").has(S3Constants.ROLE_ARN));
  }

  @Test
  public void testCheckOnOss() throws Exception {
    final S3Destination s3Destination = new S3Destination(factoryConfig, Collections.emptyMap());
    s3Destination.setFeatureFlags(FeatureFlagsWrapper.overridingDeploymentMode(new EnvVariableFeatureFlags(), "OSS"));
    final ConnectorSpecification spec = s3Destination.spec();
    assertFalse(spec.getConnectionSpecification().get("properties").has(S3Constants.ROLE_ARN));
  }

  @Test
  public void createsThenDeletesTestFile() {
    S3BaseChecks.attemptS3WriteAndDelete(mock(S3StorageOperations.class), config, "fake-fileToWriteAndDelete");

    // We want to enforce that putObject happens before deleteObject, so use inOrder.verify()
    final InOrder inOrder = Mockito.inOrder(s3);

    final ArgumentCaptor<String> testFileCaptor = ArgumentCaptor.forClass(String.class);
    inOrder.verify(s3).putObject(eq("fake-bucket"), testFileCaptor.capture(), anyString());

    final String testFile = testFileCaptor.getValue();
    assertTrue(testFile.startsWith("fake-fileToWriteAndDelete/_airbyte_connection_test_"), "testFile was actually " + testFile);

    inOrder.verify(s3).listObjects(any(ListObjectsRequest.class));
    inOrder.verify(s3).deleteObject("fake-bucket", testFile);

    verifyNoMoreInteractions(s3);
  }

}

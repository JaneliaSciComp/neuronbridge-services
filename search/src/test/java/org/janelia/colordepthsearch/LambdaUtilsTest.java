package org.janelia.colordepthsearch;

import java.net.URI;

import com.fasterxml.jackson.annotation.JsonProperty;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentMatcher;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class LambdaUtilsTest {

    private S3Client s3Client;

    @Before
    public void setUp() {
        s3Client = mock(S3Client.class);
    }

    @Test
    public void putObjectWithURI() {
        URI testURI = URI.create("s3://testBucket/private/us-east-1:853b7e81-c739-4434-99dd-aafeed3265e3/rc-upload-1594225719194-2/rc-upload-1594225719194-2.result");
        class Dummy {
            @JsonProperty
            private final String t;

            Dummy(String t) {
                this.t = t;
            }
        }
        LambdaUtils.putObject(
                s3Client,
                testURI,
                new Dummy("test"));
        verify(s3Client).putObject(
                argThat(new ArgumentMatcher<PutObjectRequest>() {
                    @Override
                    public boolean matches(PutObjectRequest argument) {
                        return "testBucket".equals(argument.bucket()) &&
                                "/private/us-east-1:853b7e81-c739-4434-99dd-aafeed3265e3/rc-upload-1594225719194-2/rc-upload-1594225719194-2.result".equals(argument.key()) &&
                                "application/json".equals(argument.contentType());
                    }
                }),
                any(RequestBody.class));
    }
}

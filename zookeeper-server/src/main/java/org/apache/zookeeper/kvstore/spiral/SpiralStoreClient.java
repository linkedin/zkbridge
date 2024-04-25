package org.apache.zookeeper.kvstore.spiral;

import java.io.File;
import java.nio.file.Path;
import java.util.Objects;

import org.apache.zookeeper.kvstore.KVStoreClient;
import org.apache.zookeeper.kvstore.spiral.SpiralStoreClient.SpiralStoreClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;

import io.grpc.ManagedChannel;
import io.grpc.netty.shaded.io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.shaded.io.grpc.netty.NegotiationType;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContext;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContextBuilder;
import proto.com.linkedin.spiral.GetRequest;
import proto.com.linkedin.spiral.GetResponse;
import proto.com.linkedin.spiral.DeleteRequest;
import proto.com.linkedin.spiral.DeleteResponse;
import proto.com.linkedin.spiral.PutRequest;
import proto.com.linkedin.spiral.PutResponse;
import proto.com.linkedin.spiral.Key;
import proto.com.linkedin.spiral.Put;
import proto.com.linkedin.spiral.SpiralApiGrpc;
import proto.com.linkedin.spiral.SpiralContext;
import proto.com.linkedin.spiral.Value;

/**
 * A client for the spiral key-value store.
 */
public class SpiralStoreClient implements KVStoreClient<String, byte[], SpiralContext> {
    private static final Logger LOG = LoggerFactory.getLogger(SpiralStoreClient.class);
    
    private final SpiralApiGrpc.SpiralApiBlockingStub _blockingStub;
    
    private SpiralStoreClient(String spiralEndpoint, boolean useSsl, String caBundle, String identityCert, String identityKey, String overrideAuthority) {
        try {

            NettyChannelBuilder channelBuilder = null;
            if (!useSsl) {
                channelBuilder = NettyChannelBuilder
                .forTarget(spiralEndpoint)
                .usePlaintext();
            } else {
                // Create an SSL context using the provided CA bundle, identity cert, and identity key.
                SslContext sslContext = GrpcSslContexts.forClient()
                                            .trustManager(new File(caBundle))
                                            .keyManager(new File(identityCert), new File(identityKey))
                                            .build();
                
                // Create a channel using the SSL context.
                channelBuilder = NettyChannelBuilder
                    .forTarget(spiralEndpoint)
                    .overrideAuthority(overrideAuthority)
                    .sslContext(sslContext);

                channelBuilder.negotiationType(NegotiationType.TLS);
                ManagedChannel channel = channelBuilder.build();
            }

            ManagedChannel channel = channelBuilder.build();
            _blockingStub = SpiralApiGrpc.newBlockingStub(channel);      
            LOG.info("Connected to spiral-service : {}", spiralEndpoint);
        } catch (Exception e) {
        LOG.error("Failed to connect to spiral service at endpoint : {}", spiralEndpoint, e);
        throw new RuntimeException(String.format("Failed to connect to spiral service at endpoint : %s", spiralEndpoint), e);
        }
    }

    public static class SpiralStoreClientBuilder {
        private static final Logger LOG = LoggerFactory.getLogger(SpiralStoreClientBuilder.class);
        private String _spiralEndPoint;
        private boolean _useSsl = false;
        private String _caBundlePath = null;
        private String _identityCertPath = null; 
        private String _identityKeyPath = null;
        private String _overrideAuthority = null;

        public SpiralStoreClientBuilder withSpiralEndPoint(String spiralEndPoint) {
        _spiralEndPoint = spiralEndPoint;
        return this;
        }

        public SpiralStoreClientBuilder withSsl(boolean useSsl) {
        _useSsl = useSsl;
        return this;
        }

        public SpiralStoreClientBuilder withCaBundlePath(String caBundlePath) {
        _caBundlePath = caBundlePath;
        return this;
        }

        public SpiralStoreClientBuilder withIdentityCertPath(String identityCertPath) {
        _identityCertPath = identityCertPath;
        return this;
        }

        public SpiralStoreClientBuilder withIdentityKeyPath(String identityKeyPath) {
        _identityKeyPath = identityKeyPath;
        return this;
        }

        public SpiralStoreClientBuilder withOverrideAuthority(String overrideAuthority) {
        _overrideAuthority = overrideAuthority;
        return this;
        }

        public SpiralStoreClient build() {
            Objects.requireNonNull(_spiralEndPoint, "Spiral endpoint is required");
            if (_useSsl) {
                Objects.requireNonNull(_caBundlePath, "CA bundle path is required");
                Objects.requireNonNull(_identityCertPath, "Identity cert path is required");
                Objects.requireNonNull(_identityKeyPath, "Identity key path is required");
                Objects.requireNonNull(_overrideAuthority, "Override authority is required");
            }
            return new SpiralStoreClient(_spiralEndPoint, _useSsl, _caBundlePath, _identityCertPath, _identityKeyPath, _overrideAuthority);
        }
    }

    @Override
    public byte[] get(SpiralContext context, String key) {
        try {
        ByteString keyBytes = ByteString.copyFromUtf8(key);
        Key apiKey = Key.newBuilder()
                        .setMessage(keyBytes)
                        .build();
        GetRequest request = GetRequest.newBuilder()
                                    .setSpiralContext(context)
                                    .setKey(apiKey)
                                    .build();
        GetResponse response = _blockingStub.get(request);
        return response.getValue().getMessage().toByteArray();
        } catch (Exception e) {
        LOG.error("Get: RPC failed for spiral context :{}, key :{}", context, key, e);
        throw e;
        }
    }

    @Override
    public void put(SpiralContext context, String key, byte[] value) {
        try {
            ByteString keyBytes = ByteString.copyFromUtf8(key);
            ByteString valueBytes = ByteString.copyFrom(value);
            Key apiKey = Key.newBuilder()
                            .setMessage(keyBytes)
                            .build();
            Value apiValue = Value.newBuilder()
                                .setMessage(valueBytes)
                                .build();
            Put putValue = Put.newBuilder()
                            .setKey(apiKey)
                            .setValue(apiValue)
                            .build();
            
            PutRequest request = PutRequest.newBuilder()
                                    .setSpiralContext(context)
                                    .setPut(putValue)
                                    .build();
            
            PutResponse response = _blockingStub.put(request);
        } catch (Exception e) {
            LOG.error("Put: RPC failed for spiral context :{}, key :{}, value :{}", context, key, value, e);
            throw e;
        }
    }

    @Override
    public void delete(SpiralContext context, String key) {
        try {
            ByteString keyBytes = ByteString.copyFromUtf8(key);
            Key apiKey = Key.newBuilder()
                            .setMessage(keyBytes)
                            .build();
            DeleteRequest request = DeleteRequest.newBuilder()
                                        .setSpiralContext(context)
                                        .setKey(apiKey)
                                        .build();
            DeleteResponse response = _blockingStub.delete(request);
        } catch (Exception e) {
            LOG.error("Delete: RPC failed for spiral context :{}, key :{}", context, key, e);
            throw e;
        }
    }
}

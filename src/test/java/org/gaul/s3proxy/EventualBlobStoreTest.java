/*
 * Copyright (c) Bounce Storage, Inc. All rights reserved.
 * For more information, please see COPYRIGHT in the top-level directory.
 */

package com.bouncestorage.bounce;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableList;
import com.google.common.hash.Hashing;
import com.google.common.io.ByteSource;
import com.google.common.net.MediaType;
import com.google.inject.Module;

import org.jclouds.ContextBuilder;
import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.BlobStoreContext;
import org.jclouds.blobstore.domain.Blob;
import org.jclouds.logging.slf4j.config.SLF4JLoggingModule;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public final class EventualBlobStoreTest {
    private static final int DELAY = 5;
    private static final TimeUnit DELAY_UNIT = TimeUnit.SECONDS;
    private BlobStoreContext nearContext;
    private BlobStoreContext farContext;
    private BlobStore nearBlobStore;
    private BlobStore farBlobStore;
    private String containerName;
    private ScheduledExecutorService executorService;
    private BlobStore eventualBlobStore;

    @Before
    public void setUp() throws Exception {
        containerName = createRandomContainerName();

        nearContext = ContextBuilder
                .newBuilder("transient")
                .credentials("identity", "credential")
                .modules(ImmutableList.<Module>of(new SLF4JLoggingModule()))
                .build(BlobStoreContext.class);
        nearBlobStore = nearContext.getBlobStore();
        nearBlobStore.createContainerInLocation(null, containerName);

        farContext = ContextBuilder
                .newBuilder("transient")
                .credentials("identity", "credential")
                .modules(ImmutableList.<Module>of(new SLF4JLoggingModule()))
                .build(BlobStoreContext.class);
        farBlobStore = farContext.getBlobStore();
        farBlobStore.createContainerInLocation(null, containerName);

        executorService = Executors.newScheduledThreadPool(1);

        eventualBlobStore = EventualBlobStore.newEventualBlobStore(
                nearBlobStore, farBlobStore, executorService, DELAY, DELAY_UNIT);
    }

    @After
    public void tearDown() throws Exception {
        if (nearContext != null) {
            nearBlobStore.deleteContainer(containerName);
            nearContext.close();
        }
        if (farContext != null) {
            farBlobStore.deleteContainer(containerName);
            farContext.close();
        }
        if (executorService != null) {
            executorService.shutdown();
        }
    }

    @Test
    public void testReadAfterCreate() throws Exception {
        String blobName = createRandomBlobName();
        Blob blob = makeBlob(eventualBlobStore, blobName);
        eventualBlobStore.putBlob(containerName, blob);
        assertThat(eventualBlobStore.getBlob(containerName, blobName))
                .isNull();
        delay();
        assertThat(eventualBlobStore.getBlob(containerName, blobName))
                .isNotNull();
    }

    @Test
    public void testReadAfterDelete() throws Exception {
        String blobName = createRandomBlobName();
        Blob blob = makeBlob(eventualBlobStore, blobName);
        eventualBlobStore.putBlob(containerName, blob);
        assertThat(eventualBlobStore.getBlob(containerName, blobName))
                .isNull();
        delay();
        eventualBlobStore.removeBlob(containerName, blobName);
        assertThat(eventualBlobStore.getBlob(containerName, blobName))
                .isNotNull();
        delay();
        assertThat(eventualBlobStore.getBlob(containerName, blobName))
                .isNull();
    }

    @Test
    public void testOverwriteAfterDelete() throws Exception {
        String blobName = createRandomBlobName();
        Blob blob = makeBlob(eventualBlobStore, blobName);
        eventualBlobStore.putBlob(containerName, blob);
        delay();
        eventualBlobStore.removeBlob(containerName, blobName);
        blob = makeBlob(eventualBlobStore, blobName);
        eventualBlobStore.putBlob(containerName, blob);
        delay();
        assertThat(eventualBlobStore.getBlob(containerName, blobName))
                .isNotNull();
    }

    private static String createRandomContainerName() {
        return "container-" + new Random().nextInt(Integer.MAX_VALUE);
    }

    private static String createRandomBlobName() {
        return "blob-" + new Random().nextInt(Integer.MAX_VALUE);
    }

    private static Blob makeBlob(BlobStore blobStore, String blobName)
            throws IOException {
        ByteSource byteSource = ByteSource.empty();
        return blobStore.blobBuilder(blobName)
                .payload(byteSource)
                .contentLength(byteSource.size())
                .contentType(MediaType.OCTET_STREAM)
                .contentMD5(byteSource.hash(Hashing.md5()))
                .build();
    }

    private static void delay() throws InterruptedException {
        DELAY_UNIT.sleep(1 + DELAY);
    }
}

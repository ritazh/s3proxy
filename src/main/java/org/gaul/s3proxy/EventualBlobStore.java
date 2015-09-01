/*
 * Copyright (c) Bounce Storage, Inc. All rights reserved.
 * For more information, please see COPYRIGHT in the top-level directory.
 */

package com.bouncestorage.bounce;

import static java.util.Objects.requireNonNull;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.Deque;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.domain.Blob;
import org.jclouds.blobstore.options.PutOptions;
import org.jclouds.blobstore.util.ForwardingBlobStore;

public final class EventualBlobStore extends ForwardingBlobStore {
    private final BlobStore nearStore;
    private final ScheduledExecutorService executorService;
    private final Deque<Callable<?>> deque = new ConcurrentLinkedDeque<>();
    private final int delay;
    private final TimeUnit delayUnit;
    private final Random random = new Random();

    private EventualBlobStore(BlobStore nearStore, BlobStore farStore,
            ScheduledExecutorService executorService, int delay,
            TimeUnit delayUnit) {
        super(farStore);
        this.nearStore = requireNonNull(nearStore);
        this.executorService = requireNonNull(executorService);
        checkArgument(delay > 0, "Delay must be greater than zero, was: %s",
                delay);
        this.delay = delay;
        this.delayUnit = requireNonNull(delayUnit);
    }

    public static BlobStore newEventualBlobStore(BlobStore nearStore,
            BlobStore farStore, ScheduledExecutorService executorService,
            int delay, TimeUnit delayUnit) {
        return new EventualBlobStore(nearStore, farStore, executorService,
                delay, delayUnit);
    }

    // TODO: copyBlob
    // TODO: completeMultipartUpload

    @Override
    public String putBlob(String containerName, Blob blob) {
        return putBlob(containerName, blob, PutOptions.NONE);
    }

    @Override
    public String putBlob(final String containerName, Blob blob,
            final PutOptions putOptions) {
        final String nearName = blob.getMetadata().getName();
        String nearETag = nearStore.putBlob(containerName, blob, putOptions);
        deque.add(new Callable<String>() {
                @Override
                public String call() {
                    Blob nearBlob = nearStore.getBlob(containerName, nearName);
                    String farETag = delegate().putBlob(containerName,
                            nearBlob, putOptions);
                    nearStore.removeBlob(containerName, nearName);
                    return farETag;
                }
            });
        executorService.schedule(new DequeCallable(),
                1 + random.nextInt(delay), delayUnit);
        return nearETag;
    }

    @Override
    public void removeBlob(final String containerName, final String blobName) {
        deque.add(new Callable<Void>() {
                @Override
                public Void call() {
                    delegate().removeBlob(containerName, blobName);
                    return null;
                }
            });
        executorService.schedule(new DequeCallable(),
                1 + random.nextInt(delay), delayUnit);
    }

    private final class DequeCallable implements Callable<Void> {
        @Override
        public Void call() throws Exception {
            Callable<?> callable = deque.poll();
            if (callable == null) {
                // TODO: warn?
                return null;
            }
            callable.call();
            return null;
        }
    }
}

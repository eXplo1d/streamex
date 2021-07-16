package org.yarr.streamex.batch;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class ChunkedStream<T> implements Spliterator<List<T>> {

    private final Spliterator<T> parent;
    private final int maxChunkSize;
    private final Queue<List<T>> chunkBuffer = new LinkedList<>();
    private final Consumer<T> itemConsumer = this::addItem;

    private List<T> currentChunk;

    private ChunkedStream(
            Spliterator<T> parentStream,
            int maxChunkSize
    ) {
        this.parent = parentStream;
        this.maxChunkSize = maxChunkSize;
    }

    @Override
    public boolean tryAdvance(Consumer<? super List<T>> action) {
        while (parent.tryAdvance(itemConsumer)) {
            if (!chunkBuffer.isEmpty()) {
                action.accept(chunkBuffer.poll());
                return true;
            }
        }
        if (!chunkBuffer.isEmpty()) {
            action.accept(chunkBuffer.poll());
            return true;
        }
        if (currentChunk != null && !currentChunk.isEmpty()) {
            action.accept(currentChunk);
            currentChunk = null;
            return true;
        }
        return false;
    }

    @Override
    public Spliterator<List<T>> trySplit() {
        Spliterator<T> split = parent.trySplit();
        if (split == null) {
            return null;
        }
        return new ChunkedStream<>(
                split,
                maxChunkSize
        );
    }

    @Override
    public long estimateSize() {
        return chunkBuffer.size();
    }

    @Override
    public int characteristics() {
        return parent.characteristics();
    }

    private void addItem(T item) {
        if (currentChunk == null) {
            currentChunk = new ArrayList<>(maxChunkSize);
        }

        if (currentChunk.size() + 1 > maxChunkSize) {
            chunkBuffer.add(currentChunk);
            currentChunk = new ArrayList<>(maxChunkSize);
        }
        currentChunk.add(item);
    }


    public static <T> Stream<List<T>> chunked(Stream<T> parentStream, int maxChunkSize) {
        return StreamSupport.stream(
                new ChunkedStream<>(parentStream.spliterator(), maxChunkSize), parentStream.isParallel()
        );
    }
}

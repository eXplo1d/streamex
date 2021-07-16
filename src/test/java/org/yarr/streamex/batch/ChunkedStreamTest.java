package org.yarr.streamex.batch;


import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;


class ChunkedStreamTest {

    @Test
    public void GivenEmptyStreamWhenChunkedShouldReturnEmptyStream() {
        int expectedResult = 0;
        int actualResult = (int) ChunkedStream
                .chunked(Stream.empty(), 100)
                .count();
        Assertions.assertEquals(expectedResult, actualResult);
    }

    @Test
    public void GivenSingleElementStreamWhenChunkedBy100ShouldReturnOneSingleElementChunk() {
        List<List<Integer>> expectedResult = Collections.singletonList(Collections.singletonList(42));
        List<List<?>> actualResult = ChunkedStream
                .chunked(Stream.of(42), 100)
                .collect(Collectors.toList());
        Assertions.assertEquals(expectedResult, actualResult);
    }

    @Test
    public void GivenOddElementsStreamWhenChunkedBy2ShouldReturnListOfCompletedChunksAndUncompletedChunk() {
        List<List<Integer>> expectedResult = Arrays.asList(
                Arrays.asList(1, 2),
                Arrays.asList(3, 4),
                Arrays.asList(5, 6),
                Arrays.asList(7, 8),
                Collections.singletonList(9)
        );
        List<Integer> oddList = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9);
        List<List<Integer>> actualResult = ChunkedStream
                .chunked(oddList.stream(), 2)
                .collect(Collectors.toList());
        Assertions.assertEquals(expectedResult, actualResult);
    }

    @Test
    public void GivenEvenElementsStreamWhenChunkedBy2ShouldReturnListOfCompletedChunks() {
        List<List<Integer>> expectedResult = Arrays.asList(
                Arrays.asList(1, 2),
                Arrays.asList(3, 4),
                Arrays.asList(5, 6),
                Arrays.asList(7, 8)
        );
        List<Integer> oddList = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8);
        List<List<Integer>> actualResult = ChunkedStream
                .chunked(oddList.stream(), 2)
                .collect(Collectors.toList());
        Assertions.assertEquals(expectedResult, actualResult);
    }

    @Test
    public void GivenEvenElementsStreamWhenChunkedBy3ShouldReturnListOfCompletedChunksAndUncompleted() {
        List<List<Integer>> expectedResult = Arrays.asList(
                Arrays.asList(1, 2, 3),
                Arrays.asList(4, 5, 6),
                Arrays.asList(7, 8)
        );
        List<Integer> oddList = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8);
        List<List<Integer>> actualResult = ChunkedStream
                .chunked(oddList.stream(), 3)
                .collect(Collectors.toList());
        Assertions.assertEquals(expectedResult, actualResult);
    }

    @Test
    public void GivenInfiniteStreamWhenLimit100ChunkedBy3ShouldReturnListOfChunks() {
        long expectedResult = 100;

        long actualResult = ChunkedStream
                .chunked(
                        IntStream
                                .generate(() -> 42)
                                .boxed(),
                        3
                ).limit(expectedResult)
                .count();
        Assertions.assertEquals(expectedResult, actualResult);
    }

    @Test
    public void GivenParallelInfiniteStreamWhenLimit10000ChunkedBy3ShouldReturnListOfChunks() {
        long expectedResult = 10000;

        long actualResult = ChunkedStream
                .chunked(
                        IntStream
                                .generate(() -> 42)
                                .boxed()
                                .parallel(),
                        3
                )
                .limit(expectedResult)
                .count();
        Assertions.assertEquals(expectedResult, actualResult);
    }

    @Test
    public void GivenParallelRangeStreamWhenLimit10000ChunkedBy3ShouldReturnListOfChunks() {
        Random r = new Random(42);
        List<Integer> expectedResult = IntStream
                .generate(r::nextInt)
                .boxed()
                .limit(100000)
                .collect(Collectors.toList());
        List<Integer> actualResult = ChunkedStream
                .chunked(
                        expectedResult
                                .stream()
                                .parallel(),
                        3
                )
                .flatMap(Collection::stream)
                .collect(Collectors.toList());
        Assertions.assertEquals(expectedResult, actualResult);
    }
}

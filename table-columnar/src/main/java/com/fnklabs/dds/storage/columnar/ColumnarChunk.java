package com.fnklabs.dds.storage.columnar;

import com.fnklabs.dds.storage.TableStorage;
import com.fnklabs.dds.table.*;
import com.fnklabs.dds.table.codec.CodecRegistry;
import com.fnklabs.dds.table.codec.DataTypeCodec;
import com.fnklabs.dds.table.expression.EvaluatorFactory;
import com.fnklabs.dds.table.expression.ExpressionEvaluator;
import com.fnklabs.metrics.Counter;
import com.fnklabs.metrics.MetricsFactory;
import com.google.common.base.Verify;
import com.google.common.collect.Range;
import com.google.protobuf.ByteString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

class ColumnarChunk implements TableEngine {
    private static final Logger LOGGER = LoggerFactory.getLogger(ColumnarChunk.class);

    private final int id;
    private final TableDefinition tableDefinition;

    private final Range<Long> tokenRange;


    private final AtomicLong items = new AtomicLong();

    private final Map<ColumnDefinition, Statistic> columnStatistic = new ConcurrentHashMap<>();
    private final Map<String, ColumnDefinition> columns = new HashMap<>();

    private final ColumnDefinition primaryColumnDefinition;

    private final TableStorage storage;

    //    private final Counter sizeCounter;
    private final Counter itemsCounter;

    private final int primaryKeySize;
    private final int rowSize;
    private final long maxSize;

    ColumnarChunk(int id, long maxSize, TableDefinition tableDefinition, TableStorage storage, Range<Long> tokenRange) {
        this.id = id;
        this.maxSize = maxSize;
        this.tableDefinition = tableDefinition;
        this.storage = storage;
        this.tokenRange = tokenRange;

        primaryColumnDefinition = tableDefinition.getColumnList()
                                                 .stream()
                                                 .filter(ColumnDefinition::getPrimary)
                                                 .findFirst()
                                                 .orElse(null);

        primaryKeySize = primaryColumnDefinition.getSize();

        rowSize = tableDefinition.getColumnList()
                                 .stream()
                                 .mapToInt(ColumnDefinition::getSize)
                                 .sum();

        long offset = 0;

        LOGGER.debug("row size {}/{}", primaryKeySize, rowSize);


        for (ColumnDefinition columnDefinition : tableDefinition.getColumnList()) {
            long beginPosition = offset;

            int columnBlockSize = columnDefinition.getSize();
            long requiredColumnBlockSize = (long) columnBlockSize * maxSize / rowSize;

            LOGGER.debug("column `{}` block size: {} total block size: {}", columnDefinition.getName(), columnBlockSize, requiredColumnBlockSize);

            offset += requiredColumnBlockSize;

            long endPosition = offset;

            columnStatistic.put(columnDefinition, new Statistic(maxSize / rowSize, 0.01f, columnBlockSize, beginPosition, endPosition));
            columns.put(columnDefinition.getName(), columnDefinition);
        }

        LOGGER.debug("columns: {} blocks: {}", tableDefinition.getColumnList(), offset);


//        sizeCounter = MetricsFactory.getMetrics().getCounter(String.format("chunk.%s-%d.size", tableDefinition.getName(), id));
        itemsCounter = MetricsFactory.getMetrics().getCounter(String.format("chunk.%s-%d.items", tableDefinition.getName(), id));
    }

    @Override
    public ResultSet query(Insert insert) {
        long currentItems = items.getAndIncrement();

        itemsCounter.inc();

        for (ColumnDefinition columnDefinition : tableDefinition.getColumnList()) {
            if (insert.containsValue(columnDefinition.getName())) {

                // calculate position
                Statistic columnStatistic = this.columnStatistic.get(columnDefinition);

                long position = columnStatistic.startPosition() + currentItems * columnStatistic.getSize();

                long endPosition = position + columnStatistic.getSize();

                Verify.verify(columnStatistic.getRange().contains(endPosition)); // verify limits

                ByteString value = insert.getValueOrThrow(columnDefinition.getName());

                // write data to storage
                storage.write(position, value.toByteArray());

                columnStatistic.addValue(value.toByteArray());
            } ;
        }

        return ResultSet.newBuilder().setWasApplied(true).build();
    }

    @Override
    public ResultSet query(Select select) {


        if (select.getWhere().getClausesCount() == 0) {
            ResultSet.Builder resultSetBuilder = ResultSet.newBuilder();

            for (Selection selection : select.getSelectionList()) {
                AggregationFunction aggregateFunction = selection.getAggregateFunction();

                if (aggregateFunction == AggregationFunction.COUNT) {
                    DataTypeCodec codec = CodecRegistry.get(DataType.LONG);

                    ByteBuffer buffer = ByteBuffer.allocate(codec.size());
                    codec.encode(items.get(), buffer);

                    Row row = Row.newBuilder().putValue("count", ByteString.copyFrom(buffer.array())).build();

                    resultSetBuilder.addResult(row);
                }
            }

            return resultSetBuilder.build();

        } else {
            Collection<Long> filteredResult = new ArrayList<>();

            for (Clause clause : select.getWhere().getClausesList()) {
                ColumnDefinition column = columns.get(clause.getColumn());

                Collection<Long> filter = filter(column, clause.getExpression(), clause.getValue().toByteArray(), filteredResult);

                filteredResult.clear();
                filteredResult.addAll(filter);

                if (filter.isEmpty()) {
                    break; // no data
                }
            }

            ResultSet.Builder resultSetBuilder = ResultSet.newBuilder();

            for (Selection selection : select.getSelectionList()) {
                AggregationFunction aggregateFunction = selection.getAggregateFunction();

                if (aggregateFunction == AggregationFunction.COUNT) {
                    ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
                    buffer.putLong(filteredResult.size());

                    Row row = Row.newBuilder().putValue("count", ByteString.copyFrom(buffer.array())).build();

                    resultSetBuilder.addResult(row);
                }
            }

            return resultSetBuilder.build();
        }
    }

    private Collection<Long> filter(ColumnDefinition columnDefinition, Expression expression, byte[] value, Collection<Long> filteredResult) {
        Statistic statistic = columnStatistic.get(columnDefinition);

        if (!statistic.match(expression, value)) {
            return Collections.emptyList();
        }

        Collection<Long> mergedResult = new ArrayList<>();

        if (filteredResult.isEmpty()) {
            long from = statistic.startPosition();
            long to = from + statistic.getSize() * items.get(); // last written position to avoid null scan

            ExpressionEvaluator expressionEvaluator = EvaluatorFactory.get(expression);

            AtomicLong itemIndex = new AtomicLong();

            storage.scan(from, to,
                         (position, data) -> {
                             long index = itemIndex.getAndIncrement();

                             if (expressionEvaluator.evaluate(value, data)) {
                                 mergedResult.add(index);
                             }

                             return true;
                         },
                         () -> new byte[statistic.getSize()]
            );
        } else {
            byte[] buffer = new byte[statistic.getSize()];

            ExpressionEvaluator expressionEvaluator = EvaluatorFactory.get(expression);

            for (Long itemIndex : filteredResult) {
                long position = statistic.startPosition() + itemIndex * statistic.getSize();

                storage.read(position, buffer);

                if (expressionEvaluator.evaluate(value, buffer)) {
                    mergedResult.add(itemIndex);
                }
            }
        }

        return mergedResult;
    }
}

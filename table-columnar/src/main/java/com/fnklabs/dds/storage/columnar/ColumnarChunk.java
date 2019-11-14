package com.fnklabs.dds.storage.columnar;

import com.fnklabs.dds.storage.TableStorage;
import com.fnklabs.dds.table.AggregationFunction;
import com.fnklabs.dds.table.Clause;
import com.fnklabs.dds.table.ColumnDefinition;
import com.fnklabs.dds.table.Expression;
import com.fnklabs.dds.table.Insert;
import com.fnklabs.dds.table.ResultSet;
import com.fnklabs.dds.table.Row;
import com.fnklabs.dds.table.Select;
import com.fnklabs.dds.table.Selection;
import com.fnklabs.dds.table.TableDefinition;
import com.fnklabs.dds.table.TableEngine;
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
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
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

    private final long maxItems;

    private final int primaryKeySize;
    private final int rowSize;

    private final ThreadLocal<Collection<Long>> resultBuffer;

    ColumnarChunk(int id, long maxSize, TableDefinition tableDefinition, TableStorage storage, Range<Long> tokenRange) {
        this.id = id;
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

        long maxItems = maxSize / rowSize;

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

            long columnMaxItems = requiredColumnBlockSize / columnBlockSize;

            if (columnMaxItems < maxItems) {
                maxItems = columnMaxItems;
            }
        }

        this.maxItems = maxItems;
        this.resultBuffer = ThreadLocal.withInitial(() -> new HashSet<Long>((int) this.maxItems));

        LOGGER.debug("columns: {} blocks: {}", tableDefinition.getColumnList(), offset);


        itemsCounter = MetricsFactory.getMetrics().getCounter(String.format("chunk.%s-%d.items", tableDefinition.getName(), id));
    }

    @Override
    public ResultSet query(Insert insert) {
        long currentItems = items.getAndIncrement();

        Verify.verify(currentItems < maxItems, "overflow");

        itemsCounter.inc();

        for (ColumnDefinition columnDefinition : tableDefinition.getColumnList()) {
            if (insert.containsValue(columnDefinition.getName())) {

                // calculate position
                Statistic columnStatistic = this.columnStatistic.get(columnDefinition);

                long position = columnStatistic.startPosition() + currentItems * columnStatistic.getSize();

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

        Collection<Long> filteredResult = resultBuffer.get();

        if (select.getWhere().getClausesList().size() == 0) {
//        todo    fullScan(primaryColumnDefinition, EvaluatorFactory.get(Expression.N) filteredResult);

        } else {
            for (Clause clause : select.getWhere().getClausesList()) {
                ColumnDefinition column = columns.get(clause.getColumn());

                filter(column, clause.getExpression(), clause.getValue().toByteArray(), filteredResult);

                if (filteredResult.isEmpty()) {
                    break; // no data
                }
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

        filteredResult.clear();

        return resultSetBuilder.build();
//        }
    }

    private void fullScan(ColumnDefinition columnDefinition, ExpressionEvaluator expressionEvaluator, byte[] value, Collection<Long> result) {
        Statistic statistic = columnStatistic.get(columnDefinition);

        long from = statistic.startPosition();
        long to = from + statistic.getSize() * items.get(); // last written position to avoid null scan

        AtomicLong itemIndex = new AtomicLong();

        storage.scan(from, to,
                     (position, data) -> {
                         long index = itemIndex.getAndIncrement();

                         if (expressionEvaluator.evaluate(data, value)) {
                             result.add(index);
                         }

                         return true;
                     },
                     () -> new byte[statistic.getSize()]
        );
    }

    private void filter(ColumnDefinition columnDefinition, Expression expression, byte[] value, Collection<Long> filteredResult) {
        Statistic statistic = columnStatistic.get(columnDefinition);

        if (!statistic.match(expression, value)) {
            return;
        }

        byte[] buffer = new byte[columnDefinition.getSize()];

        ExpressionEvaluator expressionEvaluator = EvaluatorFactory.get(expression);

        Iterator<Long> iterator = filteredResult.iterator();

        while (iterator.hasNext()) {
            Long itemIndex = iterator.next();

            long position = statistic.startPosition() + itemIndex * statistic.getSize();

            storage.read(position, buffer);

            if (!expressionEvaluator.evaluate(value, buffer)) {
                filteredResult.remove(itemIndex);
            }
        }
    }
}

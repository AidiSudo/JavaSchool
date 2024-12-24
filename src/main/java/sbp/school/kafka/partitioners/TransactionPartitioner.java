package sbp.school.kafka.partitioners;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import sbp.school.kafka.entities.Transaction;

import java.util.List;
import java.util.Map;

/**
 * Реализация логики выбора партиции для транзакции
 */
public class TransactionPartitioner implements Partitioner {
    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        List<PartitionInfo> partitionInfos = cluster.partitionsForTopic(topic);

        int partitionsCount = partitionInfos.size();

        int partitionNum = ((Transaction) value).getOperationType().getOperationKey();

        /**
         * Если ключа нет или кол-во партиций по какой-то причине меньше, чем типов операций,
         * то пишем все в последнюю партицию
         */
        if (key == null || partitionNum >= partitionsCount) {
            return partitionsCount - 1;
        }

        return partitionNum;
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}

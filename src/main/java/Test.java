import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.CanalEntry.*;
import com.alibaba.otter.canal.protocol.Message;
import com.alibaba.otter.canal.protocol.exception.CanalClientException;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 * @author pengcheng
 * @version V1.0
 * @description
 * @date 2019/04/11 17:41
 */
public class Test {
    public static void main(String args[]) throws CanalClientException, ParseException, ExecutionException, InterruptedException {
        // 创建链接
        CanalConnector connector = CanalConnectors.newSingleConnector(new InetSocketAddress("192.168.147.130",
                11111), "example", "canal", "canal");
        int batchSize = 1000;
        try {
            // 连接canal，获取数据
            connector.connect();
            connector.subscribe(".*\\..*");
            connector.rollback();
            int count = 0;
            while (true) {
                // 获取指定数量的数据
                Message message = connector.getWithoutAck(batchSize);
                // 数据批号
                long batchId = message.getId();
                // 获取该批次数据的数量
                int size = message.getEntries().size();
                // 无数据
                if (batchId == -1 || size == 0) {
                    System.out.println("没有数据更新: " + count);
                    ++count;
                    // 等待1秒后重新获取
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                    }
                } else {
                    System.out.printf("message[batchId=%s,size=%s] \n", batchId, size);
                    printEntry(message.getEntries());
                    count = 0;
                }

                // 提交确认
                connector.ack(batchId);
                // connector.rollback(batchId); // 处理失败, 回滚数据
            }
        } catch (CanalClientException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            connector.disconnect();
        }
    }

    private static void printEntry(List<CanalEntry.Entry> entrys) throws IOException, ParseException, ExecutionException, InterruptedException {
        TransportClient client = getClient();
        for (CanalEntry.Entry entry : entrys) {
            if (entry.getEntryType() == EntryType.TRANSACTIONBEGIN || entry.getEntryType() == EntryType.TRANSACTIONEND) {
                continue;
            }

            RowChange rowChage = null;
            try {
                rowChage = RowChange.parseFrom(entry.getStoreValue());
            } catch (Exception e) {
                throw new RuntimeException("ERROR ## parser of eromanga-event has an error , data:" + entry,
                        e);
            }

            CanalEntry.EventType eventType = rowChage.getEventType();
            System.out.println(String.format("================&gt; binlog[%s:%s] , name[%s,%s] , eventType : %s",
                    entry.getHeader().getLogfileName(), entry.getHeader().getLogfileOffset(),
                    entry.getHeader().getSchemaName(), entry.getHeader().getTableName(),
                    eventType));
            for (RowData rowData : rowChage.getRowDatasList()) {
                if (eventType == EventType.DELETE) {
                    printColumn(rowData.getBeforeColumnsList());
                    deleteEs(rowData.getBeforeColumnsList(), client);
                } else if (eventType == CanalEntry.EventType.INSERT) {
                    printColumn(rowData.getAfterColumnsList());
                    insertIntoEs(rowData.getAfterColumnsList(), client);
                } else {
                    System.out.println("-------&gt; before");
                    printColumn(rowData.getBeforeColumnsList());
                    System.out.println("-------&gt; after");
                    printColumn(rowData.getAfterColumnsList());
                    updateEs(rowData.getAfterColumnsList(), client);

                }
            }
        }
        client.close();
    }

    private static void printColumn(List<Column> columns) {
        for (Column column : columns) {
            System.out.println(column.getName() + " : " + column.getValue() + "    update=" + column.getUpdated());
        }
    }

    private static void insertIntoEs(List<Column> columns, TransportClient client) throws IOException {
        XContentBuilder doc = XContentFactory.jsonBuilder().startObject();
        for (Column column : columns) {
            doc.field(column.getName(), column.getValue());
        }
        doc.endObject();
        IndexResponse response = client.prepareIndex("user", "data").setSource(doc).get();
        System.out.println(response.status());
    }

    private static void updateEs(List<Column> columns, TransportClient client) throws IOException, ParseException, ExecutionException, InterruptedException {
        String id = "";
        String name = "";
        int role_id = 0;
        Date c_time = new Date();
        for (Column column : columns) {
            if ("id".equals(column.getName())) {
                id = column.getValue();
            } else if ("name".equals(column.getName())) {
                name = column.getValue();
            } else if ("role_id".equals(column.getName())) {
                role_id = Integer.valueOf(column.getValue());
            } else if ("c_time".equals(column.getName())) {
                c_time = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(column.getValue());
            }
        }
        TermQueryBuilder builder = QueryBuilders.termQuery("id", id);
        SearchResponse searchResponse = client.prepareSearch("user").setTypes("data").setQuery(builder).get();
        String trueId = "";
        for (SearchHit hit : searchResponse.getHits()) {
            trueId = hit.getId();
        }

        UpdateRequest request = new UpdateRequest();
        request.index("user").type("data").id(trueId).doc(XContentFactory.jsonBuilder().startObject().field("name", name)
                .field("role_id", role_id).field("c_time", c_time).endObject());
        UpdateResponse response = client.update(request).get();
        System.out.println(response.status());
    }


    private static void deleteEs(List<Column> columns, TransportClient client) {
        String id = "";
        for (Column column : columns) {
            if ("id".equals(column.getName())) {
                id = column.getValue();
                break;
            }
        }
        BulkByScrollResponse response = DeleteByQueryAction.INSTANCE.newRequestBuilder(client)
                .filter(QueryBuilders.matchQuery("id", id))
                .source("user")
                .get();
        long deleted = response.getDeleted();
        System.out.println(deleted);
    }

    private static TransportClient getClient() throws UnknownHostException {
        // 指定es集群名称
        Settings set = Settings.builder().put("cluster.name", "my-elasticsearch").build();
        // 创建访问es集群的客户端
        TransportClient client = new PreBuiltTransportClient(set)
                .addTransportAddress(new TransportAddress(InetAddress.getByName("192.168.147.130"), 9300));
        return client;
    }
}

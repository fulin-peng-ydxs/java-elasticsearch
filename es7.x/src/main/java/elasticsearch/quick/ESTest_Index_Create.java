package elasticsearch.quick;

import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;

public class ESTest_Index_Create {
    public static void main(String[] args) throws Exception {

        RestHighLevelClient esClient = ESTest_Client.getRestHighLevelClient();

        // 创建索引
        CreateIndexRequest request = new CreateIndexRequest("user");
        CreateIndexResponse createIndexResponse =
                esClient.indices().create(request, RequestOptions.DEFAULT);

        // 响应状态
        boolean acknowledged = createIndexResponse.isAcknowledged();
        System.out.println("索引操作 ：" + acknowledged);

        esClient.close();
    }
}

package elasticsearch.quick;

import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.client.indices.GetIndexResponse;

public class ESTest_Index_Search {
    public static void main(String[] args) throws Exception {

        RestHighLevelClient esClient = ESTest_Client.getRestHighLevelClient();

        // 查询索引
        GetIndexRequest request = new GetIndexRequest("user");

        GetIndexResponse getIndexResponse =
                esClient.indices().get(request, RequestOptions.DEFAULT);

        // 响应状态
        System.out.println(getIndexResponse.getAliases());
        System.out.println(getIndexResponse.getMappings());
        System.out.println(getIndexResponse.getSettings());

        esClient.close();
    }
}

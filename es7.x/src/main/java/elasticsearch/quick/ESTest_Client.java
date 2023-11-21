package elasticsearch.quick;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;

public class ESTest_Client {



    public static void main(String[] args) throws Exception {

        // 创建ES客户端
        RestHighLevelClient esClient =getRestHighLevelClient();
        // 关闭ES客户端
        esClient.close();
    }

    public static RestHighLevelClient getRestHighLevelClient(){
        return new RestHighLevelClient(
                RestClient.builder(new HttpHost("192.168.1.104", 9200, "http"))
        );
    }

}

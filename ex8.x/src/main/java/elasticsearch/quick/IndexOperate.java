package elasticsearch.quick;


import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.indices.CreateIndexRequest;
import co.elastic.clients.elasticsearch.indices.CreateIndexResponse;
import co.elastic.clients.elasticsearch.indices.GetIndexRequest;
import co.elastic.clients.elasticsearch.indices.GetIndexResponse;

/**
 * 索引操作
 * author: pengshuaifeng
 * 2023/12/3
 */
public class IndexOperate {


    public static void main(String[] args) throws Exception{
        ElasticsearchClient client=EsClient.getClient();
        basicIndexApi(client);
    }

    /**
     * 普通操作
     * 2023/12/3 12:24
     * @author pengshuaifeng
     */
    public static void basicIndexApi(ElasticsearchClient client) throws Exception{
        CreateIndexRequest request = new CreateIndexRequest.Builder().index("myindex").build();
        final CreateIndexResponse createIndexResponse = client.indices().create(request);
        System.out.println("创建索引成功:" + createIndexResponse.acknowledged());
        // 查询索引
        GetIndexRequest getIndexRequest = new GetIndexRequest.Builder().index("myindex").build();
        final GetIndexResponse getIndexResponse = client.indices().get(getIndexRequest);
        System.out.println("索引查询成功:" + getIndexResponse.result());
        // 删除索引
//        DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest.Builder().index("myindex").build();
//        final DeleteIndexResponse delete = client.indices().delete(deleteIndexRequest); final boolean acknowledged = delete.acknowledged(); System.out.println("删除索引成功:" + acknowledged);
    }

    /**
     * 函数式操作
     * 2023/12/3 12:25
     * @author pengshuaifeng
     */
    public static  void lambdaIndexApi(ElasticsearchClient client) throws Exception{
        // 创建索引
        final Boolean acknowledged = client.indices().create(p ->
                p.index("")).acknowledged(); System.out.println("创建索引成功"); // 获取索引
        System.out.println(
                client.indices().get(
                        req -> req.index("myindex1")
                ).result());
        // 删除索引
        client.indices().delete(
                reqbuilder -> reqbuilder.index("myindex")).acknowledged();
    }
}

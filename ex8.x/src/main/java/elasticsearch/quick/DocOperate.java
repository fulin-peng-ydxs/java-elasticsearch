package elasticsearch.quick;


import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.FieldValue;
import co.elastic.clients.elasticsearch._types.query_dsl.MatchQuery;
import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import co.elastic.clients.elasticsearch.core.*;
import co.elastic.clients.elasticsearch.core.bulk.BulkOperation;
import co.elastic.clients.elasticsearch.core.bulk.CreateOperation;
import elasticsearch.entity.User;

import java.util.ArrayList;
import java.util.List;

/**
 * 文档操作
 * author: pengshuaifeng
 * 2023/12/3
 */
public class DocOperate {

    public static void main(String[] args)throws Exception{
        ElasticsearchClient client=EsClient.getClient();
        basicDocApi(client);
//        lambdaDocApi(client);
    }

    /**
     * 普通操作
     * 2023/12/3 12:25
     * @author pengshuaifeng
     */
    public static void basicDocApi(ElasticsearchClient client) throws Exception{
        // 创建文档
        User user = new User(1001, 23, "涪陵", "湖南岳阳");
        IndexRequest<User> indexRequest = new IndexRequest.Builder<User>()
                .index("myindex")
                .id(user.getId().toString())
                .document(user)
                .build();
        final IndexResponse index = client.index(indexRequest); System.out.println("文档操作结果:" + index.result());

        // 批量创建文档
        final List<BulkOperation> operations = new ArrayList<>(); for (int i = 1; i <= 5; i++ ) {
            final CreateOperation.Builder<User> builder = new CreateOperation.Builder<>(); builder.index("myindex");
            builder.id("200" + i);
            builder.document(new User(2000 + i, 30 + i * 10, "zhangsan" + i, "beijing"));
            final CreateOperation<User> objectCreateOperation = builder.build();
            final BulkOperation bulk = new BulkOperation.Builder().create(objectCreateOperation).build();
            operations.add(bulk);
        }
        BulkRequest bulkRequest = new BulkRequest.Builder().operations(operations).build();
        final BulkResponse bulkResponse = client.bulk(bulkRequest); System.out.println("数据操作成功:" + bulkResponse);

        // 删除文档
        DeleteRequest deleteRequest = new DeleteRequest.Builder().index("myindex").id("1001").build();
        client.delete(deleteRequest);

        // 查询文档
        final SearchRequest.Builder searchRequestBuilder = new
                SearchRequest.Builder().index("myindex");
        MatchQuery matchQuery = new MatchQuery.Builder().field("address").query(FieldValue.of("湖南岳阳")).build();
        Query query = new Query.Builder().match(matchQuery).build(); searchRequestBuilder.query(query);
        SearchRequest searchRequest = searchRequestBuilder.build();
        final SearchResponse<Object> search = client.search(searchRequest, Object.class);
        System.out.println(search);
    }

    /**
     * 函数式操作
     * 2023/12/3 12:35
     * @author pengshuaifeng
     * @param
     */
    public static void lambdaDocApi(ElasticsearchClient client) throws Exception{
        User user = new User(8888, 23, "涪陵", "湖南岳阳");
        // 创建文档
        System.out.println(
        client.index(
                req ->
                        req.index("myindex")
                                .id(user.getId().toString())
                                .document(user)
        ).result());
        // 批量创建文档
        ArrayList<User> users = new ArrayList<>();
        users.add( new User(8001, 23, "涪陵", "湖南岳阳"));
        users.add( new User(8002, 23, "涪陵", "湖南岳阳"));
        client.bulk(
                req -> {
                    users.forEach(
                            u -> {
                                req.operations(
                                        b -> {
                                            b.create(
                                                    d -> d.id(u.getId().toString()).index("myindex").document(u)
                                            );
                                            return b; }
                                ); }
                    );
                    return req; }
        );
        // 删除文档
        client.delete(req -> req.index("myindex").id("1001"));
        //查询文档
        client.search(
                req -> {
                    req.query(
                            q ->
                                    q.match(
                                            m -> m.field("city").query("beijing")
                                    ) );
                    return req; }
                , Object.class
        );
    }
}

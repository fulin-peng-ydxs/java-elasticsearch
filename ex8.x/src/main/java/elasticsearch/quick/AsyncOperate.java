package elasticsearch.quick;


import co.elastic.clients.elasticsearch.ElasticsearchAsyncClient;
import co.elastic.clients.elasticsearch.indices.CreateIndexResponse;

/**
 * 异步操作
 * author: pengshuaifeng
 * 2023/12/3
 */
public class AsyncOperate {

    public static void main(String[] args) {
        ElasticsearchAsyncClient asyncClient = EsClient.getElasticsearchAsyncClient();
//        asyncClient.indices().create(
//                req -> {
//                    req.index("demo3");
//                    return req;
//                } ).whenComplete((resp, error) -> {
//                    System.out.println("回调函数");
//                    if ( resp != null ) {
//                        System.out.println(resp.acknowledged());
//                    } else {
//                        error.printStackTrace();
//                    }
//                });

        System.out.println("主线程操作...");

        asyncClient.indices().create(
                        req -> {
                            req.index("999");
                            return req;
                        }
                ).thenApply(CreateIndexResponse::acknowledged)
                .whenComplete((resp, error) -> {
                    System.out.println("回调函数");
                    if ( resp ) {
                        System.out.println(resp);
                    } else {
                        System.out.println(error);
                    }
                });
    }
}

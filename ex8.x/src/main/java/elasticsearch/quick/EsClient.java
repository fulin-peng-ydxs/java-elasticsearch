package elasticsearch.quick;

import co.elastic.clients.elasticsearch.ElasticsearchAsyncClient;
import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.http.ssl.SSLContexts;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;

import javax.net.ssl.SSLContext;
import java.io.InputStream;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;

/**
 * 客户端创建
 * author: pengshuaifeng
 * 2023/12/3
 */
public class EsClient {


    public static ElasticsearchClient getClient(){
        try{
            //es客户端构建
            ElasticsearchTransport elasticsearchTransport = getElasticsearchTransport();
            ElasticsearchClient elasticsearchClient = new ElasticsearchClient(elasticsearchTransport);
//            elasticsearchTransport.close();
            return elasticsearchClient;
        }catch (Exception e){
            throw new RuntimeException("创建es客户端异常",e);
        }
    }


    public static ElasticsearchAsyncClient getElasticsearchAsyncClient(){
        try {
            ElasticsearchTransport elasticsearchTransport = getElasticsearchTransport();
            ElasticsearchAsyncClient elasticsearchAsyncClient = new ElasticsearchAsyncClient(elasticsearchTransport);
//            elasticsearchTransport.close();
            return elasticsearchAsyncClient;
        } catch (Exception e) {
            throw new RuntimeException("创建es异步客户端异常",e);
        }
    }

    private static  ElasticsearchTransport  getElasticsearchTransport() throws Exception{
        //es访问安全认证
        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider(); credentialsProvider.setCredentials(AuthScope.ANY,
                new UsernamePasswordCredentials("elastic", "uJgJ5xyvid=qkj2*9KtC"));
        //ssl内容构建
        CertificateFactory factory = CertificateFactory.getInstance("X.509");
        Certificate trustedCa;
        try (InputStream is = EsClient.class.getResourceAsStream("/certs/java-ca.crt")   ) {
            trustedCa = factory.generateCertificate(is);
        }
        KeyStore trustStore = KeyStore.getInstance("pkcs12");
        trustStore.load(null, null);
        trustStore.setCertificateEntry("ca", trustedCa);
        SSLContextBuilder sslContextBuilder = SSLContexts.custom()
                .loadTrustMaterial(trustStore, null);
        final SSLContext sslContext = sslContextBuilder.build();
        //https客户端构建
        RestClientBuilder builder = RestClient.builder(
                        new HttpHost("linux1", 9200, "https"))
                .setHttpClientConfigCallback(httpClientBuilder -> httpClientBuilder.setSSLContext(sslContext)
                        .setSSLHostnameVerifier(NoopHostnameVerifier.INSTANCE)
                        .setDefaultCredentialsProvider(credentialsProvider));
        RestClient restClient = builder.build();
        return new RestClientTransport(restClient, new JacksonJsonpMapper());
    }




    public static void main(String[] args) {
        getClient();
        System.out.println("-------------");
        getElasticsearchAsyncClient();
    }
}

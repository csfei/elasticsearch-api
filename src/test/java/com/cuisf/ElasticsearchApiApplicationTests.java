package com.cuisf;

import com.cuisf.pojo.User;
import com.cuisf.utils.ElasticsearchConst;
import org.apache.lucene.util.QueryBuilder;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

import com.alibaba.fastjson.JSON;


/**
 * elasticsearch 7.13.3 api
 */
@SpringBootTest
class ElasticsearchApiApplicationTests {

    // 客户端 面向对象操作
    @Autowired
    private RestHighLevelClient restHighLevelClient;

    //测试索引的创建  Request
    @Test
    void testCreateIndex() throws IOException {
        // 1.创建索引请求
        CreateIndexRequest request = new CreateIndexRequest("cuisf_index");
        // 2.客户端执行请求  请求后执行响应
        CreateIndexResponse createIndexResponse = restHighLevelClient.indices().create(request, RequestOptions.DEFAULT);

        System.out.println(createIndexResponse);
    }

    //判断索引是否存在
    @Test
    void testExistIndex() throws IOException {

        GetIndexRequest request = new GetIndexRequest("cuisf_index");
        boolean exists = restHighLevelClient.indices().exists(request, RequestOptions.DEFAULT);
        System.out.println(exists);
    }

    //删除索引
    @Test
    void testDeleteIndex() throws IOException {
        DeleteIndexRequest re = new DeleteIndexRequest("cuisf_index");
        AcknowledgedResponse delete = restHighLevelClient.indices().delete(re, RequestOptions.DEFAULT);

        System.out.println(delete.isAcknowledged());
    }

    //添加文档
    @Test
    void  testAddDocument() throws IOException {
        //1.创建对象
        User user = new User("布丁的故事", 1);
        //2.创建请求
        IndexRequest request = new IndexRequest("cuisf_index");
        //3.规则
        request.id("1");
        request.timeout(TimeValue.timeValueSeconds(1));
        request.timeout("1s");

        //将数据放到请求中
        request.source(JSON.toJSONString(user), XContentType.JSON);

        //客户端发送请求  获取响应结果
        IndexResponse response = restHighLevelClient.index(request, RequestOptions.DEFAULT);

        System.out.println(response.toString());


    }


    //获取文档
    @Test
    void testIsExists() throws IOException {
        GetRequest getRequest = new GetRequest("cuisf_index", "1");

        //不获取返回的_source 的上下文
        getRequest.fetchSourceContext(new FetchSourceContext(false));
        getRequest.storedFields("_none_");

        boolean exists = restHighLevelClient.exists(getRequest, RequestOptions.DEFAULT);

        System.out.println(exists);
    }


    //获取文档
    @Test
    void testGetDocument() throws IOException {

        GetRequest request = new GetRequest("cuisf_index", "1");

        GetResponse response = restHighLevelClient.get(request, RequestOptions.DEFAULT);

        System.out.println(response.getSourceAsString());
        System.out.println(response);


    }

    //更新文档
    @Test
    void testUpdateDocument() throws IOException {

        UpdateRequest request = new UpdateRequest("cuisf_index", "1");

        request.timeout("1s");

        User user = new User("布丁的故事续集", 2);
        request.doc(com.alibaba.fastjson.JSON.toJSONString(user), XContentType.JSON);

        UpdateResponse response = restHighLevelClient.update(request, RequestOptions.DEFAULT);
        System.out.println(response.status());
        System.out.println(response);


    }

    //删除文档请求
     @Test
    void testDeleteRequest() throws IOException {

         DeleteRequest request = new DeleteRequest("cuisf_index", "1");
         request.timeout("1s");

         DeleteResponse deleteResponse = restHighLevelClient.delete(request, RequestOptions.DEFAULT);

         System.out.println(deleteResponse.status());


     }


     //大批量插入

    @Test
    void testBulkRequest() throws IOException {
        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.timeout("10s");

        ArrayList<User> userList = new ArrayList<>();
        userList.add(new User("布丁的成长1",3));
        userList.add(new User("布丁的成长2",4));
        userList.add(new User("布丁的成长3",5));
        userList.add(new User("布丁的成长4",6));
        userList.add(new User("布丁的成长5",7));
        userList.add(new User("布丁的成长6",8));

        //批量处理请求

        for (int i = 0; i <userList.size() ; i++) {
            bulkRequest.add(
                    new IndexRequest("cuisf_index").id(""+i+1)
                    .source(JSON.toJSONString(userList.get(i)),XContentType.JSON));
        }

        BulkResponse bulkResponse = restHighLevelClient.bulk(bulkRequest, RequestOptions.DEFAULT);

        System.out.println(bulkResponse.hasFailures());


    }


    //查询

    @Test
    void testSearch() throws IOException {

        SearchRequest searchRequest = new SearchRequest(ElasticsearchConst.ES_INDEX);

        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();

        //精确匹配查询
        TermQueryBuilder termQueryBuilder = QueryBuilders.termQuery("age", "1");

        sourceBuilder.query(termQueryBuilder);

        sourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS));

        searchRequest.source(sourceBuilder);

        SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);

        System.out.println(JSON.toJSONString(searchResponse.getHits()));

        System.out.println("===============");

        for (SearchHit documentFields : searchResponse.getHits().getHits()) {
            System.out.println(documentFields.getSourceAsMap());
        }


    }
}

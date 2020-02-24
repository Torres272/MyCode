package torres.es;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Bulk;
import io.searchbox.core.Index;
import torres.bean.Student;

import java.io.IOException;

public class ESWriterByBulk {
    public static void main(String[] args) throws IOException {
        JestClientFactory jestClientFactory = new JestClientFactory();
        HttpClientConfig httpClientConfig = new HttpClientConfig.Builder("http://hadoop102:9200").build();
        jestClientFactory.setHttpClientConfig(httpClientConfig);
        JestClient jestClient = jestClientFactory.getObject();

        Student stu1 = new Student(4, "lisi");

        Student stu2 = new Student(5, "wangwu");

        Index index1 = new Index.Builder(stu1).id("1004").build();
        Index index2 = new Index.Builder(stu2).id("1005").build();

        Bulk bulk = new Bulk.Builder()
                .defaultIndex("stu")
                .defaultType("_doc")
                .addAction(index1)
                .addAction(index2)
                .build();

        jestClient.execute(bulk);
        jestClient.shutdownClient();


    }
}

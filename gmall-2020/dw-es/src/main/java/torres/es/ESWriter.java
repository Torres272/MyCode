package torres.es;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Index;
import torres.bean.Student;

import java.io.IOException;

public class ESWriter {
    public static void main(String[] args) throws IOException {
        //1.创建ES客户端
        JestClientFactory jestClientFactory = new JestClientFactory();
        HttpClientConfig httpClientConfig = new HttpClientConfig.Builder("http://hadoop102:9200").build();
        jestClientFactory.setHttpClientConfig(httpClientConfig);
        JestClient jestClient = jestClientFactory.getObject();

        //创建Student对象
        Student stu1 = new Student(3, "zhangsan");

        //2.创建ES对象
        Index index = new Index.Builder(stu1)
                .index("stu")
                .type("_doc")
                .id("1001")
                .build();

        jestClient.execute(index);

        jestClient.shutdownClient();
    }
}

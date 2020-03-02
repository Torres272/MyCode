import org.apache.flink.api.common.functions.MapFunction;

import java.util.Random;

public class MyMapFunction01 implements MapFunction<String, String> {
    @Override
    public String map( String s) {
        return s + new Random().nextInt(9);
    }
}

class MyMapFunction02 implements MapFunction<Integer, Integer>{

    @Override
    public Integer map(Integer value) throws Exception {
        return value*2;
    }
}
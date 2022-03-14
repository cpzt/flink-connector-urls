package com.github.cpzt.source;

import com.github.cpzt.connector.url.source.URLSource;
import com.github.cpzt.connector.url.split.URLSourceSplit;
import java.net.URL;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.junit.Test;

public class TestSource {

  @Test
  public void testURLSource() throws Exception {

    URL[] urls = new URL[] {
      new URL("https://www.baidu.com"),
      new URL("https://cn.bing.com/"),
    };

    URLSource source = new URLSource(BasicTypeInfo.of(RowData.class), urls);

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);

    env.fromSource(source, WatermarkStrategy.noWatermarks(), "MyURLSource")
        .setParallelism(1)
        .print()
        .setParallelism(1);

    env.execute("test url source");
  }




  @Test
  public void testSourceWithAddSplitFunction() throws Exception {
    URL[] urls = new URL[] {
        new URL("https://www.baidu.com"),
        new URL("https://cn.bing.com/"),
    };

    URLSource source = new URLSource(BasicTypeInfo.of(RowData.class), urls);

    source.withAddSplitFunction(x -> {
      try {
        URL url = new URL("https://www.so.com");
        URLSourceSplit split = new URLSourceSplit("xx", url, 0, 0);
        return new URLSourceSplit[] {split};
      } catch (Exception e) {
        e.printStackTrace();
        return null;
      }
    });

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);

    env.fromSource(source, WatermarkStrategy.noWatermarks(), "MyURLSource")
        .setParallelism(1)
        .print()
        .setParallelism(1);

    env.execute("test url addSplit source");
  }

}

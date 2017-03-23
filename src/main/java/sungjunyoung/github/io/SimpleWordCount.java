package sungjunyoung.github.io;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;


public class SimpleWordCount {

	static class Split extends DoFn<String, String> {
		@ProcessElement
		public void processElement(ProcessContext c) {
			for (String word : c.element().split(" ")) {
				c.output(word);
			}
		}
	}

	static class FormatResults extends DoFn<KV<String, Long>, String> {
		@ProcessElement
		public void processElement(ProcessContext c) {
			c.output(c.element().getKey() + "의 갯수 : " + c.element().getValue());
		}
	}

	public static void main(String[] args) {

		PipelineOptions options = PipelineOptionsFactory.create();
		Pipeline p = Pipeline.create(options);

		String temp = "가 나 다 라 마 마 마 마 바 사 아 자 차 카 타라 파 하 하 하 하 가 가 가";

		p.apply(Create.of(temp))
						.apply(ParDo.of(new Split()))
						.apply(Count.perElement())
						.apply(ParDo.of(new FormatResults()))
						.apply("WriteNumbers", TextIO.Write.to("test-output").withSuffix(".txt"));

		p.run().waitUntilFinish();
	}
}

package jv.microbenchmark.runners.string_methods;

import jv.microbenchmark.ExecutionPlan;
import org.apache.commons.lang3.StringUtils;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.io.IOException;
import java.nio.file.Files;
import java.util.stream.Stream;

public class ApacheStringRunner {
    @Benchmark
    public void normalizeStream(ExecutionPlan plan, Blackhole blackhole) {
        try(Stream<String> lines = Files.lines(plan.text_input)) {
            lines.forEach(line -> {
                String result =  line.codePoints()
                        .filter(c -> Character.isLetterOrDigit(c)
                                || Character.isSpaceChar(c))
                        .collect(StringBuilder::new,
                                StringBuilder::appendCodePoint,
                                StringBuilder::append)
                        .toString();
                blackhole.consume(result);
            });
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Benchmark
    public void normalizeFori(ExecutionPlan plan, Blackhole blackhole) {
        try(Stream<String> lines = Files.lines(plan.text_input)) {
            lines.forEach(line -> {
                StringBuilder result = new StringBuilder();
                char one;
                for (int i = 0; i < line.length(); ++i) {
                    one = line.charAt(i);
                    if (Character.isLetterOrDigit(one) || Character.isSpaceChar(one)) {
                        result.append(one);
                    }
                }
                blackhole.consume(result.toString());
            });
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Benchmark
    public void split(ExecutionPlan plan, Blackhole blackhole) {
        try(Stream<String> lines = Files.lines(plan.text_input)) {
            lines.forEach(line -> {
                int pos = 0, end;
                end = StringUtils.indexOf(line,"\";\"", pos);
                int id = Integer.parseInt(StringUtils.substring(line, pos, end).replaceFirst("\"", ""));
                pos = end + 3;
                end = StringUtils.indexOf(line,"\";\"", pos);
                String text = StringUtils.substring(line, pos, end);
                pos = end + 3;
                text = text + " " + StringUtils.substring(line, pos, line.length());
                text = StringUtils.lowerCase(StringUtils.chop(text));
                String[] terms = StringUtils.split(text,' ');
                blackhole.consume(id);
                blackhole.consume(terms);

            });
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}

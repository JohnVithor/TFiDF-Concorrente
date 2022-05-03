package jv.microbenchmark.runners.string_methods;

import jv.microbenchmark.ExecutionPlan;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.io.IOException;
import java.nio.file.Files;
import java.util.stream.Stream;

public class JavaStringRunner {
    @Benchmark
    public void normalizeDefault(ExecutionPlan plan, Blackhole blackhole) {
        try(Stream<String> lines = Files.lines(plan.text_input)) {
            lines.forEach(line ->
                blackhole.consume(line.replaceAll("[^\\p{L}\\d ]", "").trim())
            );
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Benchmark
    public void normalizeCompiled(ExecutionPlan plan, Blackhole blackhole) {
        try(Stream<String> lines = Files.lines(plan.text_input)) {
            lines.forEach(line ->
                blackhole.consume(plan.normalize.matcher(line).replaceAll("").trim())
            );
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Benchmark
    public void splitDefault(ExecutionPlan plan, Blackhole blackhole) {
        try(Stream<String> lines = Files.lines(plan.text_input)) {
            lines.forEach(line -> {
                String[] splits = line.split("\";\"");
                int id = Integer.parseInt(splits[0].replaceFirst("\"", ""));
                String text = splits[1] + " " + splits[2].substring(0, splits[2].length() - 1);
                String[] terms = text.split("\\s+");
                blackhole.consume(id);
                blackhole.consume(terms);
            });
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Benchmark
    public void splitCompiled(ExecutionPlan plan, Blackhole blackhole) {
        try(Stream<String> lines = Files.lines(plan.text_input)) {
            lines.forEach(line -> {
                String[] splits = plan.csv_split.split(line);
                int id = Integer.parseInt(splits[0].replaceFirst("\"", ""));
                String text = splits[1] + " " + splits[2].substring(0, splits[2].length() - 1);
                String[] terms = plan.space_split.split(text);
                blackhole.consume(id);
                blackhole.consume(terms);
            });
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}

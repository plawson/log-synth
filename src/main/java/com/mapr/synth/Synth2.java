/*
 * Licensed to the Ted Dunning under one or more contributor license
 * agreements.  See the NOTICE file that may be
 * distributed with this work for additional information
 * regarding copyright ownership.  Ted Dunning licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.mapr.synth;

import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.mapr.synth.samplers.SchemaSampler;
import freemarker.template.Configuration;
import freemarker.template.Template;
import freemarker.template.TemplateException;
import freemarker.template.TemplateExceptionHandler;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.OptionDef;
import org.kohsuke.args4j.spi.IntOptionHandler;
import org.kohsuke.args4j.spi.Setter;

import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamWriter;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.concurrent.*;

/**
 * Generates plausible database tables in JSON, CSV or TSV format according to the data design
 * specified in a schema file.
 */
public class Synth2 {

    public static void main(String[] args) throws IOException, CmdLineException {
        final Options opts = new Options();
        CmdLineParser parser = new CmdLineParser(opts);
        try {
            parser.parseArgument(args);
        } catch (CmdLineException e) {
            System.err.println("Usage: " +
                    "[ -count <number>G|M|K ] " +
                    "-schema schema-file " +
                    "[-format JSON|TSV|CSV|XML ] " +
                    "[-rate number-of-lines-per-second] " +
                    "[-duration number-of-minutes] " +
                    "-template template-file " +
                    "-output output-file-name ");
            throw e;
        }

        Preconditions.checkArgument(opts.template.exists(),
                "Please specify a valid template file");

        Preconditions.checkArgument(opts.output.length() > 0,
                "Output filename must be greater than 0");

        boolean isCountOk = true;
        if (opts.count > 0) {
            if (opts.duration > 0) {
                isCountOk = false;
            }
        }
        Preconditions.checkArgument(isCountOk,
                "-count and -duration are mutually exclusive");

        if (opts.count > 0 && opts.rate > 0)
            Preconditions.checkArgument(opts.count >= opts.rate,
                    "Cannot generate less lines than number of lines per second");

        File outputFile = new File(opts.output);
        if (!outputFile.exists() && outputFile.getParentFile() != null) {
            Preconditions.checkArgument(outputFile.getParentFile().exists() && outputFile.getParentFile().isDirectory(),
                    String.format("Directory %s doesn't exist", outputFile.getParent()));
        }

        if (opts.schema == null) {
            throw new IllegalArgumentException("Must specify schema file using [-schema filename] option");
        }
        final SchemaSampler sampler = new SchemaSampler(opts.schema);
        Template template = null;
        if (opts.template != null) {
            final Configuration cfg = new Configuration(Configuration.VERSION_2_3_21);
            cfg.setDefaultEncoding("UTF-8");
            cfg.setTemplateExceptionHandler(TemplateExceptionHandler.RETHROW_HANDLER);
            cfg.setDirectoryForTemplateLoading(opts.template.getCanonicalFile().getParentFile());

            template = cfg.getTemplate(opts.template.getName());
        }

        ReportingWorker reportingWorker =new ReportingWorker(opts, sampler, template);

        try {
            reportingWorker.call();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static class ReportingWorker implements Callable<Integer> {
        private final Options opts;
        private final SchemaSampler sampler;
        final Template template;

        private static XmlMapper xmlMapper;
        private static XMLStreamWriter sw;

        ReportingWorker(final Options opts, final SchemaSampler sampler, final Template template) {

            this.opts = opts;
            this.sampler = sampler;
            this.template = template;
        }

        @Override
        public Integer call() throws Exception {
            Path outputPath = new File(opts.output).toPath();

            try (PrintStream out = new PrintStream(Files.newOutputStream(outputPath,
                    StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING))) {

                if (opts.format == Format.XML) {
                    XMLOutputFactory f = XMLOutputFactory.newFactory();
                    sw = f.createXMLStreamWriter(out);
                    sw.writeStartDocument();
                    sw.writeCharacters("\n");
                    sw.writeStartElement("root");
                    sw.writeCharacters("\n");

                    xmlMapper = new XmlMapper();
                    xmlMapper.enable(SerializationFeature.INDENT_OUTPUT);
                }

                header(opts.format, sampler.getFieldNames(), out);
                int rows = 0;
                SleepTime sleepTime = null;
                if (opts.rate > 0) {
                    double tmpValue = 1_000_000_000 / opts.rate;
                    long milliTime = 0;
                    int nanoTime;
                    if (tmpValue > 999_999) {
                        Double val = tmpValue / 999_999;
                        milliTime = val.intValue();
                        nanoTime = new Double((val - ((double) milliTime)) * 1_000_000).intValue();
                    } else {
                        nanoTime = (int)tmpValue;
                    }
                    sleepTime = new SleepTime(milliTime, nanoTime);
                    System.out.println("milliseconds: " + sleepTime.miliTime + ", nanoseconds: " + sleepTime.nanoTime);
                }
                generateFile(opts, sampler, template, out, sleepTime);

                if (opts.format == Format.XML) {
                    sw.close();
                }
                return rows;
            }
        }

        public static void header(Format format, List<String> names, PrintStream out) {
            switch (format) {
                case TSV:
                    out.printf("%s\n", withTabs.join(names));
                    break;
                case CSV:
                    out.printf("%s\n", withCommas.join(names));
                    break;
            }
        }


        static void generateFile(Options opts, SchemaSampler s, Template template, PrintStream out, SleepTime sleepTime) throws IOException, TemplateException {
            PrintWriter writer = new PrintWriter(out);

            if (opts.count > 0) {
                for (int i = 0; i < opts.count; i++) {
                    template.process(s.sample(), writer);
                    if (sleepTime != null) {
                        try {
                            Thread.sleep(sleepTime.miliTime, sleepTime.nanoTime);
                        } catch (InterruptedException e) {/* NOOP */}
                    }
                }
            } else if (opts.duration > 0) {
                long duration = (opts.duration * 60 * 1000) + System.currentTimeMillis();
                while (duration > System.currentTimeMillis()) {
                    template.process(s.sample(), writer);
                    if (sleepTime != null) {
                        try {
                            Thread.sleep(sleepTime.miliTime, sleepTime.nanoTime);
                        } catch (InterruptedException e) {/* NOOP */}
                    }
                }
            } else {

                //noinspection InfiniteLoopStatement
                while (true) {
                    template.process(s.sample(), writer);
                    if (sleepTime != null) {
                        try {
                            Thread.sleep(sleepTime.miliTime, sleepTime.nanoTime);
                        } catch (InterruptedException e) {/* NOOP */}
                    }
                }
            }
        }


    }

    private static class SleepTime {

        long miliTime;
        int nanoTime;

        SleepTime(long milliTime, int nanoTime) {

            this.miliTime = milliTime;
            this.nanoTime = nanoTime;
        }
    }

    static Joiner withCommas = Joiner.on(",");
    static Joiner withTabs = Joiner.on("\t");

    public enum Format {
        JSON, TSV, CSV, XML
    }


    private static class Options {
        @Option(name = "-output", required = true)
        String output;

        @Option(name = "-count", handler = SizeParser.class)
        int count;

        @Option(name = "-schema", required = true)
        File schema;

        @Option(name = "-template", required = true)
        File template;

        @Option(name = "-format")
        Format format = Format.CSV;

        @Option(name = "-rate")
        int rate;

        @Option(name = "-duration")
        int duration;

        public static class SizeParser extends IntOptionHandler {
            public SizeParser(CmdLineParser parser, OptionDef option, Setter<? super Integer> setter) {
                super(parser, option, setter);
            }

            @Override
            protected Integer parse(String argument) throws NumberFormatException {
                int n = Integer.parseInt(argument.replaceAll("[kKMG]?$", ""));

                switch (argument.charAt(argument.length() - 1)) {
                    case 'G':
                        n *= 1e9;
                        break;
                    case 'M':
                        n *= 1e6;
                        break;
                    case 'K':
                    case 'k':
                        n *= 1e3;
                        break;
                    default:
                        // no suffix leads here
                        break;
                }
                return n;
            }
        }
    }

}

package Bench;// ============================================================================
//
// Copyright (C) 2006-2019 Talend Inc. - www.talend.com
//
// This source code is available under agreement available at
// %InstallDIR%\features\org.talend.rcp.branding.%PRODUCTNAME%\%PRODUCTNAME%license.txt
//
// You should have received a copy of the agreement
// along with this program; if not, write to Talend SA
// 9 rue Pages 92150 Suresnes, France
//
// ============================================================================

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import org.apache.beam.runners.direct.DirectOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.aws.options.S3Options;
import org.apache.beam.sdk.io.xml.XmlIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;

import javax.xml.bind.annotation.XmlRootElement;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class WriteXML {

    public static void main(String[] args) {
        int nbExec = 25;
        int skipFirst = 5;

        List<Long> durations = new ArrayList<>();
        for (int i = 0; i < nbExec; i++) {
            long start = System.currentTimeMillis();
            //runLocal();
            runS3();
            long end = System.currentTimeMillis();
            if (i >= skipFirst) {
                durations.add((end - start));
            }
        }
        System.out.println("* Execution duration :");
        long sum = 0;
        for (Long d : durations) {
            System.err.println(d);
            sum += d;
        }
        System.out.println("* Average : " + (sum / durations.size()));
        System.out.println("End.");
    }

    private static Properties loadConfig() {
        Properties prop = new Properties();
        try (InputStream is = new FileInputStream(new File("aws.conf"))) {
            prop.load(is);
        } catch (Exception e) {
            System.err.println("Err : " + e.getMessage());
            e.printStackTrace();
        }

        return prop;
    }

    private static void runLocal() {
        PipelineOptions noOption = PipelineOptionsFactory.create();

        // Due to beam bug need to give aws region
        // since I have  'beam-sdks-java-io-amazon-web-services' in dependencies
        S3Options s3o = PipelineOptionsFactory.as(S3Options.class);
        s3o.setAwsRegion("eu-west-1");

        readXML(s3o, //noOption,
                "C:\\Devx\\Tmp\\Resources\\persons.xml", "c:\\beamout\\writeCsv");
    }

    private static void runS3() {
        // Configure AWS credentials
        Properties prop = loadConfig();
        BasicAWSCredentials baseCred =
                new BasicAWSCredentials(prop.getProperty("access_key"), prop.getProperty("secret_key"));
        System.out.println("Credentials : " + prop.getProperty("access_key") + "/" + prop.getProperty("secret_key"));

        AWSStaticCredentialsProvider cred = new AWSStaticCredentialsProvider(baseCred);
        S3Options s3o = PipelineOptionsFactory.as(S3Options.class);
        s3o.setAwsRegion(prop.getProperty("region"));
        s3o.setS3UploadBufferSizeBytes(5_242_880);
        s3o.setAwsCredentialsProvider(cred);

        readXML(s3o, "s3://ypiel/beam/persons.xml", "s3://ypiel/beam/out/csv/");
    }

    private static void readXML(PipelineOptions options, String pattern, String out) {
        Integer zero = new Integer(0);
        DirectOptions directOptions = options.as(DirectOptions.class);
        directOptions.setEnforceEncodability(false);
        directOptions.setEnforceImmutability(false);
        directOptions.setTargetParallelism(Runtime.getRuntime().availableProcessors());
        Pipeline p = Pipeline.create(options);
        p.apply("Configuration", FileIO.match().filepattern(pattern));

        PCollection<FileIO.ReadableFile> files = p.apply(FileIO.match().filepattern(pattern))
                /*.continuously(Duration.standardSeconds(30),
                        Watch.Growth.afterTimeSinceNewOutput(Duration.standardMinutes(2))))*/
                .apply(FileIO.readMatches()); //.withCompression(GZIP));

        files.apply(XmlIO.<Record>readFiles().withRootElement("rootTag")
                .withRecordElement(Record.class.getAnnotation(XmlRootElement.class).name())
                .withRecordClass(Record.class))
              .apply("CSV format", MapElements.into(TypeDescriptors.strings())
                        .via((Record rec) -> rec.name + ";" + rec.age))
                .apply(FileIO.<String>write().via(TextIO.sink()).to(out));

        // Execute the pipeline
        p.run().waitUntilFinish();

    }

    @SuppressWarnings("unused")
    @XmlRootElement(name = "person")
    private static class Record {

        private String name;

        private int age;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public int getAge() {
            return age;
        }

        public void setAge(int age) {
            this.age = age;
        }

    }

}
